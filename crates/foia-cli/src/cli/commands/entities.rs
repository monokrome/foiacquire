//! Entity search and backfill commands.

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use foia::config::Settings;
use foia::repository::diesel_document::entities::EntityFilter;
use foia::repository::diesel_document::DocIdRow;
use foia::repository::models::NewDocumentEntity;
#[cfg(feature = "gis")]
use foia::services::geolookup;
use foia_annotate::services::ner::{EntityType, NerResult};

/// Backfill the document_entities table from existing NER annotation metadata.
///
/// Reads documents that have `annotations.ner_extraction.data` in their metadata
/// JSON but may not yet have rows in document_entities. One-time migration aid.
pub async fn cmd_backfill_entities(
    settings: &Settings,
    source_id: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    let repos = settings.repositories()?;
    let doc_repo = repos.documents;

    let source_filter = if source_id.is_some() {
        "AND d.source_id = $1"
    } else {
        ""
    };

    let limit_clause = if limit > 0 {
        format!("LIMIT {}", limit)
    } else {
        String::new()
    };

    // Find documents with NER annotation data but no entity rows
    let query = format!(
        r#"SELECT d.id
        FROM documents d
        WHERE d.metadata LIKE '%"ner_extraction"%'
        AND d.metadata LIKE '%"data"%'
        AND d.id NOT IN (SELECT DISTINCT document_id FROM document_entities)
        {}
        ORDER BY d.updated_at DESC
        {}"#,
        source_filter, limit_clause
    );

    let doc_ids: Vec<DocIdRow> = foia::with_conn!(doc_repo.pool, conn, {
        if let Some(sid) = source_id {
            diesel_async::RunQueryDsl::load(
                diesel::sql_query(&query).bind::<diesel::sql_types::Text, _>(sid),
                &mut conn,
            )
            .await
        } else {
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await
        }
    })?;

    if doc_ids.is_empty() {
        println!("{} No documents need entity backfill", style("!").yellow());
        println!("  Documents need NER annotations (run extract-entities first)");
        return Ok(());
    }

    println!(
        "{} Backfilling entities for {} documents",
        style("→").cyan(),
        doc_ids.len()
    );

    let pb = ProgressBar::new(doc_ids.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}")
            .unwrap()
            .progress_chars("█▓░"),
    );

    let mut succeeded = 0usize;
    let mut failed = 0usize;

    for row in &doc_ids {
        let doc = match doc_repo.get(&row.id).await? {
            Some(d) => d,
            None => {
                pb.inc(1);
                continue;
            }
        };

        let ner_data = doc
            .metadata
            .get("annotations")
            .and_then(|a| a.get("ner_extraction"))
            .and_then(|n| n.get("data"))
            .and_then(|d| d.as_str());

        let ner_data = match ner_data {
            Some(d) if d != "no_result" => d,
            _ => {
                pb.inc(1);
                continue;
            }
        };

        let ner_result: NerResult = match serde_json::from_str(ner_data) {
            Ok(r) => r,
            Err(e) => {
                pb.println(format!(
                    "{} Failed to parse NER data for {}: {}",
                    style("✗").red(),
                    &doc.id[..8.min(doc.id.len())],
                    e
                ));
                failed += 1;
                pb.inc(1);
                continue;
            }
        };

        let now = chrono::Utc::now().to_rfc3339();
        let normalized: Vec<String> = ner_result
            .entities
            .iter()
            .map(|e| e.text.to_lowercase())
            .collect();

        let entity_rows: Vec<NewDocumentEntity<'_>> = ner_result
            .entities
            .iter()
            .zip(normalized.iter())
            .map(|(entity, norm_text)| {
                let entity_type_str = match entity.entity_type {
                    EntityType::Organization => "organization",
                    EntityType::Person => "person",
                    EntityType::FileNumber => "file_number",
                    EntityType::Location => "location",
                };

                let (latitude, longitude) = if entity.entity_type == EntityType::Location {
                    #[cfg(feature = "gis")]
                    {
                        geolookup::lookup(&entity.text)
                            .map(|(lat, lon)| (Some(lat), Some(lon)))
                            .unwrap_or((None, None))
                    }
                    #[cfg(not(feature = "gis"))]
                    {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                NewDocumentEntity {
                    document_id: &doc.id,
                    entity_type: entity_type_str,
                    entity_text: &entity.text,
                    normalized_text: norm_text,
                    latitude,
                    longitude,
                    created_at: &now,
                }
            })
            .collect();

        match doc_repo.save_document_entities(&entity_rows).await {
            Ok(()) => succeeded += 1,
            Err(e) => {
                pb.println(format!(
                    "{} Failed to save entities for {}: {}",
                    style("✗").red(),
                    &doc.id[..8.min(doc.id.len())],
                    e
                ));
                failed += 1;
            }
        }

        pb.inc(1);
    }

    pb.finish_and_clear();

    println!(
        "{} Backfill complete: {} succeeded, {} failed",
        style("✓").green(),
        succeeded,
        failed
    );

    Ok(())
}

/// Search documents by entity filters from the CLI.
pub async fn cmd_search_entities(
    settings: &Settings,
    query: &str,
    entity_type: Option<&str>,
    near: Option<&str>,
    source_id: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    let repos = settings.repositories()?;
    let doc_repo = repos.documents;

    // Parse --near flag: "lat,lon,radius_km"
    if let Some(near_str) = near {
        let parts: Vec<&str> = near_str.split(',').collect();
        if parts.len() != 3 {
            anyhow::bail!(
                "Invalid --near format. Expected: lat,lon,radius_km (e.g., 55.75,37.61,100)"
            );
        }
        let lat: f64 = parts[0]
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid latitude in --near"))?;
        let lon: f64 = parts[1]
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid longitude in --near"))?;
        let radius_km: f64 = parts[2]
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid radius in --near"))?;

        let doc_ids = doc_repo
            .search_near_location(lat, lon, radius_km, limit, 0)
            .await?;

        println!(
            "{} Found {} documents near ({}, {}) within {}km",
            style("✓").green(),
            doc_ids.len(),
            lat,
            lon,
            radius_km
        );

        for id in &doc_ids {
            if let Ok(Some(doc)) = doc_repo.get(id).await {
                println!(
                    "  {} {}",
                    style(&doc.id[..8.min(doc.id.len())]).dim(),
                    doc.title
                );
            }
        }

        return Ok(());
    }

    // Build entity filters from query string
    let filters = vec![EntityFilter {
        entity_type: entity_type.map(|t| t.to_string()),
        text: query.to_string(),
        exact: false,
    }];

    let count = doc_repo.count_by_entities(&filters, source_id).await?;
    let doc_ids = doc_repo
        .search_by_entities(&filters, source_id, limit, 0)
        .await?;

    let type_label = entity_type.unwrap_or("any type");
    println!(
        "{} Found {} documents matching '{}' (type: {}, showing {})",
        style("✓").green(),
        count,
        query,
        type_label,
        doc_ids.len()
    );

    if doc_ids.is_empty() {
        return Ok(());
    }

    // Fetch entity details for matched docs
    let entities_map = doc_repo.get_entities_batch(&doc_ids).await?;

    for id in &doc_ids {
        if let Ok(Some(doc)) = doc_repo.get(id).await {
            let entities = entities_map.get(id);
            let entity_summary = entities
                .map(|es| {
                    es.iter()
                        .filter(|e| e.normalized_text.contains(&query.to_lowercase()))
                        .map(|e| format!("{}:{}", e.entity_type, e.entity_text))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();

            println!(
                "  {} {} [{}]",
                style(&doc.id[..8.min(doc.id.len())]).dim(),
                doc.title,
                style(&entity_summary).cyan()
            );
        }
    }

    Ok(())
}
