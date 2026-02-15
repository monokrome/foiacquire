//! Database deduplication command.

use std::collections::HashMap;
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;

use foia::config::Settings;
use foia::schema::{
    document_analysis_results, document_pages, document_versions, documents, virtual_files,
};

/// Strategy for choosing which document to keep during deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepStrategy {
    /// Keep the oldest document (first created)
    Oldest,
    /// Keep the newest document (most recently created)
    Newest,
    /// Keep the document with the most complete data (most text, annotations, etc.)
    MostComplete,
}

impl KeepStrategy {
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "oldest" => Ok(Self::Oldest),
            "newest" => Ok(Self::Newest),
            "most-complete" | "mostcomplete" | "complete" => Ok(Self::MostComplete),
            _ => anyhow::bail!(
                "Invalid keep strategy '{}'. Use: oldest, newest, or most-complete",
                s
            ),
        }
    }
}

/// Deduplicate documents by content hash.
///
/// Finds documents with identical content (same content_hash) and merges them,
/// keeping one document and updating all references to point to the keeper.
pub async fn cmd_db_dedup(
    settings: &Settings,
    dry_run: bool,
    keep: &str,
    same_source: bool,
    batch_size: usize,
) -> anyhow::Result<()> {
    let strategy = KeepStrategy::from_str(keep)?;

    println!(
        "{} Deduplicating documents{}",
        style("→").cyan(),
        if dry_run { " (dry run)" } else { "" }
    );
    println!("  Strategy: keep {:?}", strategy);
    println!(
        "  Scope: {}",
        if same_source {
            "within same source only"
        } else {
            "cross-source"
        }
    );

    let repos = settings.repositories()?;
    let pool = repos.pool();

    // Find duplicate groups
    #[derive(diesel::QueryableByName, Debug)]
    struct DuplicateGroup {
        #[diesel(sql_type = diesel::sql_types::Text)]
        content_hash: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        doc_count: i64,
    }

    let group_query = if same_source {
        // Group by content_hash AND source_id
        r#"
        SELECT dv.content_hash, COUNT(DISTINCT d.id) as doc_count
        FROM document_versions dv
        JOIN documents d ON d.id = dv.document_id
        WHERE dv.content_hash IS NOT NULL AND dv.content_hash != ''
        GROUP BY dv.content_hash, d.source_id
        HAVING COUNT(DISTINCT d.id) > 1
        ORDER BY doc_count DESC
        "#
    } else {
        // Group by content_hash only (cross-source)
        r#"
        SELECT dv.content_hash, COUNT(DISTINCT d.id) as doc_count
        FROM document_versions dv
        JOIN documents d ON d.id = dv.document_id
        WHERE dv.content_hash IS NOT NULL AND dv.content_hash != ''
        GROUP BY dv.content_hash
        HAVING COUNT(DISTINCT d.id) > 1
        ORDER BY doc_count DESC
        "#
    };

    let groups: Vec<DuplicateGroup> = foia::with_conn!(pool, conn, {
        diesel::sql_query(group_query).load(&mut conn).await
    })?;

    if groups.is_empty() {
        println!("\n{} No duplicates found!", style("✓").green());
        return Ok(());
    }

    let total_groups = groups.len();
    let total_duplicates: i64 = groups.iter().map(|g| g.doc_count - 1).sum();

    println!(
        "\n  Found {} duplicate groups ({} documents to remove)",
        total_groups, total_duplicates
    );

    let pb = ProgressBar::new(total_groups as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  {bar:40.cyan/dim} {pos}/{len} groups ({per_sec}) {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let mut total_deleted = 0u64;
    let mut total_refs_updated = 0u64;

    // Inline schema for document_annotations (not in main schema.rs)
    diesel::table! {
        document_annotations (id) {
            id -> Integer,
            document_id -> Text,
        }
    }

    // Process in batches - collect all deletes for a batch, then execute
    for chunk in groups.chunks(batch_size) {
        // First pass: determine keepers and documents to delete for this batch
        let mut batch_deletes: Vec<String> = Vec::new();
        let mut batch_updates: Vec<(String, String)> = Vec::new(); // (keeper_id, dup_id)

        for group in chunk {
            #[derive(diesel::QueryableByName, Debug)]
            #[allow(dead_code)]
            struct DocInfo {
                #[diesel(sql_type = diesel::sql_types::Text)]
                id: String,
                #[diesel(sql_type = diesel::sql_types::Text)]
                source_id: String,
                #[diesel(sql_type = diesel::sql_types::Text)]
                created_at: String,
                #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
                extracted_text: Option<String>,
                #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
                synopsis: Option<String>,
                #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
                tags: Option<String>,
            }

            let docs: Vec<DocInfo> = foia::with_conn!(pool, conn, {
                diesel::sql_query(
                    r#"
                    SELECT d.id, d.source_id, d.created_at, d.extracted_text, d.synopsis, d.tags
                    FROM documents d
                    JOIN document_versions dv ON dv.document_id = d.id
                    WHERE dv.content_hash = $1
                    ORDER BY d.created_at ASC
                    "#,
                )
                .bind::<diesel::sql_types::Text, _>(&group.content_hash)
                .load(&mut conn)
                .await
            })?;

            if docs.len() < 2 {
                pb.inc(1);
                continue;
            }

            // Choose keeper based on strategy
            let keeper_idx = match strategy {
                KeepStrategy::Oldest => 0,
                KeepStrategy::Newest => docs.len() - 1,
                KeepStrategy::MostComplete => docs
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, d)| {
                        let text_len = d.extracted_text.as_ref().map(|t| t.len()).unwrap_or(0);
                        let has_synopsis = d.synopsis.is_some() as usize * 1000;
                        let has_tags = d
                            .tags
                            .as_ref()
                            .map(|t| if t != "[]" { 500 } else { 0 })
                            .unwrap_or(0);
                        text_len + has_synopsis + has_tags
                    })
                    .map(|(i, _)| i)
                    .unwrap_or(0),
            };

            let keeper_id = docs[keeper_idx].id.clone();
            for (i, doc) in docs.into_iter().enumerate() {
                if i != keeper_idx {
                    batch_updates.push((keeper_id.clone(), doc.id.clone()));
                    batch_deletes.push(doc.id);
                }
            }

            pb.inc(1);
        }

        if batch_deletes.is_empty() {
            continue;
        }

        let delete_count = batch_deletes.len();

        if !dry_run {
            // Batch update references - group by keeper_id for efficiency
            let mut updates_by_keeper: HashMap<String, Vec<String>> = HashMap::new();
            for (keeper_id, dup_id) in batch_updates {
                updates_by_keeper.entry(keeper_id).or_default().push(dup_id);
            }

            for (keeper_id, dup_ids) in &updates_by_keeper {
                // Update analysis_results
                let updated: usize = foia::with_conn!(pool, conn, {
                    diesel::update(
                        document_analysis_results::table
                            .filter(document_analysis_results::document_id.eq_any(dup_ids)),
                    )
                    .set(document_analysis_results::document_id.eq(keeper_id))
                    .execute(&mut conn)
                    .await
                })?;
                total_refs_updated += updated as u64;

                // Update annotations
                let updated: usize = foia::with_conn!(pool, conn, {
                    diesel::update(
                        document_annotations::table
                            .filter(document_annotations::document_id.eq_any(dup_ids)),
                    )
                    .set(document_annotations::document_id.eq(keeper_id))
                    .execute(&mut conn)
                    .await
                })?;
                total_refs_updated += updated as u64;
            }

            // Batch delete in order respecting foreign keys

            // 1. document_pages
            foia::with_conn!(pool, conn, {
                diesel::delete(
                    document_pages::table
                        .filter(document_pages::document_id.eq_any(&batch_deletes)),
                )
                .execute(&mut conn)
                .await
            })?;

            // 2. virtual_files
            foia::with_conn!(pool, conn, {
                diesel::delete(
                    virtual_files::table.filter(virtual_files::document_id.eq_any(&batch_deletes)),
                )
                .execute(&mut conn)
                .await
            })?;

            // 3. document_versions
            foia::with_conn!(pool, conn, {
                diesel::delete(
                    document_versions::table
                        .filter(document_versions::document_id.eq_any(&batch_deletes)),
                )
                .execute(&mut conn)
                .await
            })?;

            // 4. document_analysis_results (any remaining)
            foia::with_conn!(pool, conn, {
                diesel::delete(
                    document_analysis_results::table
                        .filter(document_analysis_results::document_id.eq_any(&batch_deletes)),
                )
                .execute(&mut conn)
                .await
            })?;

            // 5. document_annotations (any remaining)
            foia::with_conn!(pool, conn, {
                diesel::delete(
                    document_annotations::table
                        .filter(document_annotations::document_id.eq_any(&batch_deletes)),
                )
                .execute(&mut conn)
                .await
            })?;

            // 6. documents
            foia::with_conn!(pool, conn, {
                diesel::delete(documents::table.filter(documents::id.eq_any(&batch_deletes)))
                    .execute(&mut conn)
                    .await
            })?;
        }

        total_deleted += delete_count as u64;
        pb.set_message(format!("deleted: {}", total_deleted));
    }

    pb.finish_with_message(format!("deleted: {}", total_deleted));

    if dry_run {
        println!(
            "\n{} Dry run complete. Would delete {} documents ({} references would be updated).",
            style("✓").green(),
            total_deleted,
            total_refs_updated
        );
    } else {
        println!(
            "\n{} Deleted {} duplicate documents ({} references updated).",
            style("✓").green(),
            total_deleted,
            total_refs_updated
        );
    }

    Ok(())
}
