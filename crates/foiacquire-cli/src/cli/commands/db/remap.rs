//! Database category remapping command.

use std::collections::HashMap;
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use foiacquire::config::Settings;
use foiacquire::utils::mime_type_category;

/// Remap document categories based on MIME types.
///
/// This command updates the category_id column for all documents based on
/// the MIME type of their current (latest) version. Processes documents in
/// batches to limit memory usage.
pub async fn cmd_db_remap_categories(
    settings: &Settings,
    dry_run: bool,
    batch_size: usize,
) -> anyhow::Result<()> {
    use diesel_async::RunQueryDsl;

    println!(
        "{} Remapping document categories based on MIME types{}",
        style("→").cyan(),
        if dry_run { " (dry run)" } else { "" }
    );
    println!("  Batch size: {}", batch_size);

    let ctx = settings.create_db_context()?;
    let pool = ctx.pool();

    #[derive(diesel::QueryableByName)]
    struct DocMime {
        #[diesel(sql_type = diesel::sql_types::Text)]
        document_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        mime_type: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        current_category: Option<String>,
    }

    // Get total count for progress
    let total_docs: i64 = {
        #[derive(diesel::QueryableByName)]
        struct CountRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }
        let result: CountRow = foiacquire::with_conn!(pool, conn, {
            diesel::sql_query("SELECT COUNT(*) as count FROM documents")
                .get_result(&mut conn)
                .await
        })?;
        result.count
    };

    println!("  Total documents: {}", total_docs);
    println!("  Scanning and updating in batches...\n");

    let pb = ProgressBar::new(total_docs as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  {bar:40.cyan/dim} {pos}/{len} ({per_sec}) {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let mut total_updated = 0u64;
    let mut total_skipped = 0u64;
    let mut category_stats: HashMap<(Option<String>, String), u64> = HashMap::new();
    let mut offset = 0u64;

    loop {
        // Fetch batch of documents with their MIME types
        let batch: Vec<DocMime> = {
            let query = format!(
                r#"SELECT d.id as document_id, dv.mime_type, d.category_id as current_category
                   FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = d.id)
                   ORDER BY d.id
                   LIMIT {} OFFSET {}"#,
                batch_size, offset
            );
            foiacquire::with_conn!(pool, conn, {
                diesel::sql_query(&query).load(&mut conn).await
            })?
        };

        if batch.is_empty() {
            break;
        }

        let batch_len = batch.len();

        // Group by target category for bulk updates
        let mut updates_by_category: HashMap<String, Vec<String>> = HashMap::new();

        for doc in batch {
            let new_category = mime_type_category(&doc.mime_type).id().to_string();
            if doc.current_category.as_deref() == Some(&new_category) {
                total_skipped += 1;
            } else {
                *category_stats
                    .entry((doc.current_category.clone(), new_category.clone()))
                    .or_insert(0) += 1;
                updates_by_category
                    .entry(new_category)
                    .or_default()
                    .push(doc.document_id);
            }
        }

        // Apply bulk updates per category
        if !dry_run {
            for (category, doc_ids) in updates_by_category {
                if doc_ids.is_empty() {
                    continue;
                }

                // Build IN clause with escaped IDs
                let escaped_ids: Vec<String> = doc_ids
                    .iter()
                    .map(|id| format!("'{}'", id.replace('\'', "''")))
                    .collect();
                let in_clause = escaped_ids.join(", ");

                foiacquire::with_conn!(pool, conn, {
                    diesel::sql_query(format!(
                        "UPDATE documents SET category_id = '{}' WHERE id IN ({})",
                        category.replace('\'', "''"),
                        in_clause
                    ))
                    .execute(&mut conn)
                    .await
                })?;

                total_updated += doc_ids.len() as u64;
            }
        } else {
            // In dry run, just count what would be updated
            for doc_ids in updates_by_category.values() {
                total_updated += doc_ids.len() as u64;
            }
        }

        pb.inc(batch_len as u64);
        offset += batch_len as u64;

        pb.set_message(format!(
            "updated: {}, skipped: {}",
            total_updated, total_skipped
        ));
    }

    pb.finish_with_message(format!(
        "updated: {}, skipped: {}",
        total_updated, total_skipped
    ));

    // Print summary
    println!("\n  Category changes:");
    let mut sorted_stats: Vec<_> = category_stats.into_iter().collect();
    sorted_stats.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count descending

    for ((from, to), count) in sorted_stats {
        let from_str = from.as_deref().unwrap_or("NULL");
        println!("    {} -> {}: {} documents", from_str, to, count);
    }
    println!("    No change: {} documents", total_skipped);

    if dry_run {
        println!(
            "\n{} Dry run complete. {} documents would be updated.",
            style("✓").green(),
            total_updated
        );
    } else {
        println!(
            "\n{} Updated {} documents!",
            style("✓").green(),
            total_updated
        );
    }

    Ok(())
}
