//! Database management commands.

use std::sync::Arc;
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use crate::repository::migration::ProgressCallback;
use crate::repository::util::redact_url_password;
use crate::repository::{AsyncSqlitePool, DatabaseExporter, DatabaseImporter, SqliteMigrator};

use std::collections::HashSet;

/// Options for database copy operations.
#[derive(Clone)]
pub struct CopyOptions {
    pub clear: bool,
    pub batch_size: usize,
    pub use_copy: bool,
    pub show_progress: bool,
    pub tables: Option<HashSet<String>>,
    pub analyze: bool,
}

impl CopyOptions {
    /// Check if a table should be copied.
    pub fn should_copy(&self, table: &str) -> bool {
        match &self.tables {
            None => true,
            Some(set) => set.contains(table),
        }
    }
}

/// Copy data between databases.
pub async fn cmd_db_copy(
    source_url: &str,
    target_url: &str,
    clear: bool,
    batch_size: usize,
    use_copy: bool,
    show_progress: bool,
    tables: Option<String>,
    analyze: bool,
) -> anyhow::Result<()> {
    println!("{} Copying database:", style("→").cyan());
    println!("  From: {}", redact_url_password(source_url));
    println!("  To:   {}", redact_url_password(target_url));
    println!("  Batch size: {}", batch_size);

    const ALL_TABLES: &[&str] = &[
        "sources",
        "documents",
        "document_versions",
        "document_pages",
        "virtual_files",
        "crawl_urls",
        "crawl_requests",
        "crawl_config",
        "configuration_history",
        "rate_limit_state",
    ];

    let tables_set = tables.map(|t| {
        t.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<HashSet<_>>()
    });

    if let Some(ref set) = tables_set {
        if set.is_empty() {
            println!(
                "\n{} --tables requires one or more table names.\n\nAvailable tables:\n  {}",
                style("Error:").red().bold(),
                ALL_TABLES.join(", ")
            );
            return Ok(());
        }
        println!("  Tables: {}", set.iter().cloned().collect::<Vec<_>>().join(", "));
    }

    let options = CopyOptions {
        clear,
        batch_size,
        use_copy,
        show_progress,
        tables: tables_set,
        analyze,
    };

    // Validate --copy flag
    if use_copy {
        let target_is_postgres = target_url.starts_with("postgres");
        if !target_is_postgres {
            anyhow::bail!(
                "--copy flag requires a PostgreSQL target database.\n\
                 The COPY command is not supported by SQLite."
            );
        }
        println!(
            "{} Using COPY protocol for fast bulk load",
            style("→").cyan()
        );
    }

    // Detect database types
    let source_is_postgres = source_url.starts_with("postgres");
    let target_is_postgres = target_url.starts_with("postgres");

    if source_is_postgres || target_is_postgres {
        #[cfg(not(feature = "postgres"))]
        {
            anyhow::bail!(
                "PostgreSQL support requires the 'postgres' feature.\n\
                 Compile with: cargo build --features postgres"
            );
        }

        #[cfg(feature = "postgres")]
        {
            return copy_with_postgres(
                source_url,
                target_url,
                source_is_postgres,
                target_is_postgres,
                options,
            )
            .await;
        }
    }

    // SQLite to SQLite
    let source_pool = AsyncSqlitePool::new(source_url, 10);
    let target_pool = AsyncSqlitePool::new(target_url, 10);

    let source = SqliteMigrator::new(source_pool);
    let target = SqliteMigrator::new(target_pool);

    copy_tables(&source, &target, &options).await
}

/// Create a progress bar for a table import.
fn create_progress_bar(total: u64, table_name: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  {prefix:>20} [{bar:40.cyan/dim}] {pos}/{len} ({per_sec})")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.set_prefix(table_name.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Create a progress callback that updates the progress bar.
fn make_progress_callback(pb: ProgressBar) -> ProgressCallback {
    Arc::new(move |count| {
        pb.set_position(count as u64);
    })
}

/// Helper to create progress bar and callback if progress is enabled.
fn maybe_progress(
    show: bool,
    total: u64,
    table_name: &str,
) -> (Option<ProgressBar>, Option<ProgressCallback>) {
    if show {
        let pb = create_progress_bar(total, table_name);
        let cb = make_progress_callback(pb.clone());
        (Some(pb), Some(cb))
    } else {
        println!("  {} ...", table_name);
        (None, None)
    }
}

/// Create a progress bar for COPY operations that tracks "sending" progress.
/// Returns a callback that updates progress, and a finish function to call after sink.finish().
#[cfg(feature = "postgres")]
fn create_copy_progress(
    show: bool,
    total: u64,
    table_name: &str,
) -> (Option<ProgressCallback>, Box<dyn FnOnce()>) {
    if !show {
        println!("  {} ...", table_name);
        return (None, Box::new(|| {}));
    }

    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  {prefix:>20} [{bar:40.cyan/dim}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.set_prefix(table_name.to_string());
    pb.set_message("sending");
    pb.enable_steady_tick(Duration::from_millis(100));

    let pb_clone = pb.clone();
    let cb: ProgressCallback = Arc::new(move |count| {
        pb_clone.set_position(count as u64);
    });

    let finish = Box::new(move || {
        pb.set_message("done");
        pb.finish();
    });

    (Some(cb), finish)
}

/// Copy all tables from source to target.
async fn copy_tables<S, T>(source: &S, target: &T, options: &CopyOptions) -> anyhow::Result<()>
where
    S: DatabaseExporter,
    T: DatabaseImporter,
{
    if options.clear && options.tables.is_none() {
        println!("{} Clearing target database...", style("!").yellow());
        target.clear_all().await?;
    }

    println!("\nCopying tables:");

    // Sources
    if options.should_copy("sources") {
        let sources = source.export_sources().await?;
        let (pb, cb) = maybe_progress(options.show_progress, sources.len() as u64, "sources");
        target.import_sources(&sources, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Documents
    if options.should_copy("documents") {
        let documents = source.export_documents().await?;
        let (pb, cb) = maybe_progress(options.show_progress, documents.len() as u64, "documents");
        target.import_documents(&documents, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Document versions
    if options.should_copy("document_versions") {
        let versions = source.export_document_versions().await?;
        let (pb, cb) = maybe_progress(
            options.show_progress,
            versions.len() as u64,
            "document_versions",
        );
        target.import_document_versions(&versions, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Document pages
    if options.should_copy("document_pages") {
        let pages = source.export_document_pages().await?;
        let (pb, cb) = maybe_progress(options.show_progress, pages.len() as u64, "document_pages");
        target.import_document_pages(&pages, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Virtual files
    if options.should_copy("virtual_files") {
        let files = source.export_virtual_files().await?;
        let (pb, cb) = maybe_progress(options.show_progress, files.len() as u64, "virtual_files");
        target.import_virtual_files(&files, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Crawl URLs
    if options.should_copy("crawl_urls") {
        let urls = source.export_crawl_urls().await?;
        let (pb, cb) = maybe_progress(options.show_progress, urls.len() as u64, "crawl_urls");
        target.import_crawl_urls(&urls, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Crawl requests
    if options.should_copy("crawl_requests") {
        let requests = source.export_crawl_requests().await?;
        let (pb, cb) = maybe_progress(options.show_progress, requests.len() as u64, "crawl_requests");
        target.import_crawl_requests(&requests, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Crawl configs
    if options.should_copy("crawl_config") {
        let configs = source.export_crawl_configs().await?;
        let (pb, cb) = maybe_progress(options.show_progress, configs.len() as u64, "crawl_config");
        target.import_crawl_configs(&configs, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Config history
    if options.should_copy("configuration_history") {
        let history = source.export_config_history().await?;
        let (pb, cb) = maybe_progress(
            options.show_progress,
            history.len() as u64,
            "configuration_history",
        );
        target.import_config_history(&history, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    // Rate limit states
    if options.should_copy("rate_limit_state") {
        let states = source.export_rate_limit_states().await?;
        let (pb, cb) = maybe_progress(
            options.show_progress,
            states.len() as u64,
            "rate_limit_state",
        );
        target.import_rate_limit_states(&states, cb).await?;
        if let Some(pb) = pb {
            pb.finish();
        }
    }

    println!("\n{} Copy complete!", style("✓").green());

    Ok(())
}

#[cfg(feature = "postgres")]
async fn copy_with_postgres(
    source_url: &str,
    target_url: &str,
    source_is_postgres: bool,
    target_is_postgres: bool,
    options: CopyOptions,
) -> anyhow::Result<()> {
    use crate::repository::migration_postgres::PostgresMigrator;

    match (source_is_postgres, target_is_postgres) {
        (true, true) => {
            // Postgres to Postgres
            let source = PostgresMigrator::new(source_url).await?;
            let mut target = PostgresMigrator::new(target_url).await?;
            target.set_batch_size(options.batch_size);
            println!(
                "{} Initializing target schema...",
                console::style("→").cyan()
            );
            target.init_schema().await?;
            if options.use_copy {
                copy_tables_with_copy(&source, &target, &options).await?;
            } else {
                copy_tables(&source, &target, &options).await?;
            }
            run_analyze_if_needed(&target, &options).await
        }
        (true, false) => {
            // Postgres to SQLite
            let source = PostgresMigrator::new(source_url).await?;
            let target_pool = AsyncSqlitePool::new(target_url, 10);
            let target = SqliteMigrator::new(target_pool);
            copy_tables(&source, &target, &options).await
        }
        (false, true) => {
            // SQLite to Postgres
            let source_pool = AsyncSqlitePool::new(source_url, 10);
            let source = SqliteMigrator::new(source_pool);
            let mut target = PostgresMigrator::new(target_url).await?;
            target.set_batch_size(options.batch_size);
            println!(
                "{} Initializing target schema...",
                console::style("→").cyan()
            );
            target.init_schema().await?;
            if options.use_copy {
                copy_tables_with_copy(&source, &target, &options).await?;
            } else {
                copy_tables(&source, &target, &options).await?;
            }
            run_analyze_if_needed(&target, &options).await
        }
        (false, false) => unreachable!(),
    }
}

/// Run ANALYZE on Postgres target if --analyze flag was provided.
#[cfg(feature = "postgres")]
async fn run_analyze_if_needed(
    target: &crate::repository::migration_postgres::PostgresMigrator,
    options: &CopyOptions,
) -> anyhow::Result<()> {
    if !options.analyze {
        return Ok(());
    }
    println!("{} Running ANALYZE...", style("→").cyan());
    if let Some(ref tables) = options.tables {
        let table_refs: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();
        target.analyze_tables(&table_refs).await?;
    } else {
        target.analyze_all().await?;
    }
    Ok(())
}

/// Copy tables using PostgreSQL COPY protocol (fast bulk load).
#[cfg(feature = "postgres")]
async fn copy_tables_with_copy<S>(
    source: &S,
    target: &crate::repository::migration_postgres::PostgresMigrator,
    options: &CopyOptions,
) -> anyhow::Result<()>
where
    S: DatabaseExporter,
{
    if options.clear {
        if let Some(ref tables) = options.tables {
            println!("{} Clearing specified tables...", style("!").yellow());
            let table_refs: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();
            target.clear_tables(&table_refs).await?;
        } else {
            println!("{} Clearing target database...", style("!").yellow());
            target.clear_all().await?;
        }
    } else if options.tables.is_none() {
        println!(
            "{} COPY requires empty tables. Use --clear or ensure tables are empty.",
            style("!").yellow()
        );
    }

    println!("\nCopying tables (COPY protocol):");

    // Sources - use COPY
    if options.should_copy("sources") {
        let sources = source.export_sources().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, sources.len() as u64, "sources");
        target.copy_sources(&sources, cb).await?;
        finish();
    }

    // Documents - use COPY
    if options.should_copy("documents") {
        let documents = source.export_documents().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, documents.len() as u64, "documents");
        target.copy_documents(&documents, cb).await?;
        finish();
    }

    // Document versions - use COPY
    if options.should_copy("document_versions") {
        let versions = source.export_document_versions().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, versions.len() as u64, "document_versions");
        target.copy_document_versions(&versions, cb).await?;
        finish();
    }

    // Document pages - use COPY
    if options.should_copy("document_pages") {
        let pages = source.export_document_pages().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, pages.len() as u64, "document_pages");
        target.copy_document_pages(&pages, cb).await?;
        finish();
    }

    // Virtual files - use COPY
    if options.should_copy("virtual_files") {
        let files = source.export_virtual_files().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, files.len() as u64, "virtual_files");
        target.copy_virtual_files(&files, cb).await?;
        finish();
    }

    // Crawl URLs - use COPY
    if options.should_copy("crawl_urls") {
        let urls = source.export_crawl_urls().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, urls.len() as u64, "crawl_urls");
        target.copy_crawl_urls(&urls, cb).await?;
        finish();
    }

    // Crawl requests - use COPY
    if options.should_copy("crawl_requests") {
        let requests = source.export_crawl_requests().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, requests.len() as u64, "crawl_requests");
        target.copy_crawl_requests(&requests, cb).await?;
        finish();
    }

    // Crawl configs - use COPY
    if options.should_copy("crawl_config") {
        let configs = source.export_crawl_configs().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, configs.len() as u64, "crawl_config");
        target.copy_crawl_configs(&configs, cb).await?;
        finish();
    }

    // Config history - use COPY
    if options.should_copy("configuration_history") {
        let history = source.export_config_history().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, history.len() as u64, "configuration_history");
        target.copy_config_history(&history, cb).await?;
        finish();
    }

    // Rate limit states - use COPY
    if options.should_copy("rate_limit_state") {
        let states = source.export_rate_limit_states().await?;
        let (cb, finish) = create_copy_progress(options.show_progress, states.len() as u64, "rate_limit_state");
        target.copy_rate_limit_states(&states, cb).await?;
        finish();
    }

    // Reset sequences for SERIAL columns
    println!("{} Resetting sequences...", style("→").cyan());
    target.reset_sequences().await?;

    println!("\n{} Copy complete!", style("✓").green());

    Ok(())
}
