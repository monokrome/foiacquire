//! Database management commands.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use crate::config::Settings;
use crate::repository::migration::ProgressCallback;
use crate::repository::pool::SqlitePool;
use crate::repository::util::{is_postgres_url, redact_url_password, validate_database_url};
use crate::repository::{DatabaseExporter, DatabaseImporter, SqliteMigrator};
use crate::utils::mime_type_category;

/// Expected schema version (should match storage_meta.format_version).
const EXPECTED_SCHEMA_VERSION: &str = "13";

/// Run database migrations.
pub async fn cmd_migrate(settings: &Settings, check: bool, force: bool) -> anyhow::Result<()> {
    println!("{} Database migration", style("→").cyan());
    println!(
        "  Database: {}",
        redact_url_password(&settings.database_url())
    );

    let ctx = settings.create_db_context()?;

    // Check current schema version
    let current_version = ctx.get_schema_version().await.ok().flatten();

    match &current_version {
        Some(v) => println!("  Current schema version: {}", v),
        None => println!(
            "  Current schema version: {} (not initialized)",
            style("none").yellow()
        ),
    }
    println!("  Expected schema version: {}", EXPECTED_SCHEMA_VERSION);

    let needs_migration = current_version.as_deref() != Some(EXPECTED_SCHEMA_VERSION);
    let schema_exists = current_version.is_some();

    if check {
        // Just report status
        if needs_migration {
            if schema_exists {
                println!(
                    "\n{} Schema version mismatch. Run 'foiacquire db migrate' to update.",
                    style("!").yellow()
                );
            } else {
                println!(
                    "\n{} Database not initialized. Run 'foiacquire db migrate' to initialize.",
                    style("!").yellow()
                );
            }
        } else {
            println!("\n{} Schema is up to date.", style("✓").green());
        }
        return Ok(());
    }

    // Run migrations
    if !needs_migration && !force {
        println!(
            "\n{} Schema is already up to date. Use --force to re-run.",
            style("✓").green()
        );
        return Ok(());
    }

    if force && !needs_migration {
        println!("\n{} Forcing migration re-run...", style("!").yellow());
    }

    println!("\n{} Running migrations...", style("→").cyan());
    match ctx.init_schema().await {
        Ok(()) => {
            println!("{} Migration complete!", style("✓").green());
        }
        Err(e) => {
            eprintln!("{} Migration failed: {}", style("✗").red(), e);
            return Err(anyhow::anyhow!("Migration failed: {}", e));
        }
    }

    // Verify new version
    if let Ok(Some(new_version)) = ctx.get_schema_version().await {
        println!("  Schema version is now: {}", new_version);
    }

    Ok(())
}

/// Options for database copy operations.
#[derive(Clone)]
#[allow(dead_code)]
pub struct CopyOptions {
    pub clear: bool,
    pub batch_size: usize,
    pub use_copy: bool,
    pub show_progress: bool,
    pub tables: Option<HashSet<String>>,
    pub analyze: bool,
    pub duplicate_log: Option<Arc<Mutex<DuplicateLogger>>>,
}

/// Logger for duplicate records during merge operations.
#[allow(dead_code)]
pub struct DuplicateLogger {
    file: File,
    count: usize,
}

#[allow(dead_code)]
impl DuplicateLogger {
    /// Create a new duplicate logger writing to the specified file.
    pub fn new(path: &PathBuf) -> std::io::Result<Self> {
        let mut file = File::create(path)?;
        writeln!(file, "table,id")?;
        Ok(Self { file, count: 0 })
    }

    /// Log a duplicate record.
    pub fn log(&mut self, table: &str, id: &str) -> std::io::Result<()> {
        writeln!(self.file, "{},{}", table, id)?;
        self.count += 1;
        Ok(())
    }

    /// Get the number of duplicates logged.
    pub fn count(&self) -> usize {
        self.count
    }
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
#[allow(clippy::too_many_arguments)]
pub async fn cmd_db_copy(
    source_url: &str,
    target_url: &str,
    clear: bool,
    batch_size: usize,
    use_copy: bool,
    show_progress: bool,
    tables: Option<String>,
    analyze: bool,
    skip_duplicates: Option<String>,
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
        println!(
            "  Tables: {}",
            set.iter().cloned().collect::<Vec<_>>().join(", ")
        );
    }

    // Create duplicate logger if skip_duplicates is specified
    let duplicate_log = if let Some(ref path) = skip_duplicates {
        let path = PathBuf::from(path);
        println!(
            "  Skip duplicates: {} (logging to {})",
            style("yes").green(),
            path.display()
        );
        Some(Arc::new(Mutex::new(DuplicateLogger::new(&path)?)))
    } else {
        None
    };

    let options = CopyOptions {
        clear,
        batch_size,
        use_copy,
        show_progress,
        tables: tables_set,
        analyze,
        duplicate_log: duplicate_log.clone(),
    };

    // Detect database types
    let source_is_postgres = is_postgres_url(source_url);
    let target_is_postgres = is_postgres_url(target_url);

    // Validate URLs are supported by this build
    validate_database_url(source_url)?;
    validate_database_url(target_url)?;

    // Validate --copy flag
    if use_copy {
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

    if source_is_postgres || target_is_postgres {
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

        // This is unreachable due to validate_database_url above, but included for completeness
        #[cfg(not(feature = "postgres"))]
        unreachable!("validate_database_url should have caught this");
    }

    // SQLite to SQLite
    let source_pool = SqlitePool::new(source_url);
    let target_pool = SqlitePool::new(target_url);

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
        let (pb, cb) = maybe_progress(
            options.show_progress,
            requests.len() as u64,
            "crawl_requests",
        );
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
            } else if options.duplicate_log.is_some() {
                copy_tables_skip_dups(&source, &target, &options).await?;
            } else {
                copy_tables(&source, &target, &options).await?;
            }
            run_analyze_if_needed(&target, &options).await
        }
        (true, false) => {
            // Postgres to SQLite
            let source = PostgresMigrator::new(source_url).await?;
            let target_pool = SqlitePool::new(target_url);
            let target = SqliteMigrator::new(target_pool);
            copy_tables(&source, &target, &options).await
        }
        (false, true) => {
            // SQLite to Postgres
            let source_pool = SqlitePool::new(source_url);
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
            } else if options.duplicate_log.is_some() {
                copy_tables_skip_dups(&source, &target, &options).await?;
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

/// Copy tables with duplicate skipping (for merge operations).
#[cfg(feature = "postgres")]
async fn copy_tables_skip_dups<S>(
    source: &S,
    target: &crate::repository::migration_postgres::PostgresMigrator,
    options: &CopyOptions,
) -> anyhow::Result<()>
where
    S: DatabaseExporter,
{
    let dup_log = options.duplicate_log.as_ref().unwrap();

    println!("\nCopying tables (skipping duplicates):");

    // Helper to log duplicates
    fn log_dups(log: &Arc<Mutex<DuplicateLogger>>, table: &str, ids: impl Iterator<Item = String>) {
        if let Ok(mut logger) = log.lock() {
            for id in ids {
                let _ = logger.log(table, &id);
            }
        }
    }

    // Sources (string ID)
    if options.should_copy("sources") {
        let sources = source.export_sources().await?;
        let ids: Vec<String> = sources.iter().map(|s| s.id.clone()).collect();
        let existing = target
            .get_existing_string_ids("sources", "id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) =
            sources.into_iter().partition(|s| !existing.contains(&s.id));
        log_dups(dup_log, "sources", dups.iter().map(|s| s.id.clone()));
        println!(
            "  {:>20}: {} new, {} duplicates",
            "sources",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_sources(&to_insert, None).await?;
        }
    }

    // Documents (string ID)
    if options.should_copy("documents") {
        let documents = source.export_documents().await?;
        let ids: Vec<String> = documents.iter().map(|d| d.id.clone()).collect();
        let existing = target
            .get_existing_string_ids("documents", "id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) = documents
            .into_iter()
            .partition(|d| !existing.contains(&d.id));
        log_dups(dup_log, "documents", dups.iter().map(|d| d.id.clone()));
        println!(
            "  {:>20}: {} new, {} duplicates",
            "documents",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_documents(&to_insert, None).await?;
        }
    }

    // Document versions (integer ID)
    if options.should_copy("document_versions") {
        let versions = source.export_document_versions().await?;
        let ids: Vec<i32> = versions.iter().map(|v| v.id).collect();
        let existing = target
            .get_existing_int_ids("document_versions", "id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) = versions
            .into_iter()
            .partition(|v| !existing.contains(&v.id));
        log_dups(
            dup_log,
            "document_versions",
            dups.iter().map(|v| v.id.to_string()),
        );
        println!(
            "  {:>20}: {} new, {} duplicates",
            "document_versions",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_document_versions(&to_insert, None).await?;
        }
    }

    // Document pages (integer ID)
    if options.should_copy("document_pages") {
        let pages = source.export_document_pages().await?;
        let ids: Vec<i32> = pages.iter().map(|p| p.id).collect();
        let existing = target
            .get_existing_int_ids("document_pages", "id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) =
            pages.into_iter().partition(|p| !existing.contains(&p.id));
        log_dups(
            dup_log,
            "document_pages",
            dups.iter().map(|p| p.id.to_string()),
        );
        println!(
            "  {:>20}: {} new, {} duplicates",
            "document_pages",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_document_pages(&to_insert, None).await?;
        }
    }

    // Virtual files (string ID)
    if options.should_copy("virtual_files") {
        let files = source.export_virtual_files().await?;
        let ids: Vec<String> = files.iter().map(|f| f.id.clone()).collect();
        let existing = target
            .get_existing_string_ids("virtual_files", "id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) =
            files.into_iter().partition(|f| !existing.contains(&f.id));
        log_dups(dup_log, "virtual_files", dups.iter().map(|f| f.id.clone()));
        println!(
            "  {:>20}: {} new, {} duplicates",
            "virtual_files",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_virtual_files(&to_insert, None).await?;
        }
    }

    // Crawl URLs (integer ID)
    if options.should_copy("crawl_urls") {
        let urls = source.export_crawl_urls().await?;
        let ids: Vec<i32> = urls.iter().map(|u| u.id).collect();
        let existing = target
            .get_existing_int_ids("crawl_urls", "id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) =
            urls.into_iter().partition(|u| !existing.contains(&u.id));
        log_dups(dup_log, "crawl_urls", dups.iter().map(|u| u.id.to_string()));
        println!(
            "  {:>20}: {} new, {} duplicates",
            "crawl_urls",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_crawl_urls(&to_insert, None).await?;
        }
    }

    // Crawl requests (integer ID)
    if options.should_copy("crawl_requests") {
        let requests = source.export_crawl_requests().await?;
        let ids: Vec<i32> = requests.iter().map(|r| r.id).collect();
        let existing = target
            .get_existing_int_ids("crawl_requests", "id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) = requests
            .into_iter()
            .partition(|r| !existing.contains(&r.id));
        log_dups(
            dup_log,
            "crawl_requests",
            dups.iter().map(|r| r.id.to_string()),
        );
        println!(
            "  {:>20}: {} new, {} duplicates",
            "crawl_requests",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_crawl_requests(&to_insert, None).await?;
        }
    }

    // Crawl config (string ID - source_id is primary key)
    if options.should_copy("crawl_config") {
        let configs = source.export_crawl_configs().await?;
        let ids: Vec<String> = configs.iter().map(|c| c.source_id.clone()).collect();
        let existing = target
            .get_existing_string_ids("crawl_config", "source_id", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) = configs
            .into_iter()
            .partition(|c| !existing.contains(&c.source_id));
        log_dups(
            dup_log,
            "crawl_config",
            dups.iter().map(|c| c.source_id.clone()),
        );
        println!(
            "  {:>20}: {} new, {} duplicates",
            "crawl_config",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_crawl_configs(&to_insert, None).await?;
        }
    }

    // Configuration history (string ID - uuid)
    if options.should_copy("configuration_history") {
        let history = source.export_config_history().await?;
        let ids: Vec<String> = history.iter().map(|h| h.uuid.clone()).collect();
        let existing = target
            .get_existing_string_ids("configuration_history", "uuid", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) = history
            .into_iter()
            .partition(|h| !existing.contains(&h.uuid));
        log_dups(
            dup_log,
            "configuration_history",
            dups.iter().map(|h| h.uuid.clone()),
        );
        println!(
            "  {:>20}: {} new, {} duplicates",
            "configuration_history",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_config_history(&to_insert, None).await?;
        }
    }

    // Rate limit state (string ID - domain)
    if options.should_copy("rate_limit_state") {
        let states = source.export_rate_limit_states().await?;
        let ids: Vec<String> = states.iter().map(|s| s.domain.clone()).collect();
        let existing = target
            .get_existing_string_ids("rate_limit_state", "domain", &ids)
            .await?;
        let (to_insert, dups): (Vec<_>, Vec<_>) = states
            .into_iter()
            .partition(|s| !existing.contains(&s.domain));
        log_dups(
            dup_log,
            "rate_limit_state",
            dups.iter().map(|s| s.domain.clone()),
        );
        println!(
            "  {:>20}: {} new, {} duplicates",
            "rate_limit_state",
            to_insert.len(),
            dups.len()
        );
        if !to_insert.is_empty() {
            target.import_rate_limit_states(&to_insert, None).await?;
        }
    }

    // Print summary
    if let Ok(logger) = dup_log.lock() {
        println!(
            "\n{} Copy complete! ({} duplicates skipped, logged to file)",
            style("✓").green(),
            logger.count()
        );
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
        let (cb, finish) =
            create_copy_progress(options.show_progress, sources.len() as u64, "sources");
        target.copy_sources(&sources, cb).await?;
        finish();
    }

    // Documents - use COPY
    if options.should_copy("documents") {
        let documents = source.export_documents().await?;
        let (cb, finish) =
            create_copy_progress(options.show_progress, documents.len() as u64, "documents");
        target.copy_documents(&documents, cb).await?;
        finish();
    }

    // Document versions - use COPY
    if options.should_copy("document_versions") {
        let versions = source.export_document_versions().await?;
        let (cb, finish) = create_copy_progress(
            options.show_progress,
            versions.len() as u64,
            "document_versions",
        );
        target.copy_document_versions(&versions, cb).await?;
        finish();
    }

    // Document pages - use COPY
    if options.should_copy("document_pages") {
        let pages = source.export_document_pages().await?;
        let (cb, finish) =
            create_copy_progress(options.show_progress, pages.len() as u64, "document_pages");
        target.copy_document_pages(&pages, cb).await?;
        finish();
    }

    // Virtual files - use COPY
    if options.should_copy("virtual_files") {
        let files = source.export_virtual_files().await?;
        let (cb, finish) =
            create_copy_progress(options.show_progress, files.len() as u64, "virtual_files");
        target.copy_virtual_files(&files, cb).await?;
        finish();
    }

    // Crawl URLs - use COPY
    if options.should_copy("crawl_urls") {
        let urls = source.export_crawl_urls().await?;
        let (cb, finish) =
            create_copy_progress(options.show_progress, urls.len() as u64, "crawl_urls");
        target.copy_crawl_urls(&urls, cb).await?;
        finish();
    }

    // Crawl requests - use COPY
    if options.should_copy("crawl_requests") {
        let requests = source.export_crawl_requests().await?;
        let (cb, finish) = create_copy_progress(
            options.show_progress,
            requests.len() as u64,
            "crawl_requests",
        );
        target.copy_crawl_requests(&requests, cb).await?;
        finish();
    }

    // Crawl configs - use COPY
    if options.should_copy("crawl_config") {
        let configs = source.export_crawl_configs().await?;
        let (cb, finish) =
            create_copy_progress(options.show_progress, configs.len() as u64, "crawl_config");
        target.copy_crawl_configs(&configs, cb).await?;
        finish();
    }

    // Config history - use COPY
    if options.should_copy("configuration_history") {
        let history = source.export_config_history().await?;
        let (cb, finish) = create_copy_progress(
            options.show_progress,
            history.len() as u64,
            "configuration_history",
        );
        target.copy_config_history(&history, cb).await?;
        finish();
    }

    // Rate limit states - use COPY
    if options.should_copy("rate_limit_state") {
        let states = source.export_rate_limit_states().await?;
        let (cb, finish) = create_copy_progress(
            options.show_progress,
            states.len() as u64,
            "rate_limit_state",
        );
        target.copy_rate_limit_states(&states, cb).await?;
        finish();
    }

    // Reset sequences for SERIAL columns
    println!("{} Resetting sequences...", style("→").cyan());
    target.reset_sequences().await?;

    println!("\n{} Copy complete!", style("✓").green());

    Ok(())
}

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
        let result: CountRow = crate::with_conn!(pool, conn, {
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
            crate::with_conn!(pool, conn, {
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

                crate::with_conn!(pool, conn, {
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
