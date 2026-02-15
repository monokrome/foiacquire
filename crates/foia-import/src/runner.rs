//! Import runner that orchestrates import operations with progress tracking.

use std::collections::HashSet;
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use super::{ImportSource, ImportStats};
use foia::config::Settings;
use foia::models::{CrawlUrl, DiscoveryMethod};

/// How to store imported files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileStorageMode {
    /// Copy files to the documents directory (default, safe).
    #[default]
    Copy,
    /// Move files to the documents directory (deletes originals).
    Move,
    /// Create hard links (saves disk space, originals remain).
    HardLink,
}

/// Configuration for import operations.
#[derive(Debug, Clone)]
pub struct ImportConfig {
    /// Maximum items to import (0 = unlimited).
    pub limit: usize,
    /// Maximum items to scan (0 = unlimited).
    pub scan_limit: usize,
    /// Enable checkpointing for resume support.
    pub enable_resume: bool,
    /// Checkpoint interval (items between saves).
    pub checkpoint_interval: usize,
    /// Dry run mode (don't actually save).
    pub dry_run: bool,
    /// URL filter regex pattern.
    #[allow(dead_code)]
    pub filter: Option<regex::Regex>,
    /// Explicit source ID (overrides auto-detection).
    pub source_id: Option<String>,
    /// Documents directory for file storage.
    pub documents_dir: std::path::PathBuf,
    /// Pre-loaded existing URLs for deduplication.
    pub existing_urls: HashSet<String>,
    /// How to store files (copy, move, or hard link).
    pub storage_mode: FileStorageMode,
    /// Queue imported URLs for scraper verification.
    pub verify: bool,
    /// Tags to apply to all imported documents.
    pub tags: Vec<String>,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            limit: 0,
            scan_limit: 0,
            enable_resume: true,
            checkpoint_interval: 10000,
            dry_run: false,
            filter: None,
            source_id: None,
            documents_dir: std::path::PathBuf::from("."),
            existing_urls: HashSet::new(),
            storage_mode: FileStorageMode::Copy,
            verify: true,
            tags: Vec::new(),
        }
    }
}

/// Orchestrates import operations with progress tracking.
pub struct ImportRunner<'a> {
    settings: &'a Settings,
}

impl<'a> ImportRunner<'a> {
    /// Create a new import runner.
    pub fn new(settings: &'a Settings) -> Self {
        Self { settings }
    }

    /// Run an import operation with progress tracking.
    pub async fn run<S: ImportSource>(
        &self,
        source: &mut S,
        config: &ImportConfig,
    ) -> anyhow::Result<ImportStats> {
        // Verify source exists if specified (skip for dry run)
        if !config.dry_run {
            if let Some(ref sid) = config.source_id {
                let ctx = self.settings.create_db_context()?;
                let source_repo = ctx.sources();
                if source_repo.get(sid).await?.is_none() {
                    anyhow::bail!(
                        "Source '{}' not found. Use 'source list' to see available sources.",
                        sid
                    );
                }
            }
        }

        println!(
            "\n{} Importing from {} ({})",
            style("→").cyan(),
            source.display_name(),
            source.source_path().display()
        );

        if config.dry_run {
            println!(
                "{} Dry run mode - no changes will be made",
                style("!").yellow()
            );
        }

        // Check for resume
        let start_position = if config.enable_resume && source.supports_resume() {
            if let Some(progress) = source.load_progress() {
                if progress.done {
                    println!("  {} Already fully processed, skipping", style("✓").green());
                    return Ok(ImportStats::default());
                }
                if let Some(ref err) = progress.error {
                    println!("  {} Previous attempt failed: {}", style("!").yellow(), err);
                    println!("  {} Retrying from start", style("→").cyan());
                    0
                } else {
                    println!(
                        "  {} Resuming from position {}",
                        style("→").cyan(),
                        progress.position
                    );
                    progress.position
                }
            } else {
                0
            }
        } else {
            0
        };

        // Create progress bar
        let pb = self.create_progress_bar(source);
        pb.set_position(start_position);

        // Run the import
        let (final_progress, stats) = source.run_import(config, start_position).await?;

        pb.finish_and_clear();

        // Save final progress
        if config.enable_resume && source.supports_resume() && !config.dry_run {
            let _ = source.save_progress(&final_progress);
        }

        // Queue imported URLs for scraper verification
        if config.verify && !config.dry_run && !stats.imported_urls.is_empty() {
            if let Some(ref source_id) = config.source_id {
                let ctx = self.settings.create_db_context()?;
                let crawl_repo = ctx.crawl();

                let mut queued = 0usize;
                for url in &stats.imported_urls {
                    // Only queue real URLs, not synthetic concordance:// ones
                    if url.starts_with("http://") || url.starts_with("https://") {
                        let crawl_url = CrawlUrl::new(
                            url.clone(),
                            source_id.clone(),
                            DiscoveryMethod::ConcordanceImport,
                            None,
                            0,
                        );
                        if crawl_repo.add_url(&crawl_url).await.unwrap_or(false) {
                            queued += 1;
                        }
                    }
                }
                if queued > 0 {
                    println!(
                        "  {} Queued {} URLs for scraper verification",
                        style("→").cyan(),
                        queued
                    );
                }
            }
        }

        // Print summary
        self.print_summary(&stats);

        Ok(stats)
    }

    /// Create a config with common settings loaded.
    pub async fn create_config(
        &self,
        source_id: Option<String>,
        limit: usize,
        dry_run: bool,
        enable_resume: bool,
        storage_mode: FileStorageMode,
    ) -> anyhow::Result<ImportConfig> {
        let ctx = self.settings.create_db_context()?;
        let doc_repo = ctx.documents();

        println!(
            "{} Loading existing URLs for duplicate detection...",
            style("→").cyan()
        );
        let existing_urls = doc_repo.get_all_urls_set().await.unwrap_or_default();
        println!("  {} existing URLs loaded", existing_urls.len());

        Ok(ImportConfig {
            limit,
            scan_limit: 0,
            enable_resume,
            checkpoint_interval: 1000,
            dry_run,
            filter: None,
            source_id,
            documents_dir: self.settings.documents_dir.clone(),
            existing_urls,
            storage_mode,
            verify: true,
            tags: Vec::new(),
        })
    }

    /// Detect the optimal storage mode based on source and destination filesystems.
    ///
    /// Returns HardLink if source and destination are on the same filesystem,
    /// otherwise returns Copy.
    #[cfg(unix)]
    pub fn detect_storage_mode(
        source_path: &std::path::Path,
        dest_dir: &std::path::Path,
    ) -> FileStorageMode {
        use std::os::unix::fs::MetadataExt;

        let source_dev = source_path.metadata().map(|m| m.dev()).ok();
        let dest_dev = dest_dir.metadata().map(|m| m.dev()).ok();

        match (source_dev, dest_dev) {
            (Some(s), Some(d)) if s == d => {
                tracing::debug!(
                    "Source and destination on same filesystem (dev {}), using hard links",
                    s
                );
                FileStorageMode::HardLink
            }
            _ => FileStorageMode::Copy,
        }
    }

    #[cfg(not(unix))]
    pub fn detect_storage_mode(
        _source_path: &std::path::Path,
        _dest_dir: &std::path::Path,
    ) -> FileStorageMode {
        // On Windows and other platforms, auto-detection is unreliable due to
        // mount points and junction points. Default to Copy - users can
        // explicitly use --link if they know source and destination are on
        // the same volume. The import will fall back to copy if hard link fails.
        FileStorageMode::Copy
    }

    fn create_progress_bar<S: ImportSource>(&self, source: &S) -> ProgressBar {
        let pb = if let Some(total) = source.total_count() {
            ProgressBar::new(total)
        } else {
            ProgressBar::new_spinner()
        };

        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.cyan} [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner())
                .progress_chars("█▓░"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    fn print_summary(&self, stats: &ImportStats) {
        println!("\n{} Import complete:", style("✓").green());
        println!("  Records scanned:    {}", style(stats.scanned).dim());
        println!("  Documents imported: {}", style(stats.imported).green());
        println!("  Documents skipped:  {}", style(stats.skipped).yellow());
        if stats.filtered > 0 {
            println!("  Records filtered:   {}", style(stats.filtered).dim());
        }
        if stats.no_source > 0 {
            println!(
                "  No matching source: {} (use --source to specify)",
                style(stats.no_source).yellow()
            );
        }
        if stats.missing_files > 0 {
            println!(
                "  Missing files:      {}",
                style(stats.missing_files).yellow()
            );
        }
        if stats.errors > 0 {
            println!("  Errors:             {}", style(stats.errors).red());
        }
    }
}
