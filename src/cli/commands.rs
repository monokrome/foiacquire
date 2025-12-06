//! CLI commands implementation.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::Mutex;

use crate::config::{load_settings, Config, Settings};
use crate::llm::LlmClient;
use crate::models::{Document, DocumentStatus, DocumentVersion, Source, SourceType};
use crate::ocr::TextExtractor;
use crate::repository::{CrawlRepository, DocumentRepository, SourceRepository};
use crate::scrapers::{
    load_rate_limit_state, save_rate_limit_state, ConfigurableScraper, RateLimiter,
};

use super::progress::DownloadProgress;

#[derive(Parser)]
#[command(name = "foiacquire")]
#[command(about = "FOIA document acquisition and research system")]
#[command(version)]
pub struct Cli {
    /// Data directory
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

/// Check if verbose mode is enabled (for early logging setup).
pub fn is_verbose() -> bool {
    std::env::args().any(|arg| arg == "-v" || arg == "--verbose")
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize the data directory and database
    Init,

    /// Manage document sources
    Source {
        #[command(subcommand)]
        command: SourceCommands,
    },

    /// Discover document URLs from a source (does not download)
    Crawl {
        /// Source ID to crawl
        source_id: String,
        /// Limit number of pages to crawl (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
    },

    /// Download pending documents from queue
    Download {
        /// Source ID to download from (optional, downloads from all sources if not specified)
        source_id: Option<String>,
        /// Number of download workers (default: 4)
        #[arg(short, long, default_value = "4")]
        workers: usize,
        /// Limit number of documents to download (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Show detailed progress for each file
        #[arg(short = 'P', long)]
        progress: bool,
    },

    /// Manage crawl state
    State {
        #[command(subcommand)]
        command: StateCommands,
    },

    /// Scrape documents from one or more sources (crawl + download combined)
    Scrape {
        /// Source IDs to scrape (can specify multiple, or use --all)
        source_ids: Vec<String>,
        /// Scrape all configured sources
        #[arg(short, long)]
        all: bool,
        /// Number of download workers (default: 4)
        #[arg(short, long, default_value = "4")]
        workers: usize,
        /// Limit number of documents to download per source (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Show detailed progress for each file
        #[arg(short = 'P', long)]
        progress: bool,
    },

    /// Show system status
    Status,

    /// Process documents with OCR and extract text
    Ocr {
        /// Source ID (optional, processes all sources if not specified)
        source_id: Option<String>,
        /// Number of workers (default: 2)
        #[arg(short, long, default_value = "2")]
        workers: usize,
        /// Limit number of documents to process (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Extract URLs from documents and add to crawl queue
        #[arg(long)]
        extract_urls: bool,
    },

    /// Check if required OCR tools are installed
    OcrCheck,

    /// Compare OCR backends on an image or PDF
    OcrCompare {
        /// Image file or PDF to OCR
        file: std::path::PathBuf,
        /// Page range (e.g., "1", "1-5", "1,3,5-10"). Default: all pages
        #[arg(short, long)]
        pages: Option<String>,
        /// Backends to compare (e.g. tesseract,deepseek:gpu,deepseek:cpu,paddleocr:gpu)
        #[arg(short, long, default_value = "tesseract")]
        backends: String,
        /// DeepSeek binary path (if not in PATH)
        #[arg(long)]
        deepseek_path: Option<std::path::PathBuf>,
    },

    /// Start web server to browse documents
    Serve {
        /// Address to bind to: PORT, HOST, or HOST:PORT (default: 127.0.0.1:3030)
        #[arg(default_value = "127.0.0.1:3030")]
        bind: String,
    },

    /// Refresh metadata for existing documents (server date, original filename)
    Refresh {
        /// Source ID (optional, refreshes all sources if not specified)
        source_id: Option<String>,
        /// Number of workers (default: 4)
        #[arg(short, long, default_value = "4")]
        workers: usize,
        /// Limit number of documents to refresh (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Force full re-download even if ETag matches
        #[arg(short, long)]
        force: bool,
    },

    /// Annotate documents using local LLM (generates synopsis and tags)
    Annotate {
        /// Source ID (optional, processes all sources if not specified)
        source_id: Option<String>,
        /// Limit number of documents to process (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
    },

    /// Detect and estimate publication dates for documents
    DetectDates {
        /// Source ID (optional, processes all sources if not specified)
        source_id: Option<String>,
        /// Limit number of documents to process (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Only show what would be detected, don't update database
        #[arg(long)]
        dry_run: bool,
    },

    /// List available LLM models
    LlmModels,

    /// Extract contents from container files (zip archives, emails) as virtual files
    Archive {
        /// Source ID (optional, processes all sources if not specified)
        source_id: Option<String>,
        /// Limit number of containers to process (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Also run OCR on extracted virtual files
        #[arg(long)]
        ocr: bool,
    },

    /// List documents in the repository
    Ls {
        /// Source ID to filter by
        #[arg(short, long)]
        source: Option<String>,
        /// Filter by tag
        #[arg(short, long)]
        tag: Option<String>,
        /// Filter by file type (pdf, image, text, document, etc)
        #[arg(short = 'T', long)]
        type_filter: Option<String>,
        /// Limit number of results
        #[arg(short, long, default_value = "50")]
        limit: usize,
        /// Output format (table, json, ids)
        #[arg(short, long, default_value = "table")]
        format: String,
    },

    /// Show document metadata and info
    Info {
        /// Document ID or search term
        doc_id: String,
    },

    /// Output document content to stdout
    Read {
        /// Document ID
        doc_id: String,
        /// Output extracted text instead of binary file
        #[arg(short, long)]
        text: bool,
    },

    /// Search documents by content or metadata
    Search {
        /// Search query
        query: String,
        /// Source ID to filter by
        #[arg(short, long)]
        source: Option<String>,
        /// Limit number of results
        #[arg(short, long, default_value = "20")]
        limit: usize,
    },

    /// Import documents from WARC (Web Archive) files
    Import {
        /// WARC file(s) to import (supports .warc and .warc.gz)
        files: Vec<PathBuf>,
        /// Source ID to associate imported documents with (auto-detected from URLs if not specified)
        #[arg(short, long)]
        source: Option<String>,
        /// URL pattern filter (regex, e.g. "\.pdf$" for PDFs only)
        #[arg(short, long)]
        filter: Option<String>,
        /// Limit number of records to import (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Limit number of records to scan (0 = unlimited). Useful for testing with large archives.
        #[arg(long, default_value = "0")]
        scan_limit: usize,
        /// Dry run - show what would be imported without saving
        #[arg(long)]
        dry_run: bool,
    },

    /// Discover new document URLs by analyzing patterns in existing URLs
    Discover {
        /// Source ID to analyze and generate URLs for
        source_id: String,
        /// Limit number of candidate URLs to generate (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Show what would be discovered without adding to queue
        #[arg(long)]
        dry_run: bool,
        /// Minimum number of URL examples before generating candidates
        #[arg(long, default_value = "3")]
        min_examples: usize,
    },

    /// Test browser-based fetching (requires --features browser)
    #[cfg(feature = "browser")]
    BrowserTest {
        /// URL to fetch
        url: String,
        /// Run in headed mode (show browser window)
        #[arg(long)]
        headed: bool,
        /// Browser engine: stealth, cookies, standard
        #[arg(short, long, default_value = "stealth")]
        engine: String,
        /// Proxy server (e.g., socks5://127.0.0.1:1080)
        #[arg(long)]
        proxy: Option<String>,
        /// Remote browser URL (e.g., ws://localhost:9222)
        #[arg(long)]
        browser_url: Option<String>,
        /// Cookies file for cookie injection
        #[arg(long)]
        cookies: Option<std::path::PathBuf>,
        /// Save cookies to file after fetching
        #[arg(long)]
        save_cookies: Option<std::path::PathBuf>,
        /// Output file (defaults to stdout)
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
        /// Fetch as binary (PDF, images) using JavaScript fetch
        #[arg(long)]
        binary: bool,
        /// Context URL to establish session before binary fetch (e.g., for Akamai-protected sites)
        #[arg(long)]
        context_url: Option<String>,
    },
}

#[derive(Subcommand)]
enum SourceCommands {
    /// List configured sources
    List,
    /// Rename a source (updates all associated documents)
    Rename {
        /// Current source ID
        old_id: String,
        /// New source ID
        new_id: String,
        /// Skip confirmation prompt
        #[arg(long)]
        confirm: bool,
    },
}

#[derive(Subcommand)]
enum StateCommands {
    /// Show crawl status
    Status {
        /// Source ID (optional, shows all if not specified)
        source_id: Option<String>,
    },
    /// Clear crawl state for a source
    Clear {
        /// Source ID
        source_id: String,
        /// Confirm clearing
        #[arg(long)]
        confirm: bool,
    },
}

/// Run the CLI.
pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mut settings = load_settings().await;
    if let Some(data_dir) = cli.data_dir {
        settings = Settings::with_data_dir(data_dir);
    }

    match cli.command {
        Commands::Init => cmd_init(&settings).await,
        Commands::Source { command } => match command {
            SourceCommands::List => cmd_source_list(&settings).await,
            SourceCommands::Rename {
                old_id,
                new_id,
                confirm,
            } => cmd_source_rename(&settings, &old_id, &new_id, confirm).await,
        },
        Commands::Crawl { source_id, limit } => cmd_crawl(&settings, &source_id, limit).await,
        Commands::Download {
            source_id,
            workers,
            limit,
            progress,
        } => cmd_download(&settings, source_id.as_deref(), workers, limit, progress).await,
        Commands::State { command } => match command {
            StateCommands::Status { source_id } => cmd_crawl_status(&settings, source_id).await,
            StateCommands::Clear { source_id, confirm } => {
                cmd_crawl_clear(&settings, &source_id, confirm).await
            }
        },
        Commands::Scrape {
            source_ids,
            all,
            workers,
            limit,
            progress,
        } => cmd_scrape(&settings, &source_ids, all, workers, limit, progress).await,
        Commands::Status => cmd_status(&settings).await,
        Commands::Ocr {
            source_id,
            workers,
            limit,
            ..
        } => cmd_ocr(&settings, source_id.as_deref(), workers, limit).await,
        Commands::OcrCheck => cmd_ocr_check().await,
        Commands::OcrCompare {
            file,
            pages,
            backends,
            deepseek_path,
        } => cmd_ocr_compare(&file, pages.as_deref(), &backends, deepseek_path).await,
        Commands::Serve { bind } => cmd_serve(&settings, &bind).await,
        Commands::Refresh {
            source_id,
            workers,
            limit,
            force,
        } => cmd_refresh(&settings, source_id.as_deref(), workers, limit, force).await,
        Commands::Annotate { source_id, limit } => {
            cmd_annotate(&settings, source_id.as_deref(), limit).await
        }
        Commands::DetectDates {
            source_id,
            limit,
            dry_run,
        } => cmd_detect_dates(&settings, source_id.as_deref(), limit, dry_run).await,
        Commands::LlmModels => cmd_llm_models(&settings).await,
        Commands::Archive {
            source_id,
            limit,
            ocr,
        } => cmd_archive(&settings, source_id.as_deref(), limit, ocr).await,
        Commands::Ls {
            source,
            tag,
            type_filter,
            limit,
            format,
        } => {
            cmd_ls(
                &settings,
                source.as_deref(),
                tag.as_deref(),
                type_filter.as_deref(),
                limit,
                &format,
            )
            .await
        }
        Commands::Info { doc_id } => cmd_info(&settings, &doc_id).await,
        Commands::Read { doc_id, text } => cmd_read(&settings, &doc_id, text).await,
        Commands::Search {
            query,
            source,
            limit,
        } => cmd_search(&settings, &query, source.as_deref(), limit).await,
        Commands::Import {
            files,
            source,
            filter,
            limit,
            scan_limit,
            dry_run,
        } => cmd_import(&settings, &files, source.as_deref(), filter.as_deref(), limit, scan_limit, dry_run).await,
        Commands::Discover {
            source_id,
            limit,
            dry_run,
            min_examples,
        } => cmd_discover(&settings, &source_id, limit, dry_run, min_examples).await,
        #[cfg(feature = "browser")]
        Commands::BrowserTest {
            url,
            headed,
            engine,
            proxy,
            browser_url,
            cookies,
            save_cookies,
            output,
            binary,
            context_url,
        } => {
            cmd_browser_test(
                &url,
                headed,
                &engine,
                proxy,
                browser_url,
                cookies,
                save_cookies,
                output,
                binary,
                context_url,
            )
            .await
        }
    }
}

async fn cmd_init(settings: &Settings) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Initialize repositories
    let db_path = settings.database_path();
    let _doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;
    let source_repo = SourceRepository::new(&db_path)?;
    let _crawl_repo = CrawlRepository::new(&db_path)?;

    // Load sources from config
    let config = Config::load().await;

    let mut sources_added = 0;
    for (source_id, scraper_config) in &config.scrapers {
        if !source_repo.exists(source_id)? {
            let source = Source::new(
                source_id.clone(),
                SourceType::Custom,
                scraper_config.name_or(source_id),
                scraper_config.base_url_or(""),
            );
            source_repo.save(&source)?;
            sources_added += 1;
            println!("  {} Added source: {}", style("✓").green(), source.name);
        }
    }

    if sources_added == 0 && config.scrapers.is_empty() {
        println!(
            "{} No scrapers configured in foiacquire.json",
            style("!").yellow()
        );
        println!("  Copy foiacquire.example.json to foiacquire.json to get started");
    }

    println!(
        "{} Initialized FOIAcquire in {}",
        style("✓").green(),
        settings.data_dir.display()
    );

    Ok(())
}

async fn cmd_source_list(settings: &Settings) -> anyhow::Result<()> {
    let source_repo = SourceRepository::new(&settings.database_path())?;
    let sources = source_repo.get_all()?;

    if sources.is_empty() {
        println!(
            "{} No sources configured. Run 'foiacquire init' first.",
            style("!").yellow()
        );
        return Ok(());
    }

    println!("\n{}", style("FOIA Sources").bold());
    println!("{}", "-".repeat(60));
    println!("{:<15} {:<25} {:<10} Last Scraped", "ID", "Name", "Type");
    println!("{}", "-".repeat(60));

    for source in sources {
        let last_scraped = source
            .last_scraped
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "Never".to_string());

        println!(
            "{:<15} {:<25} {:<10} {}",
            source.id,
            truncate(&source.name, 24),
            source.source_type.as_str(),
            last_scraped
        );
    }

    Ok(())
}

async fn cmd_source_rename(
    settings: &Settings,
    old_id: &str,
    new_id: &str,
    confirm: bool,
) -> anyhow::Result<()> {
    use std::io::{self, Write};

    let db_path = settings.database_path();
    let source_repo = SourceRepository::new(&db_path)?;
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;
    let crawl_repo = CrawlRepository::new(&db_path)?;

    // Check old source exists
    let old_source = source_repo.get(old_id)?;
    if old_source.is_none() {
        println!(
            "{} Source '{}' not found",
            style("✗").red(),
            old_id
        );
        return Ok(());
    }

    // Check new source doesn't exist
    if source_repo.get(new_id)?.is_some() {
        println!(
            "{} Source '{}' already exists. Use a different name or delete it first.",
            style("✗").red(),
            new_id
        );
        return Ok(());
    }

    // Count affected documents
    let doc_count = doc_repo.count_by_source(old_id)?;
    let crawl_count = crawl_repo.count_by_source(old_id)?;

    println!(
        "\n{} Rename source '{}' → '{}'",
        style("→").cyan(),
        style(old_id).yellow(),
        style(new_id).green()
    );
    println!("  Documents to update: {}", doc_count);
    println!("  Crawl URLs to update: {}", crawl_count);

    // Confirm
    if !confirm {
        print!("\nProceed? [y/N] ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("{} Cancelled", style("!").yellow());
            return Ok(());
        }
    }

    // Perform the rename using direct SQL for atomicity
    let conn = rusqlite::Connection::open(&db_path)?;
    conn.execute("BEGIN TRANSACTION", [])?;

    // Update documents
    let docs_updated = conn.execute(
        "UPDATE documents SET source_id = ?1 WHERE source_id = ?2",
        rusqlite::params![new_id, old_id],
    )?;

    // Update crawl_urls
    let crawls_updated = conn.execute(
        "UPDATE crawl_urls SET source_id = ?1 WHERE source_id = ?2",
        rusqlite::params![new_id, old_id],
    )?;

    // Update crawl_state
    conn.execute(
        "UPDATE crawl_state SET source_id = ?1 WHERE source_id = ?2",
        rusqlite::params![new_id, old_id],
    )?;

    // Update source itself
    conn.execute(
        "UPDATE sources SET id = ?1 WHERE id = ?2",
        rusqlite::params![new_id, old_id],
    )?;

    conn.execute("COMMIT", [])?;

    println!(
        "\n{} Renamed '{}' → '{}'",
        style("✓").green(),
        old_id,
        new_id
    );
    println!("  Documents updated: {}", docs_updated);
    println!("  Crawl URLs updated: {}", crawls_updated);

    Ok(())
}

async fn cmd_crawl_status(settings: &Settings, source_id: Option<String>) -> anyhow::Result<()> {
    let source_repo = SourceRepository::new(&settings.database_path())?;
    let crawl_repo = CrawlRepository::new(&settings.database_path())?;

    let sources = match source_id {
        Some(id) => source_repo.get(&id)?.into_iter().collect(),
        None => source_repo.get_all()?,
    };

    if sources.is_empty() {
        println!("{} No sources found", style("!").yellow());
        return Ok(());
    }

    for source in sources {
        let state = crawl_repo.get_crawl_state(&source.id)?;
        let stats = crawl_repo.get_request_stats(&source.id)?;

        println!(
            "\n{}",
            style(format!("Crawl Status: {}", source.name)).bold()
        );
        println!("{}", "-".repeat(40));

        let status_str = if state.is_complete() {
            style("Complete").green().to_string()
        } else if state.needs_resume() {
            style("Needs Resume").yellow().to_string()
        } else {
            style("Not Started").dim().to_string()
        };

        println!("{:<20} {}", "Status:", status_str);

        if let Some(started) = state.last_crawl_started {
            println!(
                "{:<20} {}",
                "Last Started:",
                started.format("%Y-%m-%d %H:%M")
            );
        }
        if let Some(completed) = state.last_crawl_completed {
            println!(
                "{:<20} {}",
                "Last Completed:",
                completed.format("%Y-%m-%d %H:%M")
            );
        }

        println!("{:<20} {}", "URLs Discovered:", state.urls_discovered);
        println!("{:<20} {}", "URLs Fetched:", state.urls_fetched);
        println!("{:<20} {}", "URLs Pending:", state.urls_pending);
        println!("{:<20} {}", "URLs Failed:", state.urls_failed);

        if stats.total_requests > 0 {
            println!();
            println!("{:<20} {}", "Total Requests:", stats.total_requests);
            println!("{:<20} {}", "  Success (200):", stats.success_200);
            println!("{:<20} {}", "  Not Modified (304):", stats.not_modified_304);
            println!("{:<20} {}", "  Errors:", stats.errors);
            println!(
                "{:<20} {:.1}ms",
                "Avg Response Time:", stats.avg_duration_ms
            );
            println!(
                "{:<20} {}",
                "Total Downloaded:",
                format_bytes(stats.total_bytes)
            );
        }
    }

    Ok(())
}

async fn cmd_crawl_clear(
    settings: &Settings,
    source_id: &str,
    confirm: bool,
) -> anyhow::Result<()> {
    if !confirm {
        println!(
            "{} This will clear ALL crawl state for '{}', including fetched URLs.",
            style("!").yellow(),
            source_id
        );
        println!("  The next crawl will start completely fresh.");
        println!("  Use --confirm to proceed.");
        return Ok(());
    }

    let crawl_repo = CrawlRepository::new(&settings.database_path())?;
    crawl_repo.clear_source_all(source_id)?;

    println!(
        "{} Cleared all crawl state for '{}'",
        style("✓").green(),
        source_id
    );

    Ok(())
}

async fn cmd_scrape(
    settings: &Settings,
    source_ids: &[String],
    all: bool,
    workers: usize,
    limit: usize,
    show_progress: bool,
) -> anyhow::Result<()> {
    let config = Config::load().await;

    // Create shared rate limiter and load persisted state
    let rate_limiter = Arc::new(RateLimiter::new());
    let db_path = settings.database_path();
    if let Err(e) = load_rate_limit_state(&rate_limiter, &db_path).await {
        tracing::warn!("Failed to load rate limit state: {}", e);
    }

    // Determine which sources to scrape
    let sources_to_scrape: Vec<String> = if all {
        config.scrapers.keys().cloned().collect()
    } else if source_ids.is_empty() {
        println!(
            "{} No sources specified. Use --all or provide source IDs.",
            style("✗").red()
        );
        println!(
            "Available sources: {}",
            config
                .scrapers
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
        return Ok(());
    } else {
        source_ids.to_vec()
    };

    // Initialize TUI with fixed status pane at top (1 header + 1 line per source)
    let num_status_lines = (sources_to_scrape.len() + 1).min(10) as u16; // Cap at 10 lines
    let tui_guard = crate::cli::tui::TuiGuard::new(num_status_lines)?;

    // Set header
    let _ = crate::cli::tui::set_status(
        0,
        &format!(
            "{} Scraping {} source{}...",
            style("→").cyan(),
            sources_to_scrape.len(),
            if sources_to_scrape.len() == 1 {
                ""
            } else {
                "s"
            }
        ),
    );

    // Initialize status lines for each source
    let source_lines: std::collections::HashMap<String, u16> = sources_to_scrape
        .iter()
        .enumerate()
        .take(9) // Only show first 9 sources in status (line 0 is header)
        .map(|(i, s)| (s.clone(), (i + 1) as u16))
        .collect();

    for (source_id, line) in &source_lines {
        let _ = crate::cli::tui::set_status(
            *line,
            &format!("  {} {} waiting...", style("○").dim(), source_id),
        );
    }

    if sources_to_scrape.len() == 1 {
        // Single source - run directly
        let source_id = &sources_to_scrape[0];
        let line = source_lines.get(source_id).copied();
        cmd_scrape_single_tui(
            settings,
            source_id,
            workers,
            limit,
            show_progress,
            line,
            tui_guard.is_active(),
            Some(rate_limiter.clone()),
        )
        .await?;
    } else {
        // Multiple sources - run in parallel
        let mut handles = Vec::new();
        for source_id in &sources_to_scrape {
            let settings = settings.clone();
            let source_id_clone = source_id.clone();
            let line = source_lines.get(source_id).copied();
            let tui_active = tui_guard.is_active();
            let rate_limiter_clone = rate_limiter.clone();
            let handle = tokio::spawn(async move {
                cmd_scrape_single_tui(
                    &settings,
                    &source_id_clone,
                    workers,
                    limit,
                    show_progress,
                    line,
                    tui_active,
                    Some(rate_limiter_clone),
                )
                .await
            });
            handles.push((source_id.clone(), handle));
        }

        // Wait for all to complete
        let mut errors = Vec::new();
        for (source_id, handle) in handles {
            match handle.await {
                Ok(Ok(())) => {
                    if let Some(&line) = source_lines.get(&source_id) {
                        let _ = crate::cli::tui::set_status(
                            line,
                            &format!("  {} {} done", style("✓").green(), source_id),
                        );
                    }
                }
                Ok(Err(e)) => {
                    if let Some(&line) = source_lines.get(&source_id) {
                        let _ = crate::cli::tui::set_status(
                            line,
                            &format!("  {} {} error", style("✗").red(), source_id),
                        );
                    }
                    errors.push(format!("{}: {}", source_id, e));
                }
                Err(e) => {
                    errors.push(format!("{}: task panicked: {}", source_id, e));
                }
            }
        }

        if !errors.is_empty() {
            let _ =
                crate::cli::tui::log(&format!("\n{} Some scrapers failed:", style("!").yellow()));
            for err in &errors {
                let _ = crate::cli::tui::log(&format!("  - {}", err));
            }
        }
    }

    // Update header to show complete
    let _ = crate::cli::tui::set_status(0, &format!("{} Scraping complete", style("✓").green()));

    // Save rate limit state to database
    if let Err(e) = save_rate_limit_state(&rate_limiter, &db_path).await {
        tracing::warn!("Failed to save rate limit state: {}", e);
    }

    // TUI cleanup happens automatically when tui_guard is dropped
    drop(tui_guard);

    Ok(())
}

/// Scrape a single source with TUI status updates.
#[allow(clippy::too_many_arguments)]
async fn cmd_scrape_single_tui(
    settings: &Settings,
    source_id: &str,
    workers: usize,
    limit: usize,
    _show_progress: bool,
    status_line: Option<u16>,
    tui_active: bool,
    rate_limiter: Option<Arc<RateLimiter>>,
) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Helper to update status line or log
    let update_status = |msg: &str| {
        if let Some(line) = status_line {
            let _ = crate::cli::tui::set_status(line, &format!("  {} {}", style("●").cyan(), msg));
        }
    };

    let log_msg = |msg: &str| {
        if tui_active {
            let _ = crate::cli::tui::log(msg);
        } else {
            println!("{}", msg);
        }
    };

    // Load scraper config
    let config = Config::load().await;
    let mut scraper_config = match config.scrapers.get(source_id) {
        Some(c) => c.clone(),
        None => {
            log_msg(&format!(
                "{} No scraper configured for '{}'",
                style("✗").red(),
                source_id
            ));
            return Ok(());
        }
    };

    update_status(&format!("{} loading config...", source_id));

    // Expand search terms using LLM if configured
    if scraper_config.discovery.expand_search_terms
        && !scraper_config.discovery.search_queries.is_empty()
    {
        let llm_config = config.llm.clone();
        let llm = LlmClient::new(llm_config);

        if llm.is_available().await {
            update_status(&format!("{} expanding search terms...", source_id));
            let domain = scraper_config.name.as_deref().unwrap_or(source_id);
            if let Ok(expanded) = llm
                .expand_search_terms(&scraper_config.discovery.search_queries, domain)
                .await
            {
                let mut all_terms: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for term in &scraper_config.discovery.search_queries {
                    all_terms.insert(term.to_lowercase());
                }
                for term in expanded {
                    all_terms.insert(term.to_lowercase());
                }
                scraper_config.discovery.search_queries = all_terms.into_iter().collect();
            }
        }
    }

    let db_path = settings.database_path();
    let source_repo = SourceRepository::new(&db_path)?;
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;
    let crawl_repo = Arc::new(Mutex::new(CrawlRepository::new(&db_path)?));

    doc_repo.migrate_storage()?;

    // Auto-register source if not in database
    let source = match source_repo.get(source_id)? {
        Some(s) => s,
        None => {
            let new_source = Source::new(
                source_id.to_string(),
                SourceType::Custom,
                scraper_config.name_or(source_id),
                scraper_config.base_url_or(""),
            );
            source_repo.save(&new_source)?;
            new_source
        }
    };

    // Check crawl state
    {
        let repo = crawl_repo.lock().await;
        let (config_changed, _) = repo.check_config_changed(source_id, &scraper_config)?;
        if config_changed {
            repo.store_config_hash(source_id, &scraper_config)?;
        }
        if !config_changed {
            repo.store_config_hash(source_id, &scraper_config)?;
        }
    }

    update_status(&format!("{} starting...", source_id));

    // Create scraper and start streaming
    let refresh_ttl_days = config.get_refresh_ttl_days(source_id);
    // Clone rate limiter - RateLimiter uses Arc internally so cloning shares state
    let limiter_opt = rate_limiter.as_ref().map(|r| (**r).clone());
    let scraper = ConfigurableScraper::with_rate_limiter(
        source.clone(),
        scraper_config,
        Some(crawl_repo.clone()),
        Duration::from_millis(settings.request_delay_ms),
        refresh_ttl_days,
        limiter_opt,
    );

    let stream = scraper.scrape_stream(workers).await;
    let mut rx = stream.receiver;

    let mut count = 0u64;
    let mut new_this_session = 0u64;

    while let Some(result) = rx.recv().await {
        if result.not_modified {
            count += 1;
            update_status(&format!("{} {} processed", source_id, count));
            continue;
        }

        let content = match &result.content {
            Some(c) => c,
            None => continue,
        };

        // Save document using helper
        super::helpers::save_scraped_document(
            &doc_repo,
            content,
            &result,
            &source.id,
            &settings.documents_dir,
        )?;

        count += 1;
        new_this_session += 1;
        update_status(&format!(
            "{} {} processed ({} new)",
            source_id, count, new_this_session
        ));

        if limit > 0 && new_this_session as usize >= limit {
            break;
        }
    }

    // Update last scraped
    let mut source = source;
    source.last_scraped = Some(chrono::Utc::now());
    source_repo.save(&source)?;

    // Final status
    if let Some(line) = status_line {
        let _ = crate::cli::tui::set_status(
            line,
            &format!("  {} {} {} docs", style("✓").green(), source_id, count),
        );
    }

    Ok(())
}

/// Crawl a source to discover URLs (does not download).
async fn cmd_crawl(settings: &Settings, source_id: &str, _limit: usize) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Load scraper config
    let config = Config::load().await;
    let scraper_config = match config.scrapers.get(source_id) {
        Some(c) => c.clone(),
        None => {
            println!(
                "{} No scraper configured for '{}'",
                style("✗").red(),
                source_id
            );
            return Ok(());
        }
    };

    let db_path = settings.database_path();
    let source_repo = SourceRepository::new(&db_path)?;
    let crawl_repo = Arc::new(Mutex::new(CrawlRepository::new(&db_path)?));

    // Auto-register source
    let source = match source_repo.get(source_id)? {
        Some(s) => s,
        None => {
            let new_source = Source::new(
                source_id.to_string(),
                SourceType::Custom,
                scraper_config.name_or(source_id),
                scraper_config.base_url_or(""),
            );
            source_repo.save(&new_source)?;
            crate::cli::progress::progress_println(&format!(
                "  {} Registered source: {}",
                style("✓").green(),
                new_source.name
            ));
            new_source
        }
    };

    // Check crawl state
    {
        let repo = crawl_repo.lock().await;
        let (config_changed, _has_pending_urls) =
            repo.check_config_changed(source_id, &scraper_config)?;

        // Update config hash (we never clear discovered URLs - they're valuable!)
        repo.store_config_hash(source_id, &scraper_config)?;

        let state = repo.get_crawl_state(source_id)?;
        if state.needs_resume() {
            println!(
                "{} Resuming crawl ({} pending URLs)",
                style("→").yellow(),
                state.urls_pending
            );
        }

        // Silence unused variable warning
        let _ = config_changed;
    }

    // Create scraper for discovery
    let refresh_ttl_days = config.get_refresh_ttl_days(source_id);
    let scraper = ConfigurableScraper::new(
        source.clone(),
        scraper_config,
        Some(crawl_repo.clone()),
        Duration::from_millis(settings.request_delay_ms),
        refresh_ttl_days,
    );

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    pb.set_message(format!("Discovering URLs from {}...", source.name));

    let urls = scraper.discover().await;
    pb.finish_and_clear();

    let state = {
        let repo = crawl_repo.lock().await;
        repo.get_crawl_state(source_id)?
    };

    println!(
        "{} Discovered {} URLs from {} ({} pending)",
        style("✓").green(),
        urls.len(),
        source.name,
        state.urls_pending
    );

    if state.urls_pending > 0 {
        println!(
            "  {} Run 'foiacquire download {}' to download pending documents",
            style("→").dim(),
            source_id
        );
    }

    Ok(())
}

/// Download pending documents from the queue.
async fn cmd_download(
    settings: &Settings,
    source_id: Option<&str>,
    workers: usize,
    limit: usize,
    show_progress: bool,
) -> anyhow::Result<()> {
    use crate::services::{DownloadConfig, DownloadEvent, DownloadService};
    use tokio::sync::mpsc;

    settings.ensure_directories()?;

    let db_path = settings.database_path();
    let doc_repo = Arc::new(DocumentRepository::new(&db_path, &settings.documents_dir)?);
    let crawl_repo = Arc::new(CrawlRepository::new(&db_path)?);

    doc_repo.migrate_storage()?;

    // Check for pending work
    let initial_pending = get_pending_count(&db_path, source_id)?;

    if initial_pending == 0 {
        println!("{} No pending documents to download", style("!").yellow());
        if let Some(sid) = source_id {
            println!(
                "  {} Run 'foiacquire crawl {}' to discover new URLs",
                style("→").dim(),
                sid
            );
        }
        return Ok(());
    }

    println!(
        "{} Starting {} download workers ({} pending documents)",
        style("→").cyan(),
        workers,
        initial_pending
    );

    // Create service
    let service = DownloadService::new(
        doc_repo,
        crawl_repo,
        DownloadConfig {
            documents_dir: settings.documents_dir.clone(),
            request_timeout: Duration::from_secs(settings.request_timeout),
            request_delay: Duration::from_millis(settings.request_delay_ms),
        },
    );

    // Event channel for progress updates
    let (event_tx, mut event_rx) = mpsc::channel::<DownloadEvent>(100);

    // Set up progress display (UI concern)
    let progress_display = if show_progress {
        Some(Arc::new(DownloadProgress::new(workers, initial_pending)))
    } else {
        None
    };

    // Spawn event handler task (UI layer)
    let progress_clone = progress_display.clone();
    let event_handler = tokio::spawn(async move {
        let mut downloaded = 0usize;
        let mut skipped = 0usize;

        while let Some(event) = event_rx.recv().await {
            match event {
                DownloadEvent::Started {
                    worker_id,
                    filename,
                    ..
                } => {
                    if let Some(ref progress) = progress_clone {
                        progress.start_download(worker_id, &filename, None).await;
                    }
                }
                DownloadEvent::Progress {
                    worker_id,
                    bytes,
                    total,
                } => {
                    if let Some(ref progress) = progress_clone {
                        if let Some(t) = total {
                            progress.start_download(worker_id, "", Some(t)).await;
                        }
                        progress.update_progress(worker_id, bytes).await;
                    }
                }
                DownloadEvent::Completed { worker_id, .. } => {
                    downloaded += 1;
                    if let Some(ref progress) = progress_clone {
                        progress.set_summary(downloaded, skipped);
                        progress.finish_download(worker_id, true).await;
                    }
                }
                DownloadEvent::Unchanged { worker_id, .. } => {
                    skipped += 1;
                    if let Some(ref progress) = progress_clone {
                        progress.set_summary(downloaded, skipped);
                        progress.finish_download(worker_id, true).await;
                    }
                }
                DownloadEvent::Failed { worker_id, .. } => {
                    if let Some(ref progress) = progress_clone {
                        progress.finish_download(worker_id, false).await;
                    }
                }
            }
        }
    });

    // Run download service (business logic)
    let limit_opt = if limit > 0 { Some(limit) } else { None };
    let result = service
        .download(source_id, workers, limit_opt, event_tx)
        .await?;

    // Wait for event handler to finish
    let _ = event_handler.await;

    // Clean up progress display
    if let Some(ref progress) = progress_display {
        progress.finish().await;
    }

    // Print results (UI layer)
    println!(
        "{} Downloaded {} documents",
        style("✓").green(),
        result.downloaded
    );

    if result.skipped > 0 {
        println!(
            "  {} {} unchanged (304 Not Modified)",
            style("→").dim(),
            result.skipped
        );
    }

    if result.remaining > 0 {
        println!(
            "  {} {} URLs still pending",
            style("!").yellow(),
            result.remaining
        );
    }

    Ok(())
}

/// Get pending document count for a source or all sources.
fn get_pending_count(db_path: &std::path::Path, source_id: Option<&str>) -> anyhow::Result<u64> {
    let crawl_repo = CrawlRepository::new(db_path)?;

    if let Some(sid) = source_id {
        Ok(crawl_repo.get_crawl_state(sid)?.urls_pending)
    } else {
        let source_repo = SourceRepository::new(db_path)?;
        let sources = source_repo.get_all()?;
        let mut total = 0u64;
        for s in sources {
            total += crawl_repo.get_crawl_state(&s.id)?.urls_pending;
        }
        Ok(total)
    }
}

async fn cmd_status(settings: &Settings) -> anyhow::Result<()> {
    let db_path = settings.database_path();

    let doc_repo = match DocumentRepository::new(&db_path, &settings.documents_dir) {
        Ok(r) => r,
        Err(_) => {
            println!(
                "{} System not initialized. Run 'foiacquire init' first.",
                style("!").yellow()
            );
            return Ok(());
        }
    };

    let source_repo = SourceRepository::new(&db_path)?;

    println!("\n{}", style("FOIAcquire Status").bold());
    println!("{}", "-".repeat(40));
    println!("{:<20} {}", "Data Directory:", settings.data_dir.display());
    println!("{:<20} {}", "Sources:", source_repo.get_all()?.len());
    println!("{:<20} {}", "Total Documents:", doc_repo.count()?);

    // Count by status
    for status in [
        DocumentStatus::Pending,
        DocumentStatus::Downloaded,
        DocumentStatus::OcrComplete,
        DocumentStatus::Indexed,
        DocumentStatus::Failed,
    ] {
        let count = doc_repo.get_by_status(status)?.len();
        if count > 0 {
            println!("{:<20} {}", format!("  {}:", status.as_str()), count);
        }
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max - 3])
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.2} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Check if required OCR tools are installed.
async fn cmd_ocr_check() -> anyhow::Result<()> {
    use crate::ocr::{DeepSeekBackend, OcrBackend, TesseractBackend};

    println!("\n{}", style("OCR Tool Status").bold());
    println!("{}", "-".repeat(50));

    // Check legacy tools
    let tools = TextExtractor::check_tools();
    println!("\n{}", style("Traditional Tools:").cyan());
    let mut all_found = true;

    for (tool, available) in &tools {
        let status = if *available {
            style("✓ found").green()
        } else {
            all_found = false;
            style("✗ not found").red()
        };
        println!("  {:<15} {}", tool, status);
    }

    // Check new backends
    println!("\n{}", style("OCR Backends:").cyan());

    // Tesseract (always available)
    let tesseract = TesseractBackend::new();
    let tesseract_status = if tesseract.is_available() {
        style("✓ available").green()
    } else {
        style("✗ not available").red()
    };
    println!("  {:<15} {}", "Tesseract", tesseract_status);
    if !tesseract.is_available() {
        println!(
            "                  {}",
            style(tesseract.availability_hint()).dim()
        );
    }

    // OCRS (models auto-download on first use)
    #[cfg(feature = "ocr-ocrs")]
    {
        use crate::ocr::OcrsBackend;
        let ocrs = OcrsBackend::new();
        let ocrs_status = if ocrs.is_available() {
            style("✓ available").green()
        } else {
            style("○ models will auto-download").yellow()
        };
        println!("  {:<15} {}", "OCRS", ocrs_status);
        println!(
            "                  {}",
            style(ocrs.availability_hint()).dim()
        );
    }
    #[cfg(not(feature = "ocr-ocrs"))]
    {
        println!(
            "  {:<15} {}",
            "OCRS",
            style("not compiled (enable ocr-ocrs feature)").dim()
        );
    }

    // PaddleOCR (models auto-download on first use)
    #[cfg(feature = "ocr-paddle")]
    {
        use crate::ocr::PaddleBackend;
        let paddle = PaddleBackend::new();
        let paddle_status = if paddle.is_available() {
            style("✓ available").green()
        } else {
            style("○ models will auto-download").yellow()
        };
        println!("  {:<15} {}", "PaddleOCR", paddle_status);
        println!(
            "                  {}",
            style(paddle.availability_hint()).dim()
        );
    }
    #[cfg(not(feature = "ocr-paddle"))]
    {
        println!(
            "  {:<15} {}",
            "PaddleOCR",
            style("not compiled (enable ocr-paddle feature)").dim()
        );
    }

    // DeepSeek (always available but requires binary)
    let deepseek = DeepSeekBackend::new();
    let deepseek_status = if deepseek.is_available() {
        style("✓ available").green()
    } else {
        style("○ not installed").yellow()
    };
    println!("  {:<15} {}", "DeepSeek", deepseek_status);
    if !deepseek.is_available() {
        println!(
            "                  {}",
            style("Install: https://github.com/TimmyOVO/deepseek-ocr.rs").dim()
        );
    }

    println!();

    if all_found {
        println!("{} Basic OCR tools are available", style("✓").green());
    } else {
        println!(
            "{} Some tools are missing. Install them for full OCR support:",
            style("!").yellow()
        );
        println!("  - pdftotext, pdftoppm, pdfinfo: poppler-utils package");
        println!("  - tesseract: tesseract-ocr package");
    }

    Ok(())
}

/// Get PDF page count using pdfinfo.
fn get_pdf_page_count(file: &std::path::Path) -> anyhow::Result<u32> {
    use std::process::Command;
    let output = Command::new("pdfinfo").arg(file).output()?;

    if !output.status.success() {
        anyhow::bail!("pdfinfo failed");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.starts_with("Pages:") {
            let count = line
                .split(':')
                .nth(1)
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(1);
            return Ok(count);
        }
    }
    Ok(1)
}

/// Parse a page range string like "1", "1-5", "1,3,5-10" into a list of page numbers.
fn parse_page_range(range_str: &str, max_pages: u32) -> Vec<u32> {
    let mut pages = Vec::new();

    for part in range_str.split(',') {
        let part = part.trim();
        if part.contains('-') {
            // Range like "1-5"
            let mut iter = part.split('-');
            let start: u32 = iter.next().and_then(|s| s.trim().parse().ok()).unwrap_or(1);
            let end: u32 = iter
                .next()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(max_pages);
            for p in start..=end.min(max_pages) {
                if p >= 1 && !pages.contains(&p) {
                    pages.push(p);
                }
            }
        } else if let Ok(p) = part.parse::<u32>() {
            if p >= 1 && p <= max_pages && !pages.contains(&p) {
                pages.push(p);
            }
        }
    }

    pages.sort();
    pages
}

/// Per-page OCR result for comparison.
#[derive(Clone)]
struct PageResult {
    text: String,
}

/// Backend configuration for comparison (includes device setting).
struct BackendConfig {
    name: String,
    backend_type: crate::ocr::OcrBackendType,
    use_gpu: bool,
}

/// Parse backend string into configurations.
/// Syntax: backend[:device] where device is 'gpu' or 'cpu'
/// Examples: tesseract, deepseek:gpu, deepseek:cpu, paddleocr:gpu
/// Defaults: deepseek -> gpu, others -> cpu
fn parse_backend_configs(backends_str: &str) -> Result<Vec<BackendConfig>, String> {
    use crate::ocr::OcrBackendType;

    let mut configs = Vec::new();
    for spec in backends_str.split(',').map(|s| s.trim()) {
        let (backend_name, device) = if let Some(idx) = spec.find(':') {
            let (name, dev) = spec.split_at(idx);
            (name, Some(&dev[1..])) // Skip the ':'
        } else {
            (spec, None)
        };

        let backend_name_lower = backend_name.to_lowercase();
        let Some(backend_type) = OcrBackendType::from_str(&backend_name_lower) else {
            return Err(format!(
                "Unknown backend '{}'. Available: tesseract, ocrs, paddleocr, deepseek",
                backend_name
            ));
        };

        // Determine GPU setting based on device flag and backend capabilities
        let use_gpu = match device.map(|d| d.to_lowercase()).as_deref() {
            Some("gpu") => {
                // Check if backend supports GPU
                match backend_type {
                    OcrBackendType::Tesseract => {
                        return Err("tesseract does not support GPU acceleration".to_string());
                    }
                    #[cfg(feature = "ocr-ocrs")]
                    OcrBackendType::Ocrs => {
                        return Err("ocrs does not support GPU acceleration".to_string());
                    }
                    _ => true,
                }
            }
            Some("cpu") => false,
            Some(other) => {
                return Err(format!(
                    "Unknown device '{}'. Use :gpu or :cpu",
                    other
                ));
            }
            None => {
                // Defaults: deepseek -> GPU (CPU is impractically slow), others -> CPU
                matches!(backend_type, OcrBackendType::DeepSeek)
            }
        };

        let display_name = if device.is_some() {
            spec.to_string()
        } else if use_gpu {
            format!("{}:gpu", backend_name_lower)
        } else {
            backend_name_lower.to_string()
        };

        configs.push(BackendConfig {
            name: display_name,
            backend_type,
            use_gpu,
        });
    }
    Ok(configs)
}

/// Compare OCR backends on an image or PDF.
async fn cmd_ocr_compare(
    file: &std::path::Path,
    pages_str: Option<&str>,
    backends_str: &str,
    deepseek_path: Option<std::path::PathBuf>,
) -> anyhow::Result<()> {
    use crate::ocr::{DeepSeekBackend, OcrBackend, OcrBackendType, OcrConfig, TesseractBackend};
    use std::collections::HashMap;

    if !file.exists() {
        anyhow::bail!("File not found: {}", file.display());
    }

    // Determine file type
    let is_pdf = file
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase() == "pdf")
        .unwrap_or(false);

    // Get pages to process
    let pages: Vec<u32> = if is_pdf {
        let total_pages = get_pdf_page_count(file).unwrap_or(1);
        match pages_str {
            Some(range) => parse_page_range(range, total_pages),
            None => (1..=total_pages).collect(), // All pages by default
        }
    } else {
        vec![1] // Images only have one "page"
    };

    // Parse requested backends with their configurations
    let backend_configs = parse_backend_configs(backends_str)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    if backend_configs.is_empty() {
        anyhow::bail!("No valid backends specified");
    }

    // Progress to stderr
    eprintln!(
        "Processing {} with {} backend(s) across {} page(s)...",
        file.display(),
        backend_configs.len(),
        pages.len()
    );

    // Store per-page results for each backend: backend_name -> page_num -> PageResult
    let mut all_results: HashMap<String, HashMap<u32, PageResult>> = HashMap::new();
    let mut errors: HashMap<String, String> = HashMap::new();
    let mut total_times: HashMap<String, u64> = HashMap::new();

    for config in &backend_configs {
        let backend_name = config.name.clone();
        let mut page_results: HashMap<u32, PageResult> = HashMap::new();
        let mut total_time_ms: u64 = 0;
        let mut had_error = false;

        eprint!("  {} ", backend_name);
        use std::io::Write;
        std::io::stderr().flush().ok();

        for (i, &page) in pages.iter().enumerate() {
            let result = match config.backend_type {
                OcrBackendType::Tesseract => {
                    let backend = TesseractBackend::new();
                    if !backend.is_available() {
                        errors.insert(backend_name.clone(), backend.availability_hint());
                        had_error = true;
                        break;
                    }
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                OcrBackendType::DeepSeek => {
                    let ocr_config = OcrConfig {
                        use_gpu: config.use_gpu,
                        ..Default::default()
                    };
                    let mut backend = DeepSeekBackend::with_config(ocr_config);
                    if let Some(ref path) = deepseek_path {
                        backend = backend.with_binary_path(path);
                    }
                    if config.use_gpu {
                        backend = backend.with_device("cuda").with_dtype("f16");
                    }
                    if !backend.is_available() {
                        errors.insert(backend_name.clone(), backend.availability_hint());
                        had_error = true;
                        break;
                    }
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                #[cfg(feature = "ocr-ocrs")]
                OcrBackendType::Ocrs => {
                    use crate::ocr::OcrsBackend;
                    let backend = OcrsBackend::new();
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                #[cfg(not(feature = "ocr-ocrs"))]
                OcrBackendType::Ocrs => {
                    errors.insert(
                        backend_name.clone(),
                        "OCRS not compiled (enable ocr-ocrs feature)".to_string(),
                    );
                    had_error = true;
                    break;
                }
                #[cfg(feature = "ocr-paddle")]
                OcrBackendType::PaddleOcr => {
                    use crate::ocr::PaddleBackend;
                    let backend = PaddleBackend::new();
                    if is_pdf {
                        backend.ocr_pdf_page(file, page)
                    } else {
                        backend.ocr_image(file)
                    }
                }
                #[cfg(not(feature = "ocr-paddle"))]
                OcrBackendType::PaddleOcr => {
                    errors.insert(
                        backend_name.clone(),
                        "PaddleOCR not compiled (enable ocr-paddle feature)".to_string(),
                    );
                    had_error = true;
                    break;
                }
            };

            match result {
                Ok(ocr_result) => {
                    total_time_ms += ocr_result.processing_time_ms;
                    page_results.insert(
                        page,
                        PageResult {
                            text: ocr_result.text,
                        },
                    );
                    // Progress dots to stderr
                    if (i + 1) % 10 == 0 {
                        eprint!(".");
                        std::io::stderr().flush().ok();
                    }
                }
                Err(e) => {
                    errors.insert(backend_name.clone(), format!("Page {}: {}", page, e));
                    had_error = true;
                    break;
                }
            }
        }

        if had_error {
            eprintln!(" error");
        } else {
            eprintln!(" done ({}ms)", total_time_ms);
            all_results.insert(backend_name.clone(), page_results);
            total_times.insert(backend_name, total_time_ms);
        }
    }

    if all_results.is_empty() {
        eprintln!("No backends produced results");
        for (name, err) in &errors {
            eprintln!("  {}: {}", name, err);
        }
        return Ok(());
    }

    eprintln!();

    // Get ordered list of backends that succeeded
    let backend_names: Vec<&String> = all_results.keys().collect();
    let num_backends = backend_names.len();

    // Calculate column width based on number of backends
    let total_width = 120;
    let col_width = if num_backends > 0 {
        (total_width - num_backends - 1) / num_backends
    } else {
        40
    };

    // === PAGE-BY-PAGE DIFF ===
    println!("{}", "═".repeat(total_width));
    println!("OCR Comparison: {}", file.display());
    println!("{}", "═".repeat(total_width));

    for &page in &pages {
        println!("\n{}", style(format!("── Page {} ", page)).bold());
        println!("{}", "─".repeat(total_width));

        // Print header row with backend names
        for (i, name) in backend_names.iter().enumerate() {
            if i > 0 {
                print!("│");
            }
            print!("{:^width$}", style(*name).cyan().bold(), width = col_width);
        }
        println!();

        // Separator
        for i in 0..num_backends {
            if i > 0 {
                print!("┼");
            }
            print!("{}", "─".repeat(col_width));
        }
        println!();

        // Get lines for each backend for this page
        let mut backend_lines: Vec<Vec<&str>> = Vec::new();
        for name in &backend_names {
            let lines: Vec<&str> = all_results
                .get(*name)
                .and_then(|pages| pages.get(&page))
                .map(|r| r.text.lines().collect())
                .unwrap_or_default();
            backend_lines.push(lines);
        }

        let max_lines = backend_lines.iter().map(|l| l.len()).max().unwrap_or(0);

        for line_idx in 0..max_lines {
            // Check if all backends have the same line
            let first_line = backend_lines
                .first()
                .and_then(|l| l.get(line_idx))
                .map(|s| s.trim());
            let all_same = backend_lines
                .iter()
                .all(|lines| lines.get(line_idx).map(|s| s.trim()) == first_line);

            for (backend_idx, lines) in backend_lines.iter().enumerate() {
                if backend_idx > 0 {
                    print!("│");
                }
                let line = lines.get(line_idx).map(|s| s.trim()).unwrap_or("");
                let truncated = truncate(line, col_width);

                if all_same {
                    print!("{:<width$}", truncated, width = col_width);
                } else {
                    // Highlight differences
                    print!("{:<width$}", style(&truncated).yellow(), width = col_width);
                }
            }
            println!();
        }
    }

    // === SUMMARY ===
    println!("\n{}", "═".repeat(total_width));
    println!("{}", style("Summary").bold());
    println!("{}", "─".repeat(total_width));
    println!("File: {}", file.display());
    println!("Pages: {}", pages.len());
    println!();

    // Sort by total time
    let mut timing_vec: Vec<_> = total_times.iter().collect();
    timing_vec.sort_by_key(|(_, ms)| *ms);

    let fastest_ms = timing_vec.first().map(|(_, ms)| **ms).unwrap_or(1);

    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>8}",
        "Backend", "Total", "Per Page", "Chars", ""
    );
    println!("{}", "─".repeat(60));

    for (name, total_ms) in &timing_vec {
        let avg_ms = **total_ms / pages.len() as u64;

        // Count total chars for this backend
        let total_chars: usize = all_results
            .get(*name)
            .map(|pages| {
                pages
                    .values()
                    .map(|r| r.text.chars().filter(|c| !c.is_whitespace()).count())
                    .sum()
            })
            .unwrap_or(0);

        let speedup = if **total_ms == fastest_ms {
            style("fastest").green().to_string()
        } else {
            let ratio = **total_ms as f64 / fastest_ms as f64;
            style(format!("{:.1}x slower", ratio)).yellow().to_string()
        };

        println!(
            "{:<12} {:>8}ms {:>8}ms {:>10} {}",
            name, total_ms, avg_ms, total_chars, speedup
        );
    }

    // Show errors if any
    if !errors.is_empty() {
        println!("\n{}", style("Errors:").red().bold());
        for (name, err) in &errors {
            println!("  {}: {}", name, err);
        }
    }

    println!();
    Ok(())
}

/// Process documents with OCR.
async fn cmd_ocr(
    settings: &Settings,
    source_id: Option<&str>,
    workers: usize,
    limit: usize,
) -> anyhow::Result<()> {
    use crate::services::{OcrEvent, OcrService};
    use tokio::sync::mpsc;

    let db_path = settings.database_path();
    let doc_repo = Arc::new(DocumentRepository::new(&db_path, &settings.documents_dir)?);

    let service = OcrService::new(doc_repo);

    // Check if there's work to do
    let (docs_count, pages_count) = service.count_needing_processing(source_id)?;
    if docs_count == 0 && pages_count == 0 {
        println!("{} No documents need OCR processing", style("!").yellow());
        return Ok(());
    }

    // Create event channel for progress tracking
    let (event_tx, mut event_rx) = mpsc::channel::<OcrEvent>(100);

    // State for progress bar
    let pb = Arc::new(tokio::sync::Mutex::new(None::<ProgressBar>));
    let pb_clone = pb.clone();

    // Spawn event handler for UI
    let event_handler = tokio::spawn(async move {
        let mut phase1_succeeded = 0;
        let mut phase1_failed = 0;
        let mut phase1_pages = 0;
        let mut phase2_improved = 0;
        let mut phase2_skipped = 0;
        let mut phase2_failed = 0;
        let mut docs_finalized_incremental = 0;

        while let Some(event) = event_rx.recv().await {
            match event {
                OcrEvent::Phase1Started { total_documents } => {
                    println!(
                        "{} Phase 1: Extracting text from {} documents",
                        style("→").cyan(),
                        total_documents
                    );
                    let progress = ProgressBar::new(total_documents as u64);
                    progress.set_style(
                        ProgressStyle::default_bar()
                            .template(
                                "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                            )
                            .unwrap()
                            .progress_chars("█▓░"),
                    );
                    progress.set_message("Extracting text...");
                    *pb_clone.lock().await = Some(progress);
                }
                OcrEvent::DocumentCompleted {
                    pages_extracted, ..
                } => {
                    phase1_succeeded += 1;
                    phase1_pages += pages_extracted;
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.inc(1);
                    }
                }
                OcrEvent::DocumentFailed { .. } => {
                    phase1_failed += 1;
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.inc(1);
                    }
                }
                OcrEvent::Phase1Complete { .. } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.finish_and_clear();
                    }
                    *pb_clone.lock().await = None;
                    println!(
                        "{} Phase 1 complete: {} documents processed, {} pages extracted",
                        style("✓").green(),
                        phase1_succeeded,
                        phase1_pages
                    );
                    if phase1_failed > 0 {
                        println!(
                            "  {} {} documents failed",
                            style("!").yellow(),
                            phase1_failed
                        );
                    }
                }
                OcrEvent::Phase2Started { total_pages } => {
                    println!(
                        "{} Phase 2: Running OCR on {} pages",
                        style("→").cyan(),
                        total_pages
                    );
                    let progress = ProgressBar::new(total_pages as u64);
                    progress.set_style(
                        ProgressStyle::default_bar()
                            .template(
                                "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                            )
                            .unwrap()
                            .progress_chars("█▓░"),
                    );
                    progress.set_message("Running OCR...");
                    *pb_clone.lock().await = Some(progress);
                }
                OcrEvent::PageOcrCompleted { improved, .. } => {
                    if improved {
                        phase2_improved += 1;
                    } else {
                        phase2_skipped += 1;
                    }
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.inc(1);
                    }
                }
                OcrEvent::PageOcrFailed { .. } => {
                    phase2_failed += 1;
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.inc(1);
                    }
                }
                OcrEvent::DocumentFinalized { .. } => {
                    docs_finalized_incremental += 1;
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress
                            .set_message(format!("{} docs complete", docs_finalized_incremental));
                    }
                }
                OcrEvent::Phase2Complete { .. } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.finish_and_clear();
                    }
                    *pb_clone.lock().await = None;
                    let mut msg = format!(
                        "{} Phase 2 complete: {} pages improved by OCR, {} kept PDF text",
                        style("✓").green(),
                        phase2_improved,
                        phase2_skipped
                    );
                    if phase2_failed > 0 {
                        msg.push_str(&format!(", {} failed", phase2_failed));
                    }
                    if docs_finalized_incremental > 0 {
                        msg.push_str(&format!(
                            ", {} documents finalized",
                            docs_finalized_incremental
                        ));
                    }
                    println!("{}", msg);
                }
                _ => {}
            }
        }
    });

    // Run service
    let _result = service.process(source_id, workers, limit, event_tx).await?;

    // Wait for event handler to finish
    let _ = event_handler.await;

    Ok(())
}

/// Start web server to browse documents.
async fn cmd_serve(settings: &Settings, bind: &str) -> anyhow::Result<()> {
    let (host, port) = parse_bind_address(bind)?;

    // Run database migrations first
    println!("{} Running database migrations...", style("→").cyan(),);
    let db_path = settings.database_path();
    match crate::repository::run_all_migrations(&db_path, &settings.documents_dir) {
        Ok(tables) => {
            println!(
                "  {} Database ready ({} tables)",
                style("✓").green(),
                tables.len()
            );
        }
        Err(e) => {
            eprintln!("  {} Migration failed: {}", style("✗").red(), e);
            return Err(anyhow::anyhow!("Database migration failed: {}", e));
        }
    }

    println!(
        "{} Starting FOIAcquire server at http://{}:{}",
        style("→").cyan(),
        host,
        port
    );
    println!("  Press Ctrl+C to stop");

    crate::server::serve(settings, &host, port).await
}

/// Parse a bind address that can be:
/// - Just a port: "3030" -> 127.0.0.1:3030
/// - Just a host: "0.0.0.0" -> 0.0.0.0:3030
/// - Host and port: "0.0.0.0:3030" -> 0.0.0.0:3030
fn parse_bind_address(bind: &str) -> anyhow::Result<(String, u16)> {
    // Try parsing as just a port number
    if let Ok(port) = bind.parse::<u16>() {
        return Ok(("127.0.0.1".to_string(), port));
    }

    // Try parsing as host:port
    if let Some((host, port_str)) = bind.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            return Ok((host.to_string(), port));
        }
    }

    // Must be just a host, use default port
    Ok((bind.to_string(), 3030))
}

/// Refresh metadata for existing documents using HEAD requests.
///
/// Strategy:
/// 1. For documents missing original_filename or server_date, make a HEAD request
/// 2. If ETag matches and we get Last-Modified, update metadata without downloading
/// 3. If ETag differs or no metadata available from HEAD, do a full GET
async fn cmd_refresh(
    settings: &Settings,
    source_id: Option<&str>,
    workers: usize,
    limit: usize,
    force: bool,
) -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    let db_path = settings.database_path();
    let doc_repo = Arc::new(DocumentRepository::new(&db_path, &settings.documents_dir)?);

    // Get documents that need metadata refresh
    let documents = if let Some(sid) = source_id {
        doc_repo.get_by_source(sid)?
    } else {
        doc_repo.get_all()?
    };

    // Filter to documents needing refresh (missing original_filename or server_date)
    let docs_needing_refresh: Vec<_> = documents
        .into_iter()
        .filter(|doc| {
            if force {
                return true;
            }
            if let Some(version) = doc.current_version() {
                version.original_filename.is_none() || version.server_date.is_none()
            } else {
                false
            }
        })
        .collect();

    let total = if limit > 0 {
        std::cmp::min(limit, docs_needing_refresh.len())
    } else {
        docs_needing_refresh.len()
    };

    if total == 0 {
        println!("{} All documents already have metadata", style("✓").green());
        return Ok(());
    }

    println!(
        "{} Refreshing metadata for {} documents using {} workers",
        style("→").cyan(),
        total,
        workers
    );

    // Create work queue
    let work_queue: Arc<tokio::sync::Mutex<Vec<crate::models::Document>>> = Arc::new(
        tokio::sync::Mutex::new(docs_needing_refresh.into_iter().take(total).collect()),
    );

    let updated = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let redownloaded = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(workers));

    // Progress bar
    let pb = indicatif::ProgressBar::new(total as u64);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    let mut handles = Vec::new();
    let documents_dir = settings.documents_dir.clone();

    for _ in 0..workers {
        let work_queue = work_queue.clone();
        let doc_repo = doc_repo.clone();
        let documents_dir = documents_dir.clone();
        let updated = updated.clone();
        let skipped = skipped.clone();
        let redownloaded = redownloaded.clone();
        let semaphore = semaphore.clone();
        let pb = pb.clone();

        let handle = tokio::spawn(async move {
            let client = crate::scrapers::HttpClient::new(
                "refresh",
                std::time::Duration::from_secs(30),
                std::time::Duration::from_millis(100),
            );

            loop {
                let _permit = semaphore.acquire().await.unwrap();

                let doc = {
                    let mut queue = work_queue.lock().await;
                    queue.pop()
                };

                let doc = match doc {
                    Some(d) => d,
                    None => break,
                };

                pb.set_message(truncate(&doc.title, 40));

                let url = &doc.source_url;
                let current_version = match doc.current_version() {
                    Some(v) => v,
                    None => {
                        pb.inc(1);
                        continue;
                    }
                };

                // Try HEAD request first
                let head_result = client.head(url, None, None).await;

                match head_result {
                    Ok(head_response) if head_response.is_success() => {
                        let _head_etag = head_response.etag().map(|s| s.to_string());
                        let head_last_modified =
                            head_response.last_modified().map(|s| s.to_string());
                        let head_filename = head_response.content_disposition_filename();

                        // Parse server date from Last-Modified
                        let server_date = head_last_modified.as_ref().and_then(|lm| {
                            chrono::DateTime::parse_from_rfc2822(lm)
                                .ok()
                                .map(|dt| dt.with_timezone(&chrono::Utc))
                        });

                        // Check if we got useful metadata from HEAD
                        let got_metadata = head_filename.is_some() || server_date.is_some();

                        if got_metadata
                            && (head_filename.is_some()
                                || current_version.original_filename.is_some())
                            && (server_date.is_some() || current_version.server_date.is_some())
                        {
                            // We can update metadata without re-downloading
                            // Create updated version with new metadata
                            let mut updated_doc = doc.clone();
                            if let Some(version) = updated_doc.versions.first_mut() {
                                if version.original_filename.is_none() {
                                    version.original_filename = head_filename;
                                }
                                if version.server_date.is_none() {
                                    version.server_date = server_date;
                                }
                            }

                            if let Err(e) = doc_repo.save(&updated_doc) {
                                pb.println(format!(
                                    "{} Failed to save {}: {}",
                                    style("✗").red(),
                                    truncate(&doc.title, 30),
                                    e
                                ));
                            } else {
                                updated.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            // Need to do full GET to get metadata
                            match client.get(url, None, None).await {
                                Ok(response) if response.is_success() => {
                                    let filename = response.content_disposition_filename();
                                    let last_modified =
                                        response.last_modified().map(|s| s.to_string());
                                    let server_date = last_modified.as_ref().and_then(|lm| {
                                        chrono::DateTime::parse_from_rfc2822(lm)
                                            .ok()
                                            .map(|dt| dt.with_timezone(&chrono::Utc))
                                    });

                                    // Check ETag to see if content changed
                                    let _response_etag = response.etag().map(|s| s.to_string());
                                    let content = match response.bytes().await {
                                        Ok(b) => b,
                                        Err(_) => {
                                            pb.inc(1);
                                            continue;
                                        }
                                    };

                                    let new_hash = DocumentVersion::compute_hash(&content);
                                    let content_changed = new_hash != current_version.content_hash;

                                    let mut updated_doc = doc.clone();

                                    if content_changed {
                                        // Content changed - add new version
                                        let content_path =
                                            documents_dir.join(&new_hash[..2]).join(format!(
                                                "{}.{}",
                                                &new_hash[..8],
                                                mime_to_extension(&current_version.mime_type)
                                            ));

                                        if let Some(parent) = content_path.parent() {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&content_path, &content);

                                        let new_version = DocumentVersion::new_with_metadata(
                                            &content,
                                            content_path,
                                            current_version.mime_type.clone(),
                                            Some(url.clone()),
                                            filename,
                                            server_date,
                                        );
                                        updated_doc.add_version(new_version);
                                        redownloaded.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        // Content same - just update metadata on existing version
                                        if let Some(version) = updated_doc.versions.first_mut() {
                                            if version.original_filename.is_none() {
                                                version.original_filename = filename;
                                            }
                                            if version.server_date.is_none() {
                                                version.server_date = server_date;
                                            }
                                        }
                                        updated.fetch_add(1, Ordering::Relaxed);
                                    }

                                    if let Err(e) = doc_repo.save(&updated_doc) {
                                        pb.println(format!(
                                            "{} Failed to save {}: {}",
                                            style("✗").red(),
                                            truncate(&doc.title, 30),
                                            e
                                        ));
                                    }
                                }
                                _ => {
                                    skipped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    _ => {
                        // HEAD failed or not supported, try GET
                        match client.get(url, None, None).await {
                            Ok(response) if response.is_success() => {
                                let filename = response.content_disposition_filename();
                                let last_modified = response.last_modified().map(|s| s.to_string());
                                let server_date = last_modified.as_ref().and_then(|lm| {
                                    chrono::DateTime::parse_from_rfc2822(lm)
                                        .ok()
                                        .map(|dt| dt.with_timezone(&chrono::Utc))
                                });

                                let content = match response.bytes().await {
                                    Ok(b) => b,
                                    Err(_) => {
                                        pb.inc(1);
                                        continue;
                                    }
                                };

                                let new_hash = DocumentVersion::compute_hash(&content);
                                let content_changed = new_hash != current_version.content_hash;

                                let mut updated_doc = doc.clone();

                                if content_changed {
                                    let content_path =
                                        documents_dir.join(&new_hash[..2]).join(format!(
                                            "{}.{}",
                                            &new_hash[..8],
                                            mime_to_extension(&current_version.mime_type)
                                        ));

                                    if let Some(parent) = content_path.parent() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                    let _ = std::fs::write(&content_path, &content);

                                    let new_version = DocumentVersion::new_with_metadata(
                                        &content,
                                        content_path,
                                        current_version.mime_type.clone(),
                                        Some(url.clone()),
                                        filename,
                                        server_date,
                                    );
                                    updated_doc.add_version(new_version);
                                    redownloaded.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    if let Some(version) = updated_doc.versions.first_mut() {
                                        if version.original_filename.is_none() {
                                            version.original_filename = filename;
                                        }
                                        if version.server_date.is_none() {
                                            version.server_date = server_date;
                                        }
                                    }
                                    updated.fetch_add(1, Ordering::Relaxed);
                                }

                                if let Err(e) = doc_repo.save(&updated_doc) {
                                    pb.println(format!(
                                        "{} Failed to save {}: {}",
                                        style("✗").red(),
                                        truncate(&doc.title, 30),
                                        e
                                    ));
                                }
                            }
                            _ => {
                                skipped.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }

                pb.inc(1);
            }
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        let _ = handle.await;
    }

    pb.finish_with_message("done");

    let final_updated = updated.load(Ordering::Relaxed);
    let final_skipped = skipped.load(Ordering::Relaxed);
    let final_redownloaded = redownloaded.load(Ordering::Relaxed);

    println!(
        "{} Updated metadata for {} documents",
        style("✓").green(),
        final_updated
    );

    if final_redownloaded > 0 {
        println!(
            "  {} {} documents had content changes (new versions added)",
            style("↻").yellow(),
            final_redownloaded
        );
    }

    if final_skipped > 0 {
        println!(
            "  {} {} documents skipped (fetch failed)",
            style("→").dim(),
            final_skipped
        );
    }

    Ok(())
}

/// Get file extension from MIME type
fn mime_to_extension(mime: &str) -> &str {
    match mime {
        "application/pdf" => "pdf",
        "text/html" => "html",
        "text/plain" => "txt",
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "application/vnd.ms-excel" => "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xlsx",
        _ => "bin",
    }
}

/// Annotate documents using local LLM (generates synopsis and tags).
async fn cmd_annotate(
    settings: &Settings,
    source_id: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    use crate::services::{AnnotationEvent, AnnotationService};
    use tokio::sync::mpsc;

    let db_path = settings.database_path();
    let doc_repo = Arc::new(DocumentRepository::new(&db_path, &settings.documents_dir)?);

    // Load config for LLM settings
    let config = Config::load().await;

    if !config.llm.enabled {
        println!(
            "{} LLM annotation is disabled in configuration",
            style("!").yellow()
        );
        println!("  Set llm.enabled = true in your foiacquire.json config");
        return Ok(());
    }

    // Create service
    let service = AnnotationService::new(doc_repo, config.llm.clone());

    // Check if LLM service is available
    if !service.is_available().await {
        println!(
            "{} LLM service not available at {}",
            style("✗").red(),
            config.llm.endpoint
        );
        println!("  Make sure Ollama is running: ollama serve");
        return Ok(());
    }

    println!(
        "{} Connected to LLM at {} (model: {})",
        style("✓").green(),
        config.llm.endpoint,
        config.llm.model
    );

    // Check if there's work to do
    let total_count = service.count_needing_annotation(source_id)?;

    if total_count == 0 {
        println!("{} No documents need annotation", style("!").yellow());
        println!("  Documents need OCR complete status with extracted text to be annotated");
        return Ok(());
    }

    let effective_limit = if limit > 0 {
        limit
    } else {
        total_count as usize
    };

    println!(
        "{} Annotating up to {} documents (running sequentially to manage memory)",
        style("→").cyan(),
        effective_limit
    );

    // Create event channel for progress tracking
    let (event_tx, mut event_rx) = mpsc::channel::<AnnotationEvent>(100);

    // State for progress bar
    let pb = Arc::new(tokio::sync::Mutex::new(None::<ProgressBar>));
    let pb_clone = pb.clone();

    // Spawn event handler for UI
    let event_handler = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                AnnotationEvent::Started { total_documents } => {
                    let progress = ProgressBar::new(total_documents as u64);
                    progress.set_style(
                        ProgressStyle::default_bar()
                            .template(
                                "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                            )
                            .unwrap()
                            .progress_chars("█▓░"),
                    );
                    progress.set_message("Annotating...");
                    *pb_clone.lock().await = Some(progress);
                }
                AnnotationEvent::DocumentStarted { title, .. } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.set_message(truncate(&title, 40));
                    }
                }
                AnnotationEvent::DocumentCompleted { .. }
                | AnnotationEvent::DocumentSkipped { .. } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.inc(1);
                    }
                }
                AnnotationEvent::DocumentFailed { error, .. } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.println(format!("{} {}", style("✗").red(), error));
                        progress.inc(1);
                    }
                }
                AnnotationEvent::Complete {
                    succeeded,
                    failed,
                    remaining,
                } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.finish_and_clear();
                    }
                    *pb_clone.lock().await = None;

                    println!(
                        "{} Annotation complete: {} succeeded, {} failed",
                        style("✓").green(),
                        succeeded,
                        failed
                    );

                    if remaining > 0 {
                        println!(
                            "  {} {} documents still need annotation",
                            style("→").dim(),
                            remaining
                        );
                    }
                }
            }
        }
    });

    // Run service
    let _result = service.annotate(source_id, limit, event_tx).await?;

    // Wait for event handler to finish
    let _ = event_handler.await;

    Ok(())
}

/// Detect and estimate publication dates for documents.
async fn cmd_detect_dates(
    settings: &Settings,
    source_id: Option<&str>,
    limit: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    use crate::services::date_detection::{detect_date, DateConfidence};

    let db_path = settings.database_path();
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;

    // Count documents needing date estimation
    let total_count = doc_repo.count_documents_needing_date_estimation(source_id)?;

    if total_count == 0 {
        println!("{} No documents need date estimation", style("!").yellow());
        println!("  All documents already have estimated_date or manual_date set");
        return Ok(());
    }

    let effective_limit = if limit > 0 { limit } else { total_count as usize };

    if dry_run {
        println!(
            "{} Dry run - showing what would be detected for up to {} documents",
            style("→").cyan(),
            effective_limit
        );
    } else {
        println!(
            "{} Detecting dates for up to {} documents",
            style("→").cyan(),
            effective_limit
        );
    }

    // Fetch documents needing estimation
    let documents = doc_repo.get_documents_needing_date_estimation(source_id, effective_limit)?;

    let pb = ProgressBar::new(documents.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}")
            .unwrap()
            .progress_chars("█▓░"),
    );
    pb.set_message("Analyzing...");

    let mut detected = 0u64;
    let mut no_date = 0u64;

    for (doc_id, filename, server_date, acquired_at, source_url) in documents {
        pb.set_message(truncate(&doc_id, 36));

        // Run date detection
        let estimate = detect_date(
            server_date,
            acquired_at,
            filename.as_deref(),
            source_url.as_deref(),
        );

        if let Some(est) = estimate {
            detected += 1;

            if dry_run {
                let confidence_str = match est.confidence {
                    DateConfidence::High => style("high").green(),
                    DateConfidence::Medium => style("medium").yellow(),
                    DateConfidence::Low => style("low").red(),
                };
                pb.println(format!(
                    "  {} {} → {} ({}, {})",
                    style("✓").green(),
                    &doc_id[..8],
                    est.date.format("%Y-%m-%d"),
                    confidence_str,
                    est.source.as_str()
                ));
            } else {
                // Update database with detected date
                doc_repo.update_estimated_date(
                    &doc_id,
                    est.date,
                    est.confidence.as_str(),
                    est.source.as_str(),
                )?;
                // Record that we processed this document
                doc_repo.record_annotation(
                    &doc_id,
                    "date_detection",
                    1,
                    Some(&format!("detected:{}", est.source.as_str())),
                    None,
                )?;
            }
        } else {
            no_date += 1;
            if !dry_run {
                // Record that we tried but found no date
                doc_repo.record_annotation(&doc_id, "date_detection", 1, Some("no_date"), None)?;
            }
        }

        pb.inc(1);
    }

    pb.finish_and_clear();

    println!(
        "{} Date detection complete: {} detected, {} no date found",
        style("✓").green(),
        detected,
        no_date
    );

    if dry_run && detected > 0 {
        println!(
            "  {} Run without --dry-run to update database",
            style("→").dim()
        );
    }

    // Use saturating subtraction to avoid underflow
    // (can happen if count query and get query have slightly different criteria)
    let processed = detected + no_date;
    if processed < total_count {
        let remaining = total_count - processed;
        println!(
            "  {} {} documents still need date estimation",
            style("→").dim(),
            remaining
        );
    }

    Ok(())
}

/// List available LLM models.
async fn cmd_llm_models(_settings: &Settings) -> anyhow::Result<()> {
    let config = Config::load().await;
    let llm_client = LlmClient::new(config.llm.clone());

    println!("\n{}", style("LLM Configuration").bold());
    println!("{}", "-".repeat(40));
    println!(
        "{:<20} {}",
        "Enabled:",
        if config.llm.enabled { "Yes" } else { "No" }
    );
    println!("{:<20} {}", "Endpoint:", config.llm.endpoint);
    println!("{:<20} {}", "Current Model:", config.llm.model);
    println!("{:<20} {}", "Max Tokens:", config.llm.max_tokens);
    println!("{:<20} {:.2}", "Temperature:", config.llm.temperature);

    if !llm_client.is_available().await {
        println!(
            "\n{} LLM service not available at {}",
            style("!").yellow(),
            config.llm.endpoint
        );
        println!("  Make sure Ollama is running: ollama serve");
        return Ok(());
    }

    println!("\n{}", style("Available Models").bold());
    println!("{}", "-".repeat(40));

    match llm_client.list_models().await {
        Ok(models) => {
            if models.is_empty() {
                println!("  No models installed");
                println!("  Install one with: ollama pull llama3.2:instruct");
            } else {
                for model in models {
                    let marker = if model == config.llm.model {
                        style("*").green().to_string()
                    } else {
                        " ".to_string()
                    };
                    println!("{} {}", marker, model);
                }
            }
        }
        Err(e) => {
            println!("{} Failed to list models: {}", style("✗").red(), e);
        }
    }

    Ok(())
}

/// Process archive files to extract virtual files.
async fn cmd_archive(
    settings: &Settings,
    source_id: Option<&str>,
    limit: usize,
    run_ocr: bool,
) -> anyhow::Result<()> {
    use crate::models::{VirtualFile, VirtualFileStatus};
    use crate::ocr::{ArchiveExtractor, EmailExtractor, TextExtractor};

    let db_path = settings.database_path();
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;

    // Count unprocessed containers (archives + emails)
    let archive_count = doc_repo.count_unprocessed_archives(source_id)?;
    let email_count = doc_repo.count_unprocessed_emails(source_id)?;
    let total_count = archive_count + email_count;

    if total_count == 0 {
        println!("{} No containers need processing", style("!").yellow());
        return Ok(());
    }

    let effective_limit = if limit > 0 {
        limit.min(total_count as usize)
    } else {
        total_count as usize
    };

    println!(
        "{} Processing up to {} containers ({} archives, {} emails)",
        style("→").cyan(),
        effective_limit,
        archive_count,
        email_count
    );

    let pb = ProgressBar::new(effective_limit as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}")
            .unwrap()
            .progress_chars("█▓░"),
    );

    let mut containers_processed = 0;
    let mut files_discovered = 0;
    let mut files_extracted = 0;
    let text_extractor = TextExtractor::new();

    // Process zip archives first
    let archive_limit = effective_limit.min(archive_count as usize);
    if archive_limit > 0 {
        let archives = doc_repo.get_unprocessed_archives(source_id, archive_limit)?;

        for doc in archives {
            pb.set_message(truncate(&doc.title, 40));

            let version = match doc.current_version() {
                Some(v) => v,
                None => {
                    pb.inc(1);
                    continue;
                }
            };

            let version_id = match doc_repo.get_current_version_id(&doc.id)? {
                Some(id) => id,
                None => {
                    pb.inc(1);
                    continue;
                }
            };

            let entries = match ArchiveExtractor::list_zip_contents(&version.file_path) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("Failed to read archive {}: {}", doc.title, e);
                    pb.inc(1);
                    continue;
                }
            };

            files_discovered += entries.len();

            for entry in entries {
                let mut vf = VirtualFile::new(
                    doc.id.clone(),
                    version_id,
                    entry.path.clone(),
                    entry.filename.clone(),
                    entry.mime_type.clone(),
                    entry.size,
                );

                if run_ocr && entry.is_extractable() {
                    match ArchiveExtractor::extract_file(&version.file_path, &entry.path) {
                        Ok(extracted) => {
                            match text_extractor.extract(&extracted.file_path, &entry.mime_type) {
                                Ok(result) => {
                                    vf.extracted_text = Some(result.text);
                                    vf.status = VirtualFileStatus::OcrComplete;
                                    files_extracted += 1;
                                }
                                Err(e) => {
                                    tracing::debug!("OCR failed for {}: {}", entry.path, e);
                                    vf.status = VirtualFileStatus::Failed;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to extract {}: {}", entry.path, e);
                            vf.status = VirtualFileStatus::Failed;
                        }
                    }
                } else if !entry.is_extractable() {
                    vf.status = VirtualFileStatus::Unsupported;
                }

                if let Err(e) = doc_repo.insert_virtual_file(&vf) {
                    tracing::warn!("Failed to save virtual file {}: {}", entry.path, e);
                }
            }

            containers_processed += 1;
            pb.inc(1);
        }
    }

    // Process emails if we have room in the limit
    let remaining_limit = effective_limit.saturating_sub(containers_processed);
    if remaining_limit > 0 && email_count > 0 {
        let emails = doc_repo.get_unprocessed_emails(source_id, remaining_limit)?;

        for doc in emails {
            pb.set_message(truncate(&doc.title, 40));

            let version = match doc.current_version() {
                Some(v) => v,
                None => {
                    pb.inc(1);
                    continue;
                }
            };

            let version_id = match doc_repo.get_current_version_id(&doc.id)? {
                Some(id) => id,
                None => {
                    pb.inc(1);
                    continue;
                }
            };

            let parsed = match EmailExtractor::parse_email(&version.file_path) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("Failed to parse email {}: {}", doc.title, e);
                    pb.inc(1);
                    continue;
                }
            };

            files_discovered += parsed.attachments.len();

            for attachment in &parsed.attachments {
                let mut vf = VirtualFile::new(
                    doc.id.clone(),
                    version_id,
                    attachment.filename.clone(),
                    attachment.filename.clone(),
                    attachment.mime_type.clone(),
                    attachment.size,
                );

                if run_ocr && attachment.is_extractable() {
                    match EmailExtractor::extract_attachment(
                        &version.file_path,
                        &attachment.filename,
                    ) {
                        Ok(extracted) => {
                            match text_extractor
                                .extract(&extracted.file_path, &attachment.mime_type)
                            {
                                Ok(result) => {
                                    vf.extracted_text = Some(result.text);
                                    vf.status = VirtualFileStatus::OcrComplete;
                                    files_extracted += 1;
                                }
                                Err(e) => {
                                    tracing::debug!(
                                        "OCR failed for {}: {}",
                                        attachment.filename,
                                        e
                                    );
                                    vf.status = VirtualFileStatus::Failed;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to extract {}: {}", attachment.filename, e);
                            vf.status = VirtualFileStatus::Failed;
                        }
                    }
                } else if !attachment.is_extractable() {
                    vf.status = VirtualFileStatus::Unsupported;
                }

                if let Err(e) = doc_repo.insert_virtual_file(&vf) {
                    tracing::warn!("Failed to save virtual file {}: {}", attachment.filename, e);
                }
            }

            // Mark emails with no attachments as processed
            if parsed.attachments.is_empty() {
                let placeholder = VirtualFile::new(
                    doc.id.clone(),
                    version_id,
                    "_email_body".to_string(),
                    "_email_body".to_string(),
                    "text/plain".to_string(),
                    parsed
                        .body_text
                        .as_ref()
                        .map(|s| s.len() as u64)
                        .unwrap_or(0),
                );
                let _ = doc_repo.insert_virtual_file(&placeholder);
            }

            containers_processed += 1;
            pb.inc(1);
        }
    }

    pb.finish_and_clear();

    println!("{} Container processing complete:", style("✓").green());
    println!("  {} containers processed", containers_processed);
    println!("  {} files discovered", files_discovered);
    if run_ocr {
        println!("  {} files extracted and OCR'd", files_extracted);
    }

    Ok(())
}

/// List documents in the repository.
async fn cmd_ls(
    settings: &Settings,
    source_id: Option<&str>,
    tag: Option<&str>,
    type_filter: Option<&str>,
    limit: usize,
    format: &str,
) -> anyhow::Result<()> {
    let db_path = settings.database_path();
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;

    // Get documents based on filters
    let documents: Vec<Document> = if let Some(tag_name) = tag {
        // Filter by tag
        doc_repo.get_by_tag(tag_name, source_id)?
    } else if let Some(type_name) = type_filter {
        // Filter by type
        doc_repo.get_by_type_category(type_name, source_id, limit)?
    } else if let Some(sid) = source_id {
        // Filter by source
        doc_repo.get_by_source(sid)?
    } else {
        // Get all
        doc_repo.get_all()?
    };

    // Apply limit
    let documents: Vec<_> = documents.into_iter().take(limit).collect();

    if documents.is_empty() {
        println!("{} No documents found", style("!").yellow());
        return Ok(());
    }

    match format {
        "json" => {
            // JSON output
            let output: Vec<_> = documents
                .iter()
                .map(|doc| {
                    let version = doc.current_version();
                    serde_json::json!({
                        "id": doc.id,
                        "title": doc.title,
                        "source_id": doc.source_id,
                        "source_url": doc.source_url,
                        "synopsis": doc.synopsis,
                        "tags": doc.tags,
                        "status": doc.status.as_str(),
                        "mime_type": version.map(|v| v.mime_type.as_str()),
                        "file_size": version.map(|v| v.file_size),
                        "file_path": version.map(|v| v.file_path.to_string_lossy().to_string()),
                        "created_at": doc.created_at.to_rfc3339(),
                        "updated_at": doc.updated_at.to_rfc3339(),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        "ids" => {
            // Just IDs (for piping)
            for doc in &documents {
                println!("{}", doc.id);
            }
        }
        _ => {
            // Table format (default)
            println!(
                "\n{:<36}  {:<30}  {:<10}  {:<10}  Status",
                "ID", "Title", "Type", "Size"
            );
            println!("{}", "-".repeat(100));

            for doc in &documents {
                let version = doc.current_version();
                let mime = version.map(|v| mime_short(&v.mime_type)).unwrap_or("???");
                let size = version
                    .map(|v| format_bytes(v.file_size))
                    .unwrap_or_else(|| "-".to_string());
                let status = doc.status.as_str();

                println!(
                    "{:<36}  {:<30}  {:<10}  {:<10}  {}",
                    &doc.id[..36.min(doc.id.len())],
                    truncate(&doc.title, 30),
                    mime,
                    size,
                    status
                );
            }

            println!("\n{} documents", documents.len());
        }
    }

    Ok(())
}

/// Show document info/metadata.
async fn cmd_info(settings: &Settings, doc_id: &str) -> anyhow::Result<()> {
    let db_path = settings.database_path();
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;

    // Try to find document by ID
    let doc = match doc_repo.get(doc_id)? {
        Some(d) => d,
        None => {
            // Try to find by partial ID or title search
            let all_docs = doc_repo.get_all()?;
            let matches: Vec<_> = all_docs
                .into_iter()
                .filter(|d| {
                    d.id.starts_with(doc_id)
                        || d.title.to_lowercase().contains(&doc_id.to_lowercase())
                })
                .collect();

            match matches.len() {
                0 => {
                    println!("{} Document not found: {}", style("✗").red(), doc_id);
                    return Ok(());
                }
                1 => matches.into_iter().next().unwrap(),
                _ => {
                    println!("{} Multiple matches found:", style("!").yellow());
                    for d in &matches {
                        println!("  {} - {}", &d.id[..8], truncate(&d.title, 50));
                    }
                    return Ok(());
                }
            }
        }
    };

    // Display document info
    println!("\n{}", style("Document Info").bold());
    println!("{}", "=".repeat(60));
    println!("{:<18} {}", "ID:", doc.id);
    println!("{:<18} {}", "Title:", doc.title);
    println!("{:<18} {}", "Source:", doc.source_id);
    println!("{:<18} {}", "URL:", doc.source_url);
    println!("{:<18} {}", "Status:", doc.status.as_str());
    println!(
        "{:<18} {}",
        "Created:",
        doc.created_at.format("%Y-%m-%d %H:%M:%S")
    );
    println!(
        "{:<18} {}",
        "Updated:",
        doc.updated_at.format("%Y-%m-%d %H:%M:%S")
    );

    if let Some(synopsis) = &doc.synopsis {
        println!("\n{}", style("Synopsis").bold());
        println!("{}", "-".repeat(60));
        println!("{}", synopsis);
    }

    if !doc.tags.is_empty() {
        println!("\n{:<18} {}", "Tags:", doc.tags.join(", "));
    }

    if let Some(version) = doc.current_version() {
        println!("\n{}", style("Current Version").bold());
        println!("{}", "-".repeat(60));
        println!("{:<18} {}", "File:", version.file_path.display());
        println!("{:<18} {}", "MIME Type:", version.mime_type);
        println!("{:<18} {}", "Size:", format_bytes(version.file_size));
        println!("{:<18} {}", "Content Hash:", &version.content_hash[..16]);
        println!(
            "{:<18} {}",
            "Acquired:",
            version.acquired_at.format("%Y-%m-%d %H:%M:%S")
        );
        if let Some(filename) = &version.original_filename {
            println!("{:<18} {}", "Original Name:", filename);
        }
        if let Some(date) = &version.server_date {
            println!(
                "{:<18} {}",
                "Server Date:",
                date.format("%Y-%m-%d %H:%M:%S")
            );
        }
    }

    if doc.versions.len() > 1 {
        println!(
            "\n{} ({} versions total)",
            style("Version History").bold(),
            doc.versions.len()
        );
        println!("{}", "-".repeat(60));
        for (i, version) in doc.versions.iter().enumerate() {
            println!(
                "  {}. {} - {} ({})",
                i + 1,
                version.acquired_at.format("%Y-%m-%d"),
                &version.content_hash[..8],
                format_bytes(version.file_size)
            );
        }
    }

    if doc.extracted_text.is_some() {
        let text_len = doc.extracted_text.as_ref().map(|t| t.len()).unwrap_or(0);
        println!("\n{:<18} {} chars", "Extracted Text:", text_len);
    }

    Ok(())
}

/// Output document content to stdout.
async fn cmd_read(settings: &Settings, doc_id: &str, text_only: bool) -> anyhow::Result<()> {
    let db_path = settings.database_path();
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;

    // Find document
    let doc = match doc_repo.get(doc_id)? {
        Some(d) => d,
        None => {
            // Try partial match
            let all_docs = doc_repo.get_all()?;
            let matches: Vec<_> = all_docs
                .into_iter()
                .filter(|d| d.id.starts_with(doc_id))
                .collect();

            match matches.len() {
                0 => {
                    eprintln!("Document not found: {}", doc_id);
                    std::process::exit(1);
                }
                1 => matches.into_iter().next().unwrap(),
                _ => {
                    eprintln!("Multiple matches found for '{}', be more specific:", doc_id);
                    for d in &matches {
                        eprintln!("  {} - {}", &d.id[..8], truncate(&d.title, 50));
                    }
                    std::process::exit(1);
                }
            }
        }
    };

    if text_only {
        // Output extracted text
        match &doc.extracted_text {
            Some(text) => {
                print!("{}", text);
            }
            None => {
                eprintln!("No extracted text available for this document");
                eprintln!("Run 'foiacquire ocr' to extract text from documents");
                std::process::exit(1);
            }
        }
    } else {
        // Output binary file content
        let version = doc
            .current_version()
            .ok_or_else(|| anyhow::anyhow!("Document has no file version"))?;

        let content = std::fs::read(&version.file_path)?;

        use std::io::Write;
        std::io::stdout().write_all(&content)?;
    }

    Ok(())
}

/// Search documents by content or metadata.
async fn cmd_search(
    settings: &Settings,
    query: &str,
    source_id: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    let db_path = settings.database_path();
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;

    let query_lower = query.to_lowercase();

    // Get all documents and filter
    let documents: Vec<Document> = if let Some(sid) = source_id {
        doc_repo.get_by_source(sid)?
    } else {
        doc_repo.get_all()?
    };

    // Search in title, synopsis, tags, and extracted text
    let matches: Vec<_> = documents
        .into_iter()
        .filter(|doc| {
            // Check title
            if doc.title.to_lowercase().contains(&query_lower) {
                return true;
            }
            // Check synopsis
            if let Some(synopsis) = &doc.synopsis {
                if synopsis.to_lowercase().contains(&query_lower) {
                    return true;
                }
            }
            // Check tags
            if doc
                .tags
                .iter()
                .any(|t| t.to_lowercase().contains(&query_lower))
            {
                return true;
            }
            // Check extracted text
            if let Some(text) = &doc.extracted_text {
                if text.to_lowercase().contains(&query_lower) {
                    return true;
                }
            }
            false
        })
        .take(limit)
        .collect();

    if matches.is_empty() {
        println!(
            "{} No documents found matching '{}'",
            style("!").yellow(),
            query
        );
        return Ok(());
    }

    println!("\n{} results for '{}'\n", matches.len(), query);

    for doc in &matches {
        let version = doc.current_version();
        let mime = version.map(|v| mime_short(&v.mime_type)).unwrap_or("???");

        println!(
            "{} {} [{}]",
            style(&doc.id[..8]).cyan(),
            style(&doc.title).bold(),
            mime
        );

        // Show context of match
        if let Some(synopsis) = &doc.synopsis {
            if synopsis.to_lowercase().contains(&query_lower) {
                println!("  Synopsis: {}", truncate(synopsis, 80));
            }
        }

        if !doc.tags.is_empty() {
            let matching_tags: Vec<_> = doc
                .tags
                .iter()
                .filter(|t| t.to_lowercase().contains(&query_lower))
                .collect();
            if !matching_tags.is_empty() {
                println!(
                    "  Tags: {}",
                    matching_tags
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }

        // Show snippet from extracted text if match found there
        if let Some(text) = &doc.extracted_text {
            if let Some(pos) = text.to_lowercase().find(&query_lower) {
                let start = pos.saturating_sub(40);
                let end = (pos + query.len() + 40).min(text.len());
                let snippet: String = text[start..end].chars().collect();
                let snippet = snippet.replace('\n', " ");
                println!("  ...{}...", truncate(&snippet, 80));
            }
        }

        println!();
    }

    Ok(())
}

/// Convert MIME type to short form.
fn mime_short(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "pdf",
        m if m.starts_with("image/") => "image",
        m if m.contains("word") => "doc",
        m if m.contains("excel") || m.contains("spreadsheet") => "xls",
        "text/html" => "html",
        "text/plain" => "txt",
        _ => "other",
    }
}

/// Test browser-based fetching with stealth capabilities.
#[cfg(feature = "browser")]
#[allow(clippy::too_many_arguments)]
async fn cmd_browser_test(
    url: &str,
    headed: bool,
    engine: &str,
    proxy: Option<String>,
    browser_url: Option<String>,
    cookies_file: Option<std::path::PathBuf>,
    save_cookies: Option<std::path::PathBuf>,
    output: Option<std::path::PathBuf>,
    binary: bool,
    context_url: Option<String>,
) -> anyhow::Result<()> {
    use crate::scrapers::{BrowserEngineConfig, BrowserEngineType, BrowserFetcher};

    println!("{} Testing browser fetch: {}", style("→").cyan(), url);
    println!("  Engine: {}", engine);
    println!("  Headless: {}", !headed);
    println!("  Binary mode: {}", binary);
    if let Some(ref p) = proxy {
        println!("  Proxy: {}", p);
    }
    if let Some(ref b) = browser_url {
        println!("  Remote browser: {}", b);
    }
    if let Some(ref c) = cookies_file {
        println!("  Cookies: {:?}", c);
    }
    if let Some(ref ctx) = context_url {
        println!("  Context URL: {}", ctx);
    }

    let engine_type = match engine.to_lowercase().as_str() {
        "stealth" => BrowserEngineType::Stealth,
        "cookies" => BrowserEngineType::Cookies,
        "standard" => BrowserEngineType::Standard,
        _ => {
            println!(
                "{} Unknown engine '{}', using stealth",
                style("!").yellow(),
                engine
            );
            BrowserEngineType::Stealth
        }
    };

    let config = BrowserEngineConfig {
        engine: engine_type,
        headless: !headed,
        proxy,
        cookies_file,
        timeout: 30,
        wait_for_selector: None,
        chrome_args: vec![],
        remote_url: browser_url,
    };

    let mut fetcher = BrowserFetcher::new(config);

    println!("{} Launching browser...", style("→").cyan());

    // Binary fetch mode (for PDFs, images, etc.)
    if binary {
        match fetcher.fetch_binary(url, context_url.as_deref()).await {
            Ok(response) => {
                println!("{} Binary fetch successful!", style("✓").green());
                println!("  Status: {}", response.status);
                println!("  Content-Type: {}", response.content_type);
                println!("  Size: {} bytes", response.data.len());

                // Save binary content
                if let Some(output_path) = output {
                    std::fs::write(&output_path, &response.data)?;
                    println!("{} Saved binary to {:?}", style("✓").green(), output_path);

                    // Verify PDF magic bytes
                    if response.data.len() >= 4 && &response.data[0..4] == b"%PDF" {
                        println!("{} Verified: File is a valid PDF", style("✓").green());
                    } else if response.data.len() >= 4 {
                        println!(
                            "{} Warning: File does not have PDF magic bytes (got: {:?})",
                            style("!").yellow(),
                            &response.data[0..std::cmp::min(4, response.data.len())]
                        );
                    }
                } else {
                    println!(
                        "{} Use --output to save binary content",
                        style("!").yellow()
                    );
                }
            }
            Err(e) => {
                println!("{} Binary fetch failed: {}", style("✗").red(), e);
                return Err(e);
            }
        }
    } else {
        // Regular HTML fetch
        match fetcher.fetch(url).await {
            Ok(response) => {
                println!("{} Fetch successful!", style("✓").green());
                println!("  Final URL: {}", response.final_url);
                println!("  Status: {}", response.status);
                println!("  Content-Type: {}", response.content_type);
                println!("  Content length: {} bytes", response.content.len());

                // Check for common block indicators
                if response.content.contains("Access Denied") {
                    println!(
                        "{} Warning: Page contains 'Access Denied' - may be blocked",
                        style("!").yellow()
                    );
                }
                if response.content.contains("blocked") || response.content.contains("captcha") {
                    println!(
                        "{} Warning: Page may contain block/captcha indicators",
                        style("!").yellow()
                    );
                }

                // Save or print content
                if let Some(output_path) = output {
                    std::fs::write(&output_path, &response.content)?;
                    println!("{} Saved content to {:?}", style("✓").green(), output_path);
                } else {
                    // Print first 500 chars as preview
                    let preview: String = response.content.chars().take(500).collect();
                    println!(
                        "\n--- Content Preview ---\n{}\n--- End Preview ---",
                        preview
                    );
                }

                // Save cookies if requested
                if let Some(save_path) = save_cookies {
                    fetcher.save_cookies(&save_path).await?;
                    println!("{} Saved cookies to {:?}", style("✓").green(), save_path);
                }
            }
            Err(e) => {
                println!("{} Fetch failed: {}", style("✗").red(), e);
                return Err(e);
            }
        }
    }

    fetcher.close().await;

    Ok(())
}

/// Import documents from WARC archive files.
async fn cmd_import(
    settings: &Settings,
    files: &[PathBuf],
    source_id: Option<&str>,
    filter: Option<&str>,
    limit: usize,
    scan_limit: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    use std::collections::{HashMap, HashSet};
    use warc::{WarcHeader, WarcReader};

    let db_path = settings.database_path();
    let documents_dir = settings.documents_dir.clone();
    let doc_repo = DocumentRepository::new(&db_path, &documents_dir)?;
    let source_repo = SourceRepository::new(&db_path)?;

    // Pre-load all existing URLs into a HashSet for O(1) duplicate detection.
    // This is much faster than querying the DB for each WARC record.
    println!("{} Loading existing URLs for duplicate detection...", style("→").cyan());
    let mut existing_urls: HashSet<String> = doc_repo.get_all_urls_set().unwrap_or_default();
    println!("  {} existing URLs loaded", existing_urls.len());

    // Load all sources for URL matching
    let all_sources = source_repo.get_all()?;

    // Build URL prefix -> source_id map for auto-detection
    let source_map: HashMap<String, String> = all_sources
        .iter()
        .map(|s| (s.base_url.clone(), s.id.clone()))
        .collect();

    // If source_id provided, verify it exists
    if let Some(sid) = source_id {
        if source_repo.get(sid)?.is_none() {
            println!(
                "{} Source '{}' not found. Use 'source list' to see available sources.",
                style("✗").red(),
                sid
            );
            return Ok(());
        }
    }

    // Helper to find source from URL
    let find_source_for_url = |url: &str| -> Option<String> {
        // If explicitly provided, use that
        if let Some(sid) = source_id {
            return Some(sid.to_string());
        }
        // Otherwise, match against source base_urls
        for (base_url, sid) in &source_map {
            if url.starts_with(base_url) {
                return Some(sid.clone());
            }
        }
        None
    };

    // Compile filter regex if provided
    let filter_regex = if let Some(pattern) = filter {
        Some(regex::Regex::new(pattern)?)
    } else {
        None
    };

    if dry_run {
        println!("{} Dry run mode - no changes will be made", style("!").yellow());
    }

    let mut total_imported = 0;
    let mut total_skipped = 0;
    let mut total_filtered = 0;
    let mut total_no_source = 0;
    let mut total_errors = 0;
    let mut total_scanned = 0usize;

    for warc_path in files {
        println!(
            "\n{} Processing: {}",
            style("→").cyan(),
            warc_path.display()
        );

        if !warc_path.exists() {
            println!("{} File not found: {}", style("✗").red(), warc_path.display());
            total_errors += 1;
            continue;
        }

        // Detect if gzipped
        let is_gzip = warc_path
            .extension()
            .is_some_and(|ext| ext == "gz")
            || warc_path
                .to_string_lossy()
                .contains(".warc.gz");

        // Progress bar
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap(),
        );
        pb.enable_steady_tick(Duration::from_millis(100));

        let mut file_imported = 0;
        let mut file_skipped = 0;
        let mut file_filtered = 0;
        let mut file_no_source = 0;

        // Process based on compression - use macro to avoid code duplication
        macro_rules! process_warc {
            ($reader:expr) => {
                for record_result in $reader.iter_records() {
                    // Check import limit
                    if limit > 0 && total_imported >= limit {
                        pb.finish_with_message(format!("Import limit reached ({} documents)", limit));
                        break;
                    }

                    // Check scan limit
                    if scan_limit > 0 && total_scanned >= scan_limit {
                        pb.finish_with_message(format!("Scan limit reached ({} records)", scan_limit));
                        break;
                    }

                    total_scanned += 1;

                    let record = match record_result {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::debug!("Skipping malformed record: {}", e);
                            continue;
                        }
                    };

                    // Only process response records
                    let warc_type = record.header(WarcHeader::WarcType);
                    if warc_type.as_deref() != Some("response") {
                        continue;
                    }

                    // Get target URI
                    let target_uri = match record.header(WarcHeader::TargetURI) {
                        Some(uri) => uri.to_string(),
                        None => continue,
                    };

                    // Apply filter
                    if let Some(ref regex) = filter_regex {
                        if !regex.is_match(&target_uri) {
                            file_filtered += 1;
                            continue;
                        }
                    }

                    pb.set_message(format!(
                        "Processing: {}",
                        &target_uri[..target_uri.len().min(60)]
                    ));

                    // Get body content
                    let body = record.body();
                    if body.is_empty() {
                        continue;
                    }

                    // Parse HTTP response from body
                    let (headers, content) = match parse_http_response(body) {
                        Some(parsed) => parsed,
                        None => {
                            tracing::debug!("Could not parse HTTP response for {}", target_uri);
                            continue;
                        }
                    };

                    // Skip non-success responses
                    if !headers.status_ok {
                        continue;
                    }

                    // Skip empty content
                    if content.is_empty() {
                        continue;
                    }

                    // Auto-detect source from URL
                    let detected_source = find_source_for_url(&target_uri);
                    let effective_source_id = match &detected_source {
                        Some(sid) => sid.as_str(),
                        None => {
                            file_no_source += 1;
                            tracing::debug!("No matching source for URL: {}", target_uri);
                            continue;
                        }
                    };

                    // Check if document already exists (O(1) HashSet lookup)
                    if existing_urls.contains(&target_uri) {
                        file_skipped += 1;
                        continue;
                    }

                    // Extract title from URL
                    let title = crate::scrapers::extract_title_from_url(&target_uri);

                    // Determine MIME type
                    let mime_type = headers
                        .content_type
                        .clone()
                        .unwrap_or_else(|| guess_mime_type(&target_uri));

                    if dry_run {
                        println!(
                            "  {} [{}] {} ({}, {} bytes)",
                            style("+").green(),
                            effective_source_id,
                            target_uri,
                            mime_type,
                            content.len()
                        );
                        file_imported += 1;
                        total_imported += 1;
                    } else {
                        // Create ScraperResult for helper
                        let result = crate::scrapers::ScraperResult::new(
                            target_uri.clone(),
                            title,
                            content.to_vec(),
                            mime_type,
                        );

                        // Save using existing helper
                        match super::helpers::save_scraped_document(
                            &doc_repo,
                            content,
                            &result,
                            effective_source_id,
                            &documents_dir,
                        ) {
                            Ok(_) => {
                                // Add to URL cache to avoid re-importing in same session
                                existing_urls.insert(target_uri.clone());
                                file_imported += 1;
                                total_imported += 1;
                            }
                            Err(e) => {
                                tracing::warn!("Failed to import {}: {}", target_uri, e);
                                total_errors += 1;
                            }
                        }
                    }
                }
            };
        }

        // Open and process WARC file
        if is_gzip {
            match WarcReader::from_path_gzip(warc_path) {
                Ok(reader) => process_warc!(reader),
                Err(e) => {
                    println!("{} Failed to open WARC file: {}", style("✗").red(), e);
                    total_errors += 1;
                    continue;
                }
            }
        } else {
            match WarcReader::from_path(warc_path) {
                Ok(reader) => process_warc!(reader),
                Err(e) => {
                    println!("{} Failed to open WARC file: {}", style("✗").red(), e);
                    total_errors += 1;
                    continue;
                }
            }
        }

        pb.finish_and_clear();

        println!(
            "  {} imported, {} skipped (existing), {} filtered, {} no source",
            style(file_imported).green(),
            style(file_skipped).yellow(),
            style(file_filtered).dim(),
            style(file_no_source).dim()
        );

        total_skipped += file_skipped;
        total_filtered += file_filtered;
        total_no_source += file_no_source;
    }

    // Summary
    println!("\n{} Import complete:", style("✓").green());
    println!("  Records scanned:    {}", style(total_scanned).dim());
    println!("  Documents imported: {}", style(total_imported).green());
    println!("  Documents skipped:  {}", style(total_skipped).yellow());
    println!("  Records filtered:   {}", style(total_filtered).dim());
    if total_no_source > 0 {
        println!(
            "  No matching source: {} (use --source to specify)",
            style(total_no_source).yellow()
        );
    }
    if total_errors > 0 {
        println!("  Errors:             {}", style(total_errors).red());
    }

    Ok(())
}

/// Parse HTTP response from WARC body bytes.
/// Returns (headers, body content) if successful.
fn parse_http_response(data: &[u8]) -> Option<(HttpResponseHeaders, &[u8])> {
    // Find header/body separator (double CRLF)
    let separator = b"\r\n\r\n";
    let sep_pos = data
        .windows(separator.len())
        .position(|w| w == separator)?;

    let header_bytes = &data[..sep_pos];
    let body = &data[sep_pos + separator.len()..];

    // Parse status line and headers
    let header_str = std::str::from_utf8(header_bytes).ok()?;
    let mut lines = header_str.lines();

    // Parse status line: "HTTP/1.1 200 OK"
    let status_line = lines.next()?;
    let status_ok = status_line.contains(" 200 ") || status_line.contains(" 206 ");

    // Parse headers
    let mut content_type = None;
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_lowercase();
            let value = value.trim();
            if key == "content-type" {
                // Extract just the MIME type, not charset etc.
                content_type = Some(
                    value
                        .split(';')
                        .next()
                        .unwrap_or(value)
                        .trim()
                        .to_string(),
                );
            }
        }
    }

    Some((
        HttpResponseHeaders {
            status_ok,
            content_type,
        },
        body,
    ))
}

/// HTTP response headers extracted from WARC body.
struct HttpResponseHeaders {
    status_ok: bool,
    content_type: Option<String>,
}

/// Guess MIME type from URL extension.
fn guess_mime_type(url: &str) -> String {
    let path = url.split('?').next().unwrap_or(url);
    if path.ends_with(".pdf") || path.ends_with(".PDF") {
        "application/pdf".to_string()
    } else if path.ends_with(".html") || path.ends_with(".htm") {
        "text/html".to_string()
    } else if path.ends_with(".txt") {
        "text/plain".to_string()
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        "image/jpeg".to_string()
    } else if path.ends_with(".png") {
        "image/png".to_string()
    } else if path.ends_with(".gif") {
        "image/gif".to_string()
    } else if path.ends_with(".doc") {
        "application/msword".to_string()
    } else if path.ends_with(".docx") {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document".to_string()
    } else {
        "application/octet-stream".to_string()
    }
}

/// Discover new document URLs by analyzing patterns in existing URLs.
async fn cmd_discover(
    settings: &Settings,
    source_id: &str,
    limit: usize,
    dry_run: bool,
    min_examples: usize,
) -> anyhow::Result<()> {
    use crate::models::{CrawlUrl, DiscoveryMethod};
    use regex::Regex;
    use std::collections::{HashMap, HashSet};

    let db_path = settings.database_path();
    let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;
    let crawl_repo = CrawlRepository::new(&db_path)?;

    println!(
        "{} Analyzing URL patterns for source: {}",
        style("🔍").cyan(),
        style(source_id).bold()
    );

    // Get just the URLs for this source (lightweight query)
    let urls = doc_repo.get_urls_by_source(source_id)?;
    if urls.is_empty() {
        println!("{} No documents found for source {}", style("!").yellow(), source_id);
        return Ok(());
    }

    println!("  Found {} existing document URLs", urls.len());

    // === PHASE 1: Parent Directory Discovery ===
    // Extract unique parent directories from URLs that might have directory listings
    println!(
        "\n{} Phase 1: Analyzing parent directories...",
        style("📁").cyan()
    );

    let mut parent_dirs: HashSet<String> = HashSet::new();

    // Sample URLs if there are too many (parent dirs converge quickly)
    let sample_size = 10000.min(urls.len());
    let sample_urls: Vec<_> = if urls.len() > sample_size {
        println!("  Sampling {} of {} URLs for directory analysis", sample_size, urls.len());
        urls.iter().step_by(urls.len() / sample_size).take(sample_size).collect()
    } else {
        urls.iter().collect()
    };

    for url in sample_urls {
        if let Ok(parsed) = url::Url::parse(url) {
            let mut path = parsed.path().to_string();

            // Remove the filename to get the directory
            if let Some(last_slash) = path.rfind('/') {
                path = path[..=last_slash].to_string();
            }

            // Walk up the directory tree
            while path.len() > 1 {
                // Reconstruct the full URL with this path
                let mut parent_url = parsed.clone();
                parent_url.set_path(&path);
                parent_url.set_query(None);
                parent_url.set_fragment(None);

                let parent_str = parent_url.to_string();

                // Don't add if it matches an existing document URL
                if !urls.contains(&parent_str) {
                    parent_dirs.insert(parent_str);
                }

                // Move up one level
                if path.ends_with('/') {
                    path = path[..path.len() - 1].to_string();
                }
                if let Some(last_slash) = path.rfind('/') {
                    path = path[..=last_slash].to_string();
                } else {
                    break;
                }
            }
        }
    }

    println!(
        "  Found {} unique parent directories",
        parent_dirs.len()
    );

    // === PHASE 2: Numeric Pattern Enumeration ===
    println!(
        "\n{} Phase 2: Analyzing numeric patterns...",
        style("🔢").cyan()
    );

    // Find patterns with numeric sequences
    // Pattern: look for numbers in URLs and try to find ranges
    let num_regex = Regex::new(r"\d+").unwrap();

    // Group URLs by their "template" (URL with numbers replaced by placeholder)
    let mut templates: HashMap<String, Vec<(String, Vec<u64>)>> = HashMap::new();

    for url in &urls {
        // Find all numeric sequences in the URL
        let nums: Vec<u64> = num_regex
            .find_iter(url)
            .filter_map(|m| m.as_str().parse().ok())
            .collect();

        if nums.is_empty() {
            continue;
        }

        // Create template by replacing all numbers with {N} for grouping
        let template = num_regex.replace_all(url, "{N}").to_string();
        templates
            .entry(template)
            .or_default()
            .push((url.to_string(), nums));
    }

    // Filter to templates with enough examples
    let viable_templates: Vec<_> = templates
        .iter()
        .filter(|(_, examples)| examples.len() >= min_examples)
        .collect();

    if viable_templates.is_empty() {
        println!(
            "{} No URL patterns found with at least {} examples",
            style("!").yellow(),
            min_examples
        );
        return Ok(());
    }

    println!(
        "\n{} Found {} URL pattern(s) with {} or more examples:",
        style("📊").cyan(),
        viable_templates.len(),
        min_examples
    );

    let mut total_candidates = 0;
    let mut new_urls: Vec<String> = Vec::new();

    // Get existing URLs to avoid duplicates
    let existing_urls: HashSet<String> = urls.iter().cloned().collect();
    let queued_urls: HashSet<String> = crawl_repo
        .get_pending_urls(source_id, 0)?
        .into_iter()
        .map(|u| u.url)
        .collect();

    for (template, examples) in viable_templates {
        println!("\n  Template: {}", style(template).dim());
        println!("  Examples: {} URLs", examples.len());

        // For each position in the template, find the range of numbers
        if examples.is_empty() {
            continue;
        }

        // Get the number of numeric positions from first example
        let num_positions = examples[0].1.len();
        if num_positions == 0 {
            continue;
        }

        // Focus on the last numeric position (most likely to be the document ID)
        let last_pos = num_positions - 1;
        let mut seen_nums: Vec<u64> = examples.iter().map(|(_, nums)| nums[last_pos]).collect();
        seen_nums.sort();
        seen_nums.dedup();

        if seen_nums.len() < 2 {
            continue;
        }

        let min_num = *seen_nums.first().unwrap();
        let max_num = *seen_nums.last().unwrap();
        let gaps: Vec<u64> = (min_num..=max_num)
            .filter(|n| !seen_nums.contains(n))
            .collect();

        println!(
            "  Last numeric position: {} - {} ({} gaps)",
            min_num,
            max_num,
            gaps.len()
        );

        // Generate candidate URLs for gaps
        let base_url = &examples[0].0;
        let base_nums = &examples[0].1;

        for gap_num in &gaps {
            // Reconstruct URL with the gap number
            let mut candidate = base_url.clone();
            let mut offset = 0i64;

            for (idx, m) in num_regex.find_iter(base_url).enumerate() {
                let replacement = if idx == last_pos {
                    gap_num.to_string()
                } else {
                    base_nums[idx].to_string()
                };

                let start = (m.start() as i64 + offset) as usize;
                let end = (m.end() as i64 + offset) as usize;
                let old_len = end - start;
                let new_len = replacement.len();

                candidate = format!(
                    "{}{}{}",
                    &candidate[..start],
                    replacement,
                    &candidate[end..]
                );
                offset += new_len as i64 - old_len as i64;
            }

            if !existing_urls.contains(&candidate) && !queued_urls.contains(&candidate) {
                new_urls.push(candidate);
                total_candidates += 1;

                if limit > 0 && total_candidates >= limit {
                    break;
                }
            }
        }

        // Also try extending beyond the range
        let extend_count = 10.min(max_num - min_num + 1);
        for i in 1..=extend_count {
            let extended_num = max_num + i;

            // Reconstruct URL with the extended number
            let mut candidate = base_url.clone();
            let mut offset = 0i64;

            for (idx, m) in num_regex.find_iter(base_url).enumerate() {
                let replacement = if idx == last_pos {
                    extended_num.to_string()
                } else {
                    base_nums[idx].to_string()
                };

                let start = (m.start() as i64 + offset) as usize;
                let end = (m.end() as i64 + offset) as usize;
                let old_len = end - start;
                let new_len = replacement.len();

                candidate = format!(
                    "{}{}{}",
                    &candidate[..start],
                    replacement,
                    &candidate[end..]
                );
                offset += new_len as i64 - old_len as i64;
            }

            if !existing_urls.contains(&candidate) && !queued_urls.contains(&candidate) {
                new_urls.push(candidate);
                total_candidates += 1;

                if limit > 0 && total_candidates >= limit {
                    break;
                }
            }
        }

        if limit > 0 && total_candidates >= limit {
            break;
        }
    }

    // Filter parent directories to exclude already queued ones
    let new_parent_dirs: Vec<String> = parent_dirs
        .into_iter()
        .filter(|u| !existing_urls.contains(u) && !queued_urls.contains(u))
        .collect();

    println!(
        "\n{} Summary:",
        style("📊").cyan()
    );
    println!("  {} parent directories to explore", new_parent_dirs.len());
    println!("  {} candidate URLs from patterns", new_urls.len());

    let total_new = new_parent_dirs.len() + new_urls.len();
    if total_new == 0 {
        println!("\n{} No new URLs to discover (all already queued or fetched)", style("!").yellow());
        return Ok(());
    }

    if dry_run {
        println!("\n{} Dry run - would add these URLs:", style("ℹ").blue());

        println!("\n  Parent directories (for directory listing discovery):");
        for url in new_parent_dirs.iter().take(10) {
            println!("    {}", url);
        }
        if new_parent_dirs.len() > 10 {
            println!("    ... and {} more directories", new_parent_dirs.len() - 10);
        }

        println!("\n  Pattern-enumerated URLs:");
        for url in new_urls.iter().take(10) {
            println!("    {}", url);
        }
        if new_urls.len() > 10 {
            println!("    ... and {} more pattern URLs", new_urls.len() - 10);
        }
    } else {
        println!("\n{} Adding URLs to crawl queue...", style("📥").cyan());

        let mut added = 0;

        // Add parent directories (these will be crawled for links, not as documents)
        for url in &new_parent_dirs {
            let crawl_url = CrawlUrl::new(
                url.clone(),
                source_id.to_string(),
                DiscoveryMethod::PatternEnumeration, // Use same method for now
                None,
                0,
            );

            match crawl_repo.add_url(&crawl_url) {
                Ok(true) => added += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!("Failed to add directory URL {}: {}", url, e),
            }
        }

        // Add pattern-enumerated URLs
        for url in &new_urls {
            let crawl_url = CrawlUrl::new(
                url.clone(),
                source_id.to_string(),
                DiscoveryMethod::PatternEnumeration,
                None,
                0,
            );

            match crawl_repo.add_url(&crawl_url) {
                Ok(true) => added += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!("Failed to add URL {}: {}", url, e),
            }
        }

        println!(
            "{} Added {} URLs to crawl queue",
            style("✓").green(),
            added
        );
        println!(
            "  Run {} to crawl discovered URLs",
            style(format!("foiacquire crawl {}", source_id)).cyan()
        );
        println!(
            "  Run {} to download discovered documents",
            style(format!("foiacquire download {}", source_id)).cyan()
        );
    }

    Ok(())
}
