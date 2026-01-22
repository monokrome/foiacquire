//! CLI commands implementation.
//!
//! This module contains the CLI parser and dispatches to command-specific modules.

mod analyze;
mod annotate;
mod config_cmd;
mod db;
mod discover;
mod documents;
mod helpers;
mod import;
mod init;
mod llm;
mod scrape;
mod serve;
mod source;
mod state;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::config::{load_settings_with_options, LoadOptions};

// Re-export ReloadMode for use by other modules
pub use scrape::ReloadMode;

/// Backend type for rate limiting storage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum RateLimitBackendType {
    /// In-memory (single process, not persisted)
    Memory,
    /// Database via Diesel (SQLite or PostgreSQL, persisted, multi-process)
    #[default]
    Database,
    /// Redis (distributed, requires redis-backend feature)
    #[cfg(feature = "redis-backend")]
    Redis,
}

#[derive(Parser)]
#[command(name = "foia")]
#[command(about = "FOIA document acquisition and research system")]
#[command(version)]
pub struct Cli {
    /// Target directory or database file (overrides config file).
    /// Can be a directory containing foiacquire.db or a .db file directly.
    #[arg(long, short = 't', global = true)]
    target: Option<PathBuf>,

    /// Config file path (overrides auto-discovery)
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Resolve relative paths from current working directory instead of config file location
    #[arg(long, global = true)]
    cwd: bool,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Disable Tor (INSECURE - your IP will be exposed to target servers)
    #[arg(short = 'D', long, global = true)]
    direct: bool,

    /// Use Tor without obfuscation (detectable as Tor traffic)
    #[arg(long, global = true)]
    no_obfuscation: bool,

    /// Security warning countdown delay in seconds (0 to skip countdown)
    #[arg(long, global = true)]
    privacy_warning_delay: Option<u64>,

    /// Disable Tor legality warning
    #[arg(long, global = true)]
    no_tor_warning: bool,

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

    /// Configuration management
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },

    /// Database management (copy between SQLite/Postgres)
    Db {
        #[command(subcommand)]
        command: DbCommands,
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
        /// Limit number of documents to download per source per cycle (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Show detailed progress for each file
        #[arg(short = 'P', long)]
        progress: bool,
        /// Run continuously, checking for new work
        #[arg(long)]
        daemon: bool,
        /// Seconds to wait between checks in daemon mode (default: 300)
        #[arg(long, default_value = "300")]
        interval: u64,
        /// Config reload behavior in daemon mode [default: next-run, or inplace if flag used without value]
        #[arg(short = 'r', long, value_enum, num_args = 0..=1, default_value = "next-run", default_missing_value = "inplace", require_equals = true)]
        reload: ReloadMode,
        /// Rate limit backend: memory, database (default), or redis
        #[arg(long, value_enum, default_value = "database")]
        rate_limit_backend: RateLimitBackendType,
    },

    /// Show system status
    Status {
        /// Server URL to fetch status from (e.g., http://localhost:3030).
        /// Can also be set via FOIA_API_URL environment variable.
        #[arg(long, short, env = "FOIA_API_URL")]
        url: Option<String>,

        /// Source ID to filter status (optional)
        source_id: Option<String>,

        /// Continuously refresh status display (TUI mode)
        #[arg(long)]
        live: bool,

        /// Refresh interval in seconds
        #[arg(long, default_value = "5")]
        interval: u64,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Analyze documents: detect content types, extract text, and run OCR
    Analyze {
        /// Source ID (optional, processes all sources if not specified)
        source_id: Option<String>,
        /// Specific document ID to process
        #[arg(long)]
        doc_id: Option<String>,
        /// Analysis methods to run (comma-separated: ocr,whisper,custom_name)
        /// Default: ocr (or config default_methods)
        #[arg(short, long)]
        method: Option<String>,
        /// Number of workers (default: 2)
        #[arg(short, long, default_value = "2")]
        workers: usize,
        /// Limit number of documents to process per cycle (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Extract URLs from documents and add to crawl queue
        #[arg(long)]
        extract_urls: bool,
        /// Filter by mime type (e.g., application/pdf)
        #[arg(long)]
        mime_type: Option<String>,
        /// Run continuously, checking for new work
        #[arg(long)]
        daemon: bool,
        /// Seconds to wait between checks in daemon mode (default: 60)
        #[arg(long, default_value = "60")]
        interval: u64,
        /// Config reload behavior in daemon mode [default: next-run, or inplace if flag used without value]
        #[arg(short = 'r', long, value_enum, num_args = 0..=1, default_value = "next-run", default_missing_value = "inplace", require_equals = true)]
        reload: ReloadMode,
    },

    /// Check if required analysis tools (OCR, etc.) are installed
    AnalyzeCheck,

    /// Compare OCR backends on an image or PDF
    AnalyzeCompare {
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

    /// Start web server to browse documents (as Tor hidden service by default)
    Serve {
        /// Address to bind to: PORT, HOST, or HOST:PORT (default: 127.0.0.1:3030)
        #[arg(default_value = "127.0.0.1:3030")]
        bind: String,

        /// Skip automatic database migration on startup
        #[arg(long)]
        no_migrate: bool,

        /// Disable hidden service (clearnet only - shows security warning)
        #[arg(long)]
        no_hidden_service: bool,

        /// Use experimental Arti for hidden service instead of C-Tor
        /// (requires allow_potentially_insecure_circuits in config)
        #[arg(long)]
        use_arti: bool,
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

    /// Annotate documents using LLM (generates synopsis and tags)
    Annotate {
        #[command(subcommand)]
        command: Option<AnnotateCommands>,

        /// Source ID (optional, processes all sources if not specified)
        #[arg(long)]
        source_id: Option<String>,
        /// Specific document ID to process
        #[arg(long)]
        doc_id: Option<String>,
        /// Limit number of documents to process per cycle (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// LLM API endpoint (e.g., http://localhost:11434)
        #[arg(long)]
        endpoint: Option<String>,
        /// LLM model name (e.g., dolphin-llama3:8b)
        #[arg(long)]
        model: Option<String>,
        /// Run continuously, checking for new work
        #[arg(short, long)]
        daemon: bool,
        /// Seconds to wait between checks in daemon mode (default: 60)
        #[arg(long, default_value = "60")]
        interval: u64,
        /// Config reload behavior in daemon mode [default: next-run, or inplace if flag used without value]
        #[arg(short = 'r', long, value_enum, num_args = 0..=1, default_value = "next-run", default_missing_value = "inplace", require_equals = true)]
        reload: ReloadMode,
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

    /// Import documents or URLs from various sources
    Import {
        #[command(subcommand)]
        command: ImportCommands,
    },

    /// Discover new document URLs using various methods
    Discover {
        #[command(subcommand)]
        command: DiscoverCommands,
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
enum ConfigCommands {
    /// Recover a skeleton config from an existing database (generates from sources)
    Recover {
        /// Path to the database file
        database: PathBuf,
        /// Output file (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Restore the most recent config from database history
    Restore {
        /// Output file (default: foiacquire.json next to the database)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// List configuration history entries
    History {
        /// Show full config data (default: show summary only)
        #[arg(long)]
        full: bool,
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

#[derive(Subcommand)]
enum DiscoverCommands {
    /// Discover URLs by analyzing patterns in existing URLs
    Pattern {
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

    /// Discover URLs using external search engines
    Search {
        /// Source ID (used to determine target domain)
        source_id: String,
        /// Search engines to use (comma-separated: duckduckgo,google,bing,brave)
        #[arg(short, long, default_value = "duckduckgo")]
        engines: String,
        /// Search terms (comma-separated). Uses source's configured terms if not specified.
        #[arg(short, long)]
        terms: Option<String>,
        /// Expand terms using LLM
        #[arg(long)]
        expand: bool,
        /// Extract terms from HTML templates
        #[arg(long)]
        template: bool,
        /// Maximum results per query (default: 100)
        #[arg(short, long, default_value = "100")]
        limit: usize,
        /// Show what would be discovered without adding to queue
        #[arg(long)]
        dry_run: bool,
    },

    /// Discover URLs from sitemaps and robots.txt
    Sitemap {
        /// Source ID (used to determine target domain)
        source_id: String,
        /// Maximum URLs to discover (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Show what would be discovered without adding to queue
        #[arg(long)]
        dry_run: bool,
    },

    /// Discover URLs from Wayback Machine historical snapshots
    Wayback {
        /// Source ID (used to determine target domain)
        source_id: String,
        /// Start date (YYYYMMDD format)
        #[arg(long)]
        from: Option<String>,
        /// End date (YYYYMMDD format)
        #[arg(long)]
        to: Option<String>,
        /// Maximum URLs to discover (0 = unlimited)
        #[arg(short, long, default_value = "1000")]
        limit: usize,
        /// Show what would be discovered without adding to queue
        #[arg(long)]
        dry_run: bool,
    },

    /// Discover URLs by checking common document paths
    Paths {
        /// Source ID (used to determine target domain)
        source_id: String,
        /// Additional paths to check (comma-separated)
        #[arg(short = 'p', long)]
        extra_paths: Option<String>,
        /// Show what would be discovered without adding to queue
        #[arg(long)]
        dry_run: bool,
    },

    /// Run all discovery methods
    All {
        /// Source ID
        source_id: String,
        /// Show what would be discovered without adding to queue
        #[arg(long)]
        dry_run: bool,
        /// Maximum URLs per discovery method (0 = unlimited)
        #[arg(short, long, default_value = "500")]
        limit: usize,
    },
}

#[derive(Subcommand)]
enum AnnotateCommands {
    /// Reset annotations to allow re-processing
    Reset {
        /// Source ID (optional, resets all sources if not specified)
        #[arg(long)]
        source_id: Option<String>,
        /// Skip confirmation prompt
        #[arg(long)]
        confirm: bool,
    },
}

#[derive(Subcommand)]
enum ImportCommands {
    /// Import documents from WARC (Web Archive) files
    Warc {
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
        /// Disable resume support - ignore progress files and start from beginning
        #[arg(long)]
        no_resume: bool,
        /// How often to save progress (in records). Set to 0 to disable checkpointing.
        #[arg(long, default_value = "10000")]
        checkpoint_interval: usize,
    },

    /// Import URLs from a file to add to the crawl queue
    Urls {
        /// File containing URLs (one per line)
        #[arg(short, long)]
        file: PathBuf,
        /// Source ID to associate URLs with (required)
        #[arg(short, long)]
        source: String,
        /// Discovery method to tag URLs with (default: "import")
        #[arg(long, default_value = "import")]
        method: String,
        /// Skip invalid URLs instead of failing
        #[arg(long)]
        skip_invalid: bool,
    },

    /// Import document content from stdin
    Stdin {
        /// URL to associate with the imported content
        #[arg(short, long)]
        url: String,
        /// Source ID to associate the document with (required)
        #[arg(short, long)]
        source: String,
        /// Content type (MIME type, auto-detected if not specified)
        #[arg(short = 't', long)]
        content_type: Option<String>,
        /// Original filename (extracted from URL if not specified)
        #[arg(short = 'n', long)]
        filename: Option<String>,
    },
}

#[derive(Subcommand)]
enum DbCommands {
    /// Run database migrations
    Migrate {
        /// Only check migration status, don't run migrations
        #[arg(long)]
        check: bool,

        /// Force re-run migrations even if schema appears up-to-date
        #[arg(long)]
        force: bool,
    },

    /// Copy data between databases (e.g., SQLite to Postgres)
    Copy {
        /// Source database URL (e.g., ./data.db or postgres://user:pass@host/db)
        from: String,
        /// Destination database URL
        to: String,
        /// Clear destination database before copy
        #[arg(long)]
        clear: bool,
        /// Batch size for inserts (default: 1000)
        #[arg(long, default_value = "1000")]
        batch_size: usize,
        /// Use COPY command for faster initial load (requires empty target, Postgres only)
        #[arg(long)]
        copy: bool,
        /// Show progress bars (requires counting records first)
        #[arg(long)]
        progress: bool,
        /// Only copy specific tables (comma-separated). Available: sources, documents,
        /// document_versions, document_pages, virtual_files, crawl_urls, crawl_requests,
        /// crawl_config, configuration_history, rate_limit_state
        // num_args + default_missing_value allows `--tables` without a value, so we can
        // show a helpful message listing available tables instead of clap's generic error
        #[arg(long, num_args = 0..=1, default_missing_value = "")]
        tables: Option<String>,
        /// Run ANALYZE on copied tables afterward (Postgres only, recommended after bulk loads)
        #[arg(long)]
        analyze: bool,
        /// Skip duplicate records and log them to the specified CSV file (table,id format)
        #[arg(long, value_name = "FILE")]
        skip_duplicates: Option<String>,
    },

    /// Remap document categories based on MIME types
    RemapCategories {
        /// Only show what would be changed, don't actually update
        #[arg(long)]
        dry_run: bool,
        /// Batch size for scanning and updating (default: 4096)
        #[arg(long, default_value = "4096")]
        batch_size: usize,
    },

    /// Deduplicate documents by content hash
    Deduplicate {
        /// Only show what would be deleted, don't actually delete
        #[arg(long)]
        dry_run: bool,
        /// Keep strategy: oldest (default), newest, or most-complete
        #[arg(long, default_value = "oldest")]
        keep: String,
        /// Only deduplicate within a single source (don't merge cross-source)
        #[arg(long)]
        same_source: bool,
        /// Batch size for processing (default: 1000)
        #[arg(long, default_value = "1000")]
        batch_size: usize,
    },
}

/// Run the CLI.
pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let options = LoadOptions {
        config_path: cli.config,
        use_cwd: cli.cwd,
        target: cli.target,
    };
    let (settings, mut config) = load_settings_with_options(options).await;

    // Apply CLI privacy overrides
    config.privacy = config.privacy.with_cli_overrides(
        cli.direct,
        cli.no_obfuscation,
        cli.privacy_warning_delay,
        cli.no_tor_warning,
    );

    // Show Tor legality warning (can be disabled)
    config.privacy.show_tor_legal_warning();

    // Check Tor availability when needed (skip for commands that don't need network)
    let needs_tor = !matches!(
        cli.command,
        Commands::Init | Commands::Source { .. } | Commands::Config { .. }
    );
    if needs_tor {
        if let Err(e) = config.privacy.check_tor_availability() {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    }

    // Enforce security warning with countdown if insecure (cannot be disabled)
    config.privacy.enforce_security_warning().await;

    match cli.command {
        Commands::Init => init::cmd_init(&settings).await,
        Commands::Source { command } => match command {
            SourceCommands::List => source::cmd_source_list(&settings).await,
            SourceCommands::Rename {
                old_id,
                new_id,
                confirm,
            } => source::cmd_source_rename(&settings, &old_id, &new_id, confirm).await,
        },
        Commands::Crawl { source_id, limit } => {
            state::cmd_crawl(&settings, &source_id, limit).await
        }
        Commands::Download {
            source_id,
            workers,
            limit,
            progress,
        } => {
            scrape::cmd_download(
                &settings,
                source_id.as_deref(),
                workers,
                limit,
                progress,
                &config.privacy,
            )
            .await
        }
        Commands::State { command } => match command {
            StateCommands::Status { source_id } => {
                state::cmd_crawl_status(&settings, source_id).await
            }
            StateCommands::Clear { source_id, confirm } => {
                state::cmd_crawl_clear(&settings, &source_id, confirm).await
            }
        },
        Commands::Config { command } => match command {
            ConfigCommands::Recover { database, output } => {
                config_cmd::cmd_config_recover(&database, output.as_deref()).await
            }
            ConfigCommands::Restore { output } => {
                config_cmd::cmd_config_restore(&settings, output.as_deref()).await
            }
            ConfigCommands::History { full } => {
                config_cmd::cmd_config_history(&settings, full).await
            }
        },
        Commands::Db { command } => match command {
            DbCommands::Migrate { check, force } => db::cmd_migrate(&settings, check, force).await,
            DbCommands::Copy {
                from,
                to,
                clear,
                batch_size,
                copy,
                progress,
                tables,
                analyze,
                skip_duplicates,
            } => {
                db::cmd_db_copy(
                    &from,
                    &to,
                    clear,
                    batch_size,
                    copy,
                    progress,
                    tables,
                    analyze,
                    skip_duplicates,
                )
                .await
            }
            DbCommands::RemapCategories {
                dry_run,
                batch_size,
            } => db::cmd_db_remap_categories(&settings, dry_run, batch_size).await,
            DbCommands::Deduplicate {
                dry_run,
                keep,
                same_source,
                batch_size,
            } => db::cmd_db_dedup(&settings, dry_run, &keep, same_source, batch_size).await,
        },
        Commands::Scrape {
            source_ids,
            all,
            workers,
            limit,
            progress,
            daemon,
            interval,
            reload,
            rate_limit_backend,
        } => {
            scrape::cmd_scrape(
                &settings,
                &source_ids,
                all,
                workers,
                limit,
                progress,
                daemon,
                interval,
                reload,
                rate_limit_backend,
                &config.privacy,
            )
            .await
        }
        Commands::Status {
            url,
            source_id,
            live,
            interval,
            json,
        } => scrape::cmd_status(&settings, url, source_id, live, interval, json).await,
        Commands::Analyze {
            source_id,
            doc_id,
            method,
            workers,
            limit,
            mime_type,
            daemon,
            interval,
            reload,
            ..
        } => {
            analyze::cmd_analyze(
                &settings,
                source_id.as_deref(),
                doc_id.as_deref(),
                method.as_deref(),
                workers,
                limit,
                mime_type.as_deref(),
                daemon,
                interval,
                reload,
            )
            .await
        }
        Commands::AnalyzeCheck => analyze::cmd_analyze_check().await,
        Commands::AnalyzeCompare {
            file,
            pages,
            backends,
            deepseek_path,
        } => analyze::cmd_analyze_compare(&file, pages.as_deref(), &backends, deepseek_path).await,
        Commands::Serve {
            bind,
            no_migrate,
            no_hidden_service,
            use_arti,
        } => {
            serve::cmd_serve(
                &settings,
                &config,
                &bind,
                no_migrate,
                no_hidden_service,
                use_arti,
            )
            .await
        }
        Commands::Refresh {
            source_id,
            workers,
            limit,
            force,
        } => {
            scrape::cmd_refresh(
                &settings,
                source_id.as_deref(),
                workers,
                limit,
                force,
                &config.privacy,
            )
            .await
        }
        Commands::Annotate {
            command,
            source_id,
            doc_id,
            limit,
            endpoint,
            model,
            daemon,
            interval,
            reload,
        } => match command {
            Some(AnnotateCommands::Reset { source_id, confirm }) => {
                annotate::cmd_annotate_reset(&settings, source_id.as_deref(), confirm).await
            }
            None => {
                annotate::cmd_annotate(
                    &settings,
                    source_id.as_deref(),
                    doc_id.as_deref(),
                    limit,
                    endpoint,
                    model,
                    daemon,
                    interval,
                    reload,
                )
                .await
            }
        },
        Commands::DetectDates {
            source_id,
            limit,
            dry_run,
        } => annotate::cmd_detect_dates(&settings, source_id.as_deref(), limit, dry_run).await,
        Commands::LlmModels => llm::cmd_llm_models(&settings).await,
        Commands::Archive {
            source_id,
            limit,
            ocr,
        } => documents::cmd_archive(&settings, source_id.as_deref(), limit, ocr).await,
        Commands::Ls {
            source,
            tag,
            type_filter,
            limit,
            format,
        } => {
            documents::cmd_ls(
                &settings,
                source.as_deref(),
                tag.as_deref(),
                type_filter.as_deref(),
                limit,
                &format,
            )
            .await
        }
        Commands::Info { doc_id } => documents::cmd_info(&settings, &doc_id).await,
        Commands::Read { doc_id, text } => documents::cmd_read(&settings, &doc_id, text).await,
        Commands::Search {
            query,
            source,
            limit,
        } => documents::cmd_search(&settings, &query, source.as_deref(), limit).await,
        Commands::Import { command } => match command {
            ImportCommands::Warc {
                files,
                source,
                filter,
                limit,
                scan_limit,
                dry_run,
                no_resume,
                checkpoint_interval,
            } => {
                import::cmd_import(
                    &settings,
                    &files,
                    source.as_deref(),
                    filter.as_deref(),
                    limit,
                    scan_limit,
                    dry_run,
                    !no_resume,
                    checkpoint_interval,
                )
                .await
            }
            ImportCommands::Urls {
                file,
                source,
                method,
                skip_invalid,
            } => import::cmd_import_urls(&settings, &file, &source, &method, skip_invalid).await,
            ImportCommands::Stdin {
                url,
                source,
                content_type,
                filename,
            } => {
                import::cmd_import_stdin(
                    &settings,
                    &url,
                    &source,
                    content_type.as_deref(),
                    filename.as_deref(),
                )
                .await
            }
        },
        Commands::Discover { command } => match command {
            DiscoverCommands::Pattern {
                source_id,
                limit,
                dry_run,
                min_examples,
            } => {
                discover::cmd_discover_pattern(&settings, &source_id, limit, dry_run, min_examples)
                    .await
            }
            DiscoverCommands::Search {
                source_id,
                engines,
                terms,
                expand,
                template,
                limit,
                dry_run,
            } => {
                discover::cmd_discover_search(
                    &settings,
                    &source_id,
                    &engines,
                    terms.as_deref(),
                    expand,
                    template,
                    limit,
                    dry_run,
                )
                .await
            }
            DiscoverCommands::Sitemap {
                source_id,
                limit,
                dry_run,
            } => discover::cmd_discover_sitemap(&settings, &source_id, limit, dry_run).await,
            DiscoverCommands::Wayback {
                source_id,
                from,
                to,
                limit,
                dry_run,
            } => {
                discover::cmd_discover_wayback(
                    &settings,
                    &source_id,
                    from.as_deref(),
                    to.as_deref(),
                    limit,
                    dry_run,
                )
                .await
            }
            DiscoverCommands::Paths {
                source_id,
                extra_paths,
                dry_run,
            } => {
                discover::cmd_discover_paths(&settings, &source_id, extra_paths.as_deref(), dry_run)
                    .await
            }
            DiscoverCommands::All {
                source_id,
                dry_run,
                limit,
            } => discover::cmd_discover_all(&settings, &source_id, dry_run, limit).await,
        },
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
            discover::cmd_browser_test(
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
