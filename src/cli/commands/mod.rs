//! CLI commands implementation.
//!
//! This module contains the CLI parser and dispatches to command-specific modules.

mod annotate;
mod config_cmd;
mod discover;
mod documents;
mod helpers;
mod import;
mod init;
mod llm;
mod ocr;
mod scrape;
mod serve;
mod source;
mod state;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::config::{load_settings_with_options, LoadOptions};

// Re-export ReloadMode for use by other modules
pub use scrape::ReloadMode;

#[derive(Parser)]
#[command(name = "foiacquire")]
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
        /// Config reload behavior in daemon mode
        #[arg(short = 'r', long, value_enum, default_value = "next-run")]
        reload: ReloadMode,
    },

    /// Show system status
    Status,

    /// Process documents with OCR and extract text
    Ocr {
        /// Source ID (optional, processes all sources if not specified)
        source_id: Option<String>,
        /// Specific document ID to process
        #[arg(long)]
        doc_id: Option<String>,
        /// Number of workers (default: 2)
        #[arg(short, long, default_value = "2")]
        workers: usize,
        /// Limit number of documents to process per cycle (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        limit: usize,
        /// Extract URLs from documents and add to crawl queue
        #[arg(long)]
        extract_urls: bool,
        /// Run continuously, checking for new work
        #[arg(long)]
        daemon: bool,
        /// Seconds to wait between checks in daemon mode (default: 60)
        #[arg(long, default_value = "60")]
        interval: u64,
        /// Config reload behavior in daemon mode
        #[arg(short = 'r', long, value_enum, default_value = "next-run")]
        reload: ReloadMode,
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
        /// Config reload behavior in daemon mode
        #[arg(short = 'r', long, value_enum, default_value = "next-run")]
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
        /// Disable resume support - ignore progress files and start from beginning
        #[arg(long)]
        no_resume: bool,
        /// How often to save progress (in records). Set to 0 to disable checkpointing.
        #[arg(long, default_value = "10000")]
        checkpoint_interval: usize,
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

/// Run the CLI.
pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let options = LoadOptions {
        config_path: cli.config,
        use_cwd: cli.cwd,
        target: cli.target,
    };
    let (settings, _config) = load_settings_with_options(options).await;

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
        } => scrape::cmd_download(&settings, source_id.as_deref(), workers, limit, progress).await,
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
        Commands::Scrape {
            source_ids,
            all,
            workers,
            limit,
            progress,
            daemon,
            interval,
            reload,
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
            )
            .await
        }
        Commands::Status => scrape::cmd_status(&settings).await,
        Commands::Ocr {
            source_id,
            doc_id,
            workers,
            limit,
            daemon,
            interval,
            reload,
            ..
        } => {
            ocr::cmd_ocr(
                &settings,
                source_id.as_deref(),
                doc_id.as_deref(),
                workers,
                limit,
                daemon,
                interval,
                reload,
            )
            .await
        }
        Commands::OcrCheck => ocr::cmd_ocr_check().await,
        Commands::OcrCompare {
            file,
            pages,
            backends,
            deepseek_path,
        } => ocr::cmd_ocr_compare(&file, pages.as_deref(), &backends, deepseek_path).await,
        Commands::Serve { bind } => serve::cmd_serve(&settings, &bind).await,
        Commands::Refresh {
            source_id,
            workers,
            limit,
            force,
        } => scrape::cmd_refresh(&settings, source_id.as_deref(), workers, limit, force).await,
        Commands::Annotate {
            source_id,
            doc_id,
            limit,
            endpoint,
            model,
            daemon,
            interval,
            reload,
        } => {
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
        Commands::Import {
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
        Commands::Discover {
            source_id,
            limit,
            dry_run,
            min_examples,
        } => discover::cmd_discover(&settings, &source_id, limit, dry_run, min_examples).await,
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
