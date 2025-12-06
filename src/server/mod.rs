//! Web server for browsing FOIA documents.
//!
//! Provides a directory-style listing of scraped documents with:
//! - Source-level grouping (each scraper is a "folder")
//! - Timeline visualization with date range filtering
//! - Cross-source deduplication display
//! - Document version history

mod cache;
mod handlers;
mod routes;
mod templates;

pub use routes::create_router;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::Settings;
use crate::repository::{CrawlRepository, DocumentRepository, SourceRepository};

use cache::StatsCache;

/// Status of a DeepSeek OCR job.
#[derive(Clone, Debug, Default)]
pub struct DeepSeekJobStatus {
    /// Document being processed (None if no job running).
    pub document_id: Option<String>,
    /// Number of pages processed so far.
    pub pages_processed: u32,
    /// Total pages to process.
    pub total_pages: u32,
    /// Error message if job failed.
    pub error: Option<String>,
    /// Whether the job is complete.
    pub completed: bool,
}

/// Shared state for the web server.
#[derive(Clone)]
pub struct AppState {
    pub doc_repo: Arc<DocumentRepository>,
    pub source_repo: Arc<SourceRepository>,
    pub crawl_repo: Arc<CrawlRepository>,
    pub documents_dir: PathBuf,
    pub stats_cache: Arc<StatsCache>,
    /// DeepSeek OCR job status (only one can run at a time).
    pub deepseek_job: Arc<RwLock<DeepSeekJobStatus>>,
}

impl AppState {
    pub fn new(settings: &Settings) -> anyhow::Result<Self> {
        let db_path = settings.database_path();
        let doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;
        let source_repo = SourceRepository::new(&db_path)?;
        let crawl_repo = CrawlRepository::new(&db_path)?;

        Ok(Self {
            doc_repo: Arc::new(doc_repo),
            source_repo: Arc::new(source_repo),
            crawl_repo: Arc::new(crawl_repo),
            documents_dir: settings.documents_dir.clone(),
            stats_cache: Arc::new(StatsCache::new()),
            deepseek_job: Arc::new(RwLock::new(DeepSeekJobStatus::default())),
        })
    }
}

/// Start the web server.
pub async fn serve(settings: &Settings, host: &str, port: u16) -> anyhow::Result<()> {
    let state = AppState::new(settings)?;
    let app = create_router(state);

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;
    tracing::info!("Starting server at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
