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
mod template_structs;
mod templates;

pub use routes::create_router;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::Settings;
use crate::repository::{DieselCrawlRepository, DieselDocumentRepository, DieselSourceRepository};

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
    pub doc_repo: Arc<DieselDocumentRepository>,
    pub source_repo: Arc<DieselSourceRepository>,
    pub crawl_repo: Arc<DieselCrawlRepository>,
    pub documents_dir: PathBuf,
    pub stats_cache: Arc<StatsCache>,
    /// DeepSeek OCR job status (only one can run at a time).
    pub deepseek_job: Arc<RwLock<DeepSeekJobStatus>>,
}

impl AppState {
    pub async fn new(settings: &Settings) -> anyhow::Result<Self> {
        let ctx = settings.create_db_context()?;

        Ok(Self {
            doc_repo: Arc::new(ctx.documents()),
            source_repo: Arc::new(ctx.sources()),
            crawl_repo: Arc::new(ctx.crawl()),
            documents_dir: settings.documents_dir.clone(),
            stats_cache: Arc::new(StatsCache::new()),
            deepseek_job: Arc::new(RwLock::new(DeepSeekJobStatus::default())),
        })
    }
}

/// Start the web server.
pub async fn serve(settings: &Settings, host: &str, port: u16) -> anyhow::Result<()> {
    let state = AppState::new(settings).await?;
    let app = create_router(state);

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;
    tracing::info!("Starting server at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::models::{Document, DocumentStatus, Source, SourceType};
    use crate::repository::diesel_context::DieselDbContext;

    async fn setup_test_app() -> (axum::Router, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let docs_dir = dir.path().join("docs");
        std::fs::create_dir_all(&docs_dir).unwrap();

        let ctx = DieselDbContext::from_sqlite_path(&db_path, &docs_dir).unwrap();
        ctx.init_schema().await.unwrap();

        let state = AppState {
            doc_repo: Arc::new(ctx.documents()),
            source_repo: Arc::new(ctx.sources()),
            crawl_repo: Arc::new(ctx.crawl()),
            documents_dir: docs_dir,
            stats_cache: Arc::new(StatsCache::new()),
            deepseek_job: Arc::new(RwLock::new(DeepSeekJobStatus::default())),
        };

        let app = create_router(state);
        (app, dir)
    }

    async fn setup_test_app_with_data() -> (axum::Router, tempfile::TempDir) {
        use crate::models::DocumentVersion;
        use std::path::PathBuf;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let docs_dir = dir.path().join("docs");
        std::fs::create_dir_all(&docs_dir).unwrap();

        let ctx = DieselDbContext::from_sqlite_path(&db_path, &docs_dir).unwrap();
        ctx.init_schema().await.unwrap();

        // Add test data
        let source = Source::new(
            "test-source".to_string(),
            SourceType::Custom,
            "Test Source".to_string(),
            "https://example.com".to_string(),
        );
        ctx.sources().save(&source).await.unwrap();

        // Create a test document with version
        let test_content = b"test document content";
        let version = DocumentVersion::new(
            test_content,
            PathBuf::from("test/doc.pdf"),
            "application/pdf".to_string(),
            Some("https://example.com/doc.pdf".to_string()),
        );

        let mut doc = Document::new(
            uuid::Uuid::new_v4().to_string(),
            "test-source".to_string(),
            "Test Document".to_string(),
            "https://example.com/doc.pdf".to_string(),
            version,
            serde_json::json!({}),
        );
        doc.status = DocumentStatus::OcrComplete;
        doc.synopsis = Some("This is a test document synopsis.".to_string());
        doc.tags = vec![
            "test".to_string(),
            "example".to_string(),
            "foia".to_string(),
        ];
        ctx.documents().save(&doc).await.unwrap();

        let state = AppState {
            doc_repo: Arc::new(ctx.documents()),
            source_repo: Arc::new(ctx.sources()),
            crawl_repo: Arc::new(ctx.crawl()),
            documents_dir: docs_dir,
            stats_cache: Arc::new(StatsCache::new()),
            deepseek_job: Arc::new(RwLock::new(DeepSeekJobStatus::default())),
        };

        let app = create_router(state);
        (app, dir)
    }

    #[tokio::test]
    async fn test_api_sources_empty() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_api_sources_with_data() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/sources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 1);
        assert_eq!(json[0]["id"], "test-source");
        assert_eq!(json[0]["name"], "Test Source");
    }

    #[tokio::test]
    async fn test_api_status() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["documents"]["total"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn test_api_recent_docs() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/recent?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn test_api_types() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/types")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn test_api_tags() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tags")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_api_tags_search() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/tags/search?q=test&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn test_api_timeline() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/timeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_api_duplicates() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/duplicates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_browse_root() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("<!DOCTYPE html>") || html.contains("<html"));
    }

    #[tokio::test]
    async fn test_browse_page() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/browse")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_browse_with_filters() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/browse?source=test-source&status=ocr_complete")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_static_css() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/static/style.css")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap_or(""));
        assert!(content_type.unwrap_or("").contains("css"));
    }

    #[tokio::test]
    async fn test_static_js() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/static/timeline.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tags_list() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(Request::builder().uri("/tags").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_types_list() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/types")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_document_not_found() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/documents/nonexistent-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Handler returns 200 with "Not Found" HTML page
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Not Found") || html.contains("not found"));
    }

    #[tokio::test]
    async fn test_api_source_status() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status/test-source")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["source_id"], "test-source");
    }

    #[tokio::test]
    async fn test_api_reocr_status() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/documents/reocr/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // When idle, status is "idle" and document_id is empty string
        assert!(json["status"] == "idle" || json["status"] == "complete");
    }
}
