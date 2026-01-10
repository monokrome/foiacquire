//! Web server for browsing FOIA documents.
//!
//! Provides a directory-style listing of scraped documents with:
//! - Source-level grouping (each scraper is a "folder")
//! - Timeline visualization with date range filtering
//! - Cross-source deduplication display
//! - Document version history

mod assets;
mod cache;
mod handlers;
mod routes;
mod template_structs;

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
    use crate::repository::migrations;

    async fn setup_test_app() -> (axum::Router, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let docs_dir = dir.path().join("docs");
        std::fs::create_dir_all(&docs_dir).unwrap();

        let db_url = format!("sqlite:{}", db_path.display());
        migrations::run_migrations(&db_url).await.unwrap();
        let ctx = DieselDbContext::from_sqlite_path(&db_path).unwrap();

        let state = AppState {
            doc_repo: Arc::new(ctx.documents()),
            source_repo: Arc::new(ctx.sources()),
            crawl_repo: Arc::new(ctx.crawl()),
            documents_dir: docs_dir.clone(),
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

        let db_url = format!("sqlite:{}", db_path.display());
        migrations::run_migrations(&db_url).await.unwrap();
        let ctx = DieselDbContext::from_sqlite_path(&db_path).unwrap();

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
        let (app, _dir) = setup_test_app_with_data().await;

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

        // Verify response structure includes category, name, and count
        let items = json.as_array().unwrap();
        assert!(!items.is_empty(), "Should have at least one category");
        for item in items {
            assert!(item.get("category").is_some(), "Missing 'category' field");
            assert!(item.get("name").is_some(), "Missing 'name' field");
            assert!(item.get("count").is_some(), "Missing 'count' field");
        }

        // The test data has a PDF, which should be categorized as "documents"
        let has_documents = items
            .iter()
            .any(|i| i.get("category").and_then(|c| c.as_str()) == Some("documents"));
        assert!(
            has_documents,
            "Should have 'documents' category from test PDF"
        );
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

    // ==========================================
    // New API endpoint tests (v0.8.0)
    // ==========================================

    #[tokio::test]
    async fn test_api_documents_list() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/documents")
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

        // Check paginated response structure
        assert!(json["items"].is_array());
        assert!(json["page"].is_number());
        assert!(json["per_page"].is_number());
        assert!(json["total"].is_number());
        assert!(json["total_pages"].is_number());
    }

    #[tokio::test]
    async fn test_api_documents_list_with_filters() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/documents?source=test-source&per_page=10&page=1")
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

        assert_eq!(json["per_page"], 10);
        assert_eq!(json["page"], 1);
    }

    #[tokio::test]
    async fn test_api_documents_get() {
        let (app, _dir) = setup_test_app_with_data().await;

        // First get a document ID from the list
        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/documents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let doc_id = json["items"][0]["id"].as_str().unwrap();

        // Now get that specific document
        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/documents/{}", doc_id))
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

        assert_eq!(json["id"], doc_id);
        assert!(json["source_id"].is_string());
        assert!(json["title"].is_string());
        assert!(json["status"].is_string());
    }

    #[tokio::test]
    async fn test_api_documents_get_not_found() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/documents/nonexistent-doc-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_api_documents_versions() {
        let (app, _dir) = setup_test_app_with_data().await;

        // Get a document ID first
        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/documents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let doc_id = json["items"][0]["id"].as_str().unwrap();

        // Get versions
        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/documents/{}/versions", doc_id))
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

        assert_eq!(json["document_id"], doc_id);
        assert!(json["versions"].is_array());
        assert!(json["version_count"].is_number());
    }

    #[tokio::test]
    async fn test_api_annotations_list() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/annotations")
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

        assert!(json["items"].is_array());
        assert!(json["stats"]["annotated"].is_number());
        assert!(json["stats"]["needing_annotation"].is_number());
    }

    #[tokio::test]
    async fn test_api_annotations_stats() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/annotations/stats")
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

        assert!(json["total_documents"].is_number());
        assert!(json["annotated"].is_number());
        assert!(json["needing_annotation"].is_number());
        assert!(json["by_source"].is_array());
    }

    #[tokio::test]
    async fn test_api_scrapers_list() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/scrapers")
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
        // Should have at least the test source
        let arr = json.as_array().unwrap();
        assert!(!arr.is_empty());
        assert!(arr[0]["id"].is_string());
        assert!(arr[0]["name"].is_string());
    }

    #[tokio::test]
    async fn test_api_scrapers_status() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/scrapers/test-source")
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
        assert!(json["name"].is_string());
    }

    #[tokio::test]
    async fn test_api_scrapers_queue() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/scrapers/queue?source=test-source")
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

        assert!(json["items"].is_array());
        assert!(json["per_page"].is_number());
    }

    #[tokio::test]
    async fn test_api_export_documents_json() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/export/documents?format=json")
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
        assert!(content_type.unwrap_or("").contains("json"));

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn test_api_export_documents_csv() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/export/documents?format=csv")
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
        assert!(content_type.unwrap_or("").contains("csv"));
    }

    #[tokio::test]
    async fn test_api_export_stats() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/export/stats")
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

        assert!(json["total_documents"].is_number());
        assert!(json["by_type"].is_object());
        assert!(json["by_source"].is_object());
        assert!(json["by_status"].is_object());
    }

    #[tokio::test]
    async fn test_api_export_annotations() {
        let (app, _dir) = setup_test_app_with_data().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/export/annotations")
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
        assert!(content_type.unwrap_or("").contains("json"));
    }

    #[tokio::test]
    async fn test_api_versions_hash_not_found() {
        let (app, _dir) = setup_test_app().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/versions/hash/0000000000000000000000000000000000000000000000000000000000000000")
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

        assert!(json["sources"].is_array());
        assert!(json["sources"].as_array().unwrap().is_empty());
    }
}
