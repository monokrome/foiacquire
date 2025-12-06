//! Router configuration for the web server.

use axum::{routing::{get, post}, Router};
use tower_http::cors::CorsLayer;

use super::handlers;
use super::AppState;

/// Create the main router with all routes.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Root and /browse are the unified browse page
        .route("/", get(handlers::browse_documents))
        .route("/browse", get(handlers::browse_documents))
        // Document details and file serving
        .route("/documents/:doc_id", get(handlers::document_detail))
        .route(
            "/documents/:doc_id/versions",
            get(handlers::document_versions),
        )
        .route(
            "/api/documents/:doc_id/pages",
            get(handlers::api_document_pages),
        )
        .route(
            "/api/documents/:doc_id/reocr",
            post(handlers::api_reocr_document),
        )
        .route(
            "/api/documents/reocr/status",
            get(handlers::api_reocr_status),
        )
        .route("/files/*path", get(handlers::serve_file))
        // Tags
        .route("/tags", get(handlers::list_tags))
        .route("/tags/:tag", get(handlers::list_tag_documents))
        // Timeline API
        .route("/api/timeline", get(handlers::timeline_aggregate))
        .route("/api/timeline/:source_id", get(handlers::timeline_source))
        .route("/api/duplicates", get(handlers::list_duplicates))
        .route("/api/tags", get(handlers::api_tags))
        // Status/State API
        .route("/api/status", get(handlers::api_status))
        .route("/api/status/:source_id", get(handlers::api_source_status))
        .route("/api/recent", get(handlers::api_recent_docs))
        .route("/api/types", get(handlers::api_type_stats))
        .route("/api/sources", get(handlers::api_sources))
        // Type filtering endpoints
        .route("/types", get(handlers::list_types))
        .route("/types/:type_name", get(handlers::list_by_type))
        .route("/api/tags/search", get(handlers::api_search_tags))
        // Static assets (CSS/JS)
        .route("/static/style.css", get(handlers::serve_css))
        .route("/static/timeline.js", get(handlers::serve_js))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
