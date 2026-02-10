//! Router configuration for the web server.

use axum::{
    routing::{get, options, post},
    Router,
};
use tower_http::cors::CorsLayer;

use super::handlers;
use super::AppState;

/// Create the main router with all routes.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Health check for container orchestration
        .route("/health", get(handlers::health))
        // Root and /browse are the unified browse page
        .route("/", get(handlers::browse_documents))
        .route("/browse", get(handlers::browse_documents))
        // Document details and file serving (HTML views)
        .route("/documents/:doc_id", get(handlers::document_detail))
        .route(
            "/documents/:doc_id/versions",
            get(handlers::document_versions),
        )
        .route("/files/*path", get(handlers::serve_file))
        // Tags (HTML views)
        .route("/tags", get(handlers::list_tags))
        .route("/tags/:tag", get(handlers::list_tag_documents))
        // Type filtering (HTML views)
        .route("/types", get(handlers::list_types))
        .route("/types/:type_name", get(handlers::list_by_type))
        // Static assets (CSS/JS)
        .route("/static/style.css", get(handlers::serve_css))
        .route("/static/timeline.js", get(handlers::serve_js))
        // ===========================================
        // JSON API Endpoints
        // ===========================================
        // Documents API - search, filter, paginate
        .route("/api/documents", get(handlers::list_documents))
        .route("/api/documents/:doc_id", get(handlers::get_document))
        .route(
            "/api/documents/:doc_id/content",
            get(handlers::get_document_content),
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
        // Versions API - document version history
        .route(
            "/api/documents/:doc_id/versions",
            get(handlers::list_versions),
        )
        .route(
            "/api/documents/:doc_id/versions/:version_id",
            get(handlers::get_version),
        )
        .route("/api/versions/hash/:hash", get(handlers::find_by_hash))
        // Annotations API - LLM-generated metadata
        .route("/api/annotations", get(handlers::list_annotations))
        .route("/api/annotations/stats", get(handlers::annotation_stats))
        .route(
            "/api/annotations/:doc_id",
            get(handlers::get_annotation).put(handlers::update_annotation),
        )
        // Scrape API - scraper control and monitoring
        .route("/api/scrapers", get(handlers::list_scrapers))
        .route("/api/scrapers/:source_id", get(handlers::get_scrape_status))
        .route("/api/scrapers/queue", get(handlers::list_queue))
        .route("/api/scrapers/retry", post(handlers::retry_failed))
        // Export API - bulk data export
        .route("/api/export/documents", get(handlers::export_documents))
        .route("/api/export/annotations", get(handlers::export_annotations))
        .route("/api/export/stats", get(handlers::export_stats))
        // Entities API - NER-extracted entity search
        .route("/api/entities/search", get(handlers::search_entities))
        .route("/api/entities/types", get(handlers::entity_types))
        .route("/api/entities/top", get(handlers::top_entities))
        .route("/api/entities/locations", get(handlers::entity_locations))
        .route(
            "/api/documents/:doc_id/entities",
            get(handlers::document_entities),
        )
        // Legacy/existing API endpoints
        .route("/api/timeline", get(handlers::timeline_aggregate))
        .route("/api/timeline/:source_id", get(handlers::timeline_source))
        .route("/api/duplicates", get(handlers::list_duplicates))
        .route("/api/tags", get(handlers::api_tags))
        .route("/api/tags/search", get(handlers::api_search_tags))
        .route("/api/status", get(handlers::api_status))
        .route("/api/status/:source_id", get(handlers::api_source_status))
        .route("/api/recent", get(handlers::api_recent_docs))
        .route("/api/types", get(handlers::api_type_stats))
        .route("/api/sources", get(handlers::api_sources))
        // OpenAPI spec
        .route("/api", options(handlers::openapi_spec))
        .route("/api/openapi.json", get(handlers::openapi_spec))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
