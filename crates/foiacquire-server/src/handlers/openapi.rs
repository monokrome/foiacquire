//! OpenAPI spec generation and serving.

use axum::{http::StatusCode, response::IntoResponse};
use utoipa::OpenApi;

use super::annotations_api;
use super::api;
use super::api_types;
use super::documents_api;
use super::entities_api;
use super::export_api;
use super::helpers;
use super::ocr;
use super::pages;
use super::scrape_api;
use super::tags;
use super::timeline;
use super::versions_api;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "FOIAcquire API",
        description = "FOIA document acquisition and research system",
        version = "0.12.0"
    ),
    paths(
        // Health
        api::health,
        // Documents
        documents_api::list_documents,
        documents_api::get_document,
        documents_api::get_document_content,
        // Pages
        pages::api_document_pages,
        // OCR
        ocr::api_reocr_document,
        ocr::api_reocr_status,
        // Versions
        versions_api::list_versions,
        versions_api::get_version,
        versions_api::find_by_hash,
        // Annotations
        annotations_api::list_annotations,
        annotations_api::get_annotation,
        annotations_api::update_annotation,
        annotations_api::annotation_stats,
        // Scrapers
        scrape_api::list_scrapers,
        scrape_api::get_scrape_status,
        scrape_api::list_queue,
        scrape_api::retry_failed,
        // Export
        export_api::export_documents,
        export_api::export_annotations,
        export_api::export_stats,
        // Entities
        entities_api::search_entities,
        entities_api::entity_types,
        entities_api::top_entities,
        entities_api::entity_locations,
        entities_api::document_entities,
        // Timeline
        timeline::timeline_aggregate,
        timeline::timeline_source,
        // Status / Tags / Sources
        api::api_sources,
        api::api_status,
        api::api_source_status,
        api::api_recent_docs,
        api::api_type_stats,
        api::api_search_tags,
        tags::api_tags,
    ),
    components(schemas(
        // Envelope types
        api_types::EmptyContext,
        api_types::PaginationContext,
        api_types::ErrorData,
        // Helper types
        helpers::VersionSummary,
        helpers::DocumentSummary,
        helpers::TimelineResponse,
        helpers::TimelineBucket,
        helpers::VersionInfo,
        // Document API types
        documents_api::DocumentContentResponse,
        documents_api::PageContent,
        // Version API types
        versions_api::VersionResponse,
        api_types::VersionsListResponse,
        api_types::HashSearchResponse,
        // Annotation API types
        annotations_api::AnnotationResponse,
        annotations_api::UpdateAnnotationRequest,
        annotations_api::AnnotationStats,
        annotations_api::SourceAnnotationStats,
        api_types::AnnotationsListResponse,
        api_types::AnnotationListStats,
        api_types::UpdateAnnotationResponse,
        // Scraper API types
        scrape_api::RetryRequest,
        api_types::ScraperInfo,
        api_types::ScraperCrawlStats,
        api_types::ScraperStatusResponse,
        api_types::CrawlState,
        api_types::RequestStats,
        api_types::QueueItem,
        api_types::QueueResponse,
        api_types::RetryResponse,
        api_types::RecentUrl,
        api_types::FailedUrl,
        // Export API types
        export_api::ExportFormat,
        export_api::ExportDocument,
        api_types::ExportStatsResponse,
        api_types::AnnotationExport,
        // Entity API types
        entities_api::MatchedEntity,
        entities_api::EntitySearchResult,
        entities_api::EntityTypeStats,
        entities_api::TopEntity,
        entities_api::GeocodedLocation,
        // OCR types
        ocr::ReOcrRequest,
        ocr::ReOcrResponse,
        // Page types
        pages::PageData,
        pages::PagesResponse,
        // Status types
        api_types::SourceInfo,
        api_types::CategoryStat,
        api_types::TagCount,
        api_types::RecentDocument,
        api_types::MimeTypeStat,
        api_types::StatusResponse,
        api_types::DocumentStats,
        api_types::CrawlStats,
        api_types::SourceCrawlStat,
        api_types::SourceStatusResponse,
    )),
    tags(
        (name = "Health", description = "Health check"),
        (name = "Documents", description = "Document search, filter, and details"),
        (name = "Versions", description = "Document version history"),
        (name = "Pages", description = "Document page content and OCR"),
        (name = "OCR", description = "Re-OCR document processing"),
        (name = "Annotations", description = "LLM-generated metadata and tags"),
        (name = "Scrapers", description = "Scraper control and monitoring"),
        (name = "Export", description = "Bulk data export"),
        (name = "Entities", description = "NER-extracted entity search"),
        (name = "Timeline", description = "Document timeline visualization"),
        (name = "Status", description = "System status, sources, types, and tags"),
    )
)]
struct ApiDoc;

/// Serve the OpenAPI spec as JSON.
pub async fn openapi_spec() -> impl IntoResponse {
    let spec = ApiDoc::openapi()
        .to_json()
        .unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e));
    (StatusCode::OK, [("content-type", "application/json")], spec)
}
