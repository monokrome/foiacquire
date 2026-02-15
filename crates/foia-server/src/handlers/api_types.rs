//! Typed API response types for OpenAPI spec generation.
//!
//! Replaces inline `serde_json::json!()` usage with proper structs that
//! derive `ToSchema` for utoipa.

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use utoipa::ToSchema;

/// Standard API response envelope.
///
/// Every endpoint returns this wrapper:
/// ```json
/// { "error": false, "context": {}, "data": { ... } }
/// ```
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiResponse<C: Serialize, T: Serialize> {
    pub error: bool,
    pub context: C,
    pub data: T,
}

/// Empty context for non-paginated responses.
#[derive(Debug, Default, Serialize, ToSchema)]
pub struct EmptyContext {}

/// Pagination context metadata.
#[derive(Debug, Serialize, ToSchema)]
pub struct PaginationContext {
    pub page: usize,
    pub per_page: usize,
    pub total: u64,
    pub total_pages: u64,
}

/// Error payload inside the envelope.
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorData {
    pub message: String,
}

impl ApiResponse<EmptyContext, ErrorData> {
    pub fn error(status: StatusCode, message: impl Into<String>) -> impl IntoResponse {
        (
            status,
            Json(ApiResponse {
                error: true,
                context: EmptyContext {},
                data: ErrorData {
                    message: message.into(),
                },
            }),
        )
    }
}

impl<T: Serialize> ApiResponse<EmptyContext, T> {
    pub fn ok(data: T) -> Json<ApiResponse<EmptyContext, T>> {
        Json(ApiResponse {
            error: false,
            context: EmptyContext {},
            data,
        })
    }
}

impl<T: Serialize> ApiResponse<PaginationContext, T> {
    #[allow(dead_code)]
    pub fn paginated(
        page: usize,
        per_page: usize,
        total: u64,
        data: T,
    ) -> Json<ApiResponse<PaginationContext, T>> {
        let total_pages = total.div_ceil(per_page as u64);
        Json(ApiResponse {
            error: false,
            context: PaginationContext {
                page,
                per_page,
                total,
                total_pages,
            },
            data,
        })
    }
}

// --- Typed response structs for endpoints that used inline JSON ---

/// Source info returned by `GET /api/sources`.
#[derive(Debug, Serialize, ToSchema)]
pub struct SourceInfo {
    pub id: String,
    pub name: String,
    pub count: u64,
}

/// Category stat returned by `GET /api/types`.
#[derive(Debug, Serialize, ToSchema)]
pub struct CategoryStat {
    pub category: String,
    pub name: String,
    pub count: u64,
}

/// Tag with count returned by `GET /api/tags` and `GET /api/tags/search`.
#[derive(Debug, Serialize, ToSchema)]
pub struct TagCount {
    pub tag: String,
    pub count: usize,
}

/// Recent document returned by `GET /api/recent`.
#[derive(Debug, Serialize, ToSchema)]
pub struct RecentDocument {
    pub id: String,
    pub title: String,
    pub source_id: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub status: String,
    pub updated_at: String,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
}

/// MIME type stat for status endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub struct MimeTypeStat {
    pub mime_type: String,
    pub count: u64,
}

/// Recent URL entry in status responses.
#[derive(Debug, Serialize, ToSchema)]
pub struct RecentUrl {
    pub url: String,
    pub source_id: Option<String>,
    pub fetched_at: Option<String>,
    pub document_id: Option<String>,
}

/// Failed URL entry in status responses.
#[derive(Debug, Serialize, ToSchema)]
pub struct FailedUrl {
    pub url: String,
    pub source_id: Option<String>,
    pub error: Option<String>,
    pub retry_count: u32,
}

/// Per-source crawl stats in the overall status response.
#[derive(Debug, Serialize, ToSchema)]
pub struct SourceCrawlStat {
    pub source_id: String,
    pub discovered: u64,
    pub fetched: u64,
    pub pending: u64,
    pub failed: u64,
    pub has_pending: bool,
}

/// Document stats block for status endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub struct DocumentStats {
    pub total: u64,
    pub needing_ocr: u64,
    pub needing_summarization: u64,
}

/// Crawl stats block for status endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub struct CrawlStats {
    pub total_discovered: u64,
    pub total_pending: u64,
    pub total_failed: u64,
    pub sources: Vec<SourceCrawlStat>,
}

/// Overall status returned by `GET /api/status`.
#[derive(Debug, Serialize, ToSchema)]
pub struct StatusResponse {
    pub documents: DocumentStats,
    pub crawl: CrawlStats,
    pub recent_downloads: Vec<RecentUrl>,
    pub recent_failures: Vec<FailedUrl>,
    pub type_stats: Vec<MimeTypeStat>,
}

/// Per-source crawl state detail.
#[derive(Debug, Serialize, ToSchema)]
pub struct CrawlState {
    pub discovered: u64,
    pub fetched: u64,
    pub pending: u64,
    pub failed: u64,
    pub has_pending: bool,
    pub last_crawl_started: Option<String>,
    pub last_crawl_completed: Option<String>,
}

/// Per-source request stats detail.
#[derive(Debug, Serialize, ToSchema)]
pub struct RequestStats {
    pub total_requests: u64,
    pub success_200: u64,
    pub not_modified_304: u64,
    pub errors: u64,
    pub avg_duration_ms: u64,
    pub total_bytes: u64,
}

/// Source-specific status returned by `GET /api/status/:source_id`.
#[derive(Debug, Serialize, ToSchema)]
pub struct SourceStatusResponse {
    pub source_id: String,
    pub documents: DocumentStats,
    pub crawl: Option<CrawlState>,
    pub request_stats: Option<RequestStats>,
    pub recent_downloads: Vec<RecentUrl>,
    pub recent_failures: Vec<FailedUrl>,
    pub type_stats: Vec<MimeTypeStat>,
}

/// Scraper info returned by `GET /api/scrapers`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ScraperInfo {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub base_url: String,
    pub last_scraped: Option<String>,
    pub document_count: u64,
    pub crawl_stats: Option<ScraperCrawlStats>,
}

/// Crawl stats within a scraper info entry.
#[derive(Debug, Serialize, ToSchema)]
pub struct ScraperCrawlStats {
    pub urls_discovered: u64,
    pub urls_fetched: u64,
    pub urls_pending: u64,
    pub urls_failed: u64,
    pub has_pending: bool,
}

/// Full scraper status returned by `GET /api/scrapers/:source_id`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ScraperStatusResponse {
    pub source_id: String,
    pub name: String,
    pub last_scraped: Option<String>,
    pub crawl_state: Option<CrawlState>,
    pub request_stats: Option<RequestStats>,
    pub recent_downloads: Vec<RecentUrl>,
    pub failed_urls: Vec<FailedUrl>,
}

/// Queue item returned by `GET /api/scrapers/queue`.
#[derive(Debug, Serialize, ToSchema)]
pub struct QueueItem {
    pub url: String,
    pub source_id: String,
    pub status: String,
    pub discovery_method: String,
    pub discovered_at: String,
    pub retry_count: u32,
    pub depth: u32,
}

/// Queue listing response.
#[derive(Debug, Serialize, ToSchema)]
pub struct QueueResponse {
    pub items: Vec<QueueItem>,
    pub per_page: usize,
}

/// Retry response from `POST /api/scrapers/retry`.
#[derive(Debug, Serialize, ToSchema)]
pub struct RetryResponse {
    pub reset_count: u64,
    pub message: String,
}

/// Versions listing response from `GET /api/documents/:id/versions`.
#[derive(Debug, Serialize, ToSchema)]
pub struct VersionsListResponse {
    pub document_id: String,
    pub version_count: usize,
    pub versions: Vec<super::versions_api::VersionResponse>,
}

/// Hash search response from `GET /api/versions/hash/:hash`.
#[derive(Debug, Serialize, ToSchema)]
pub struct HashSearchResponse {
    pub hash: String,
    pub sources: Vec<(String, String, String)>,
}

/// Annotations listing response.
#[derive(Debug, Serialize, ToSchema)]
pub struct AnnotationsListResponse {
    pub items: Vec<super::annotations_api::AnnotationResponse>,
    pub page: usize,
    pub per_page: usize,
    pub stats: AnnotationListStats,
}

/// Stats returned within annotations listing.
#[derive(Debug, Serialize, ToSchema)]
pub struct AnnotationListStats {
    pub annotated: u64,
    pub needing_annotation: u64,
}

/// Update annotation response.
#[derive(Debug, Serialize, ToSchema)]
pub struct UpdateAnnotationResponse {
    pub document_id: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub updated: bool,
}

/// Export stats response from `GET /api/export/stats`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ExportStatsResponse {
    pub total_documents: u64,
    pub by_type: std::collections::HashMap<String, u64>,
    pub by_source: std::collections::HashMap<String, u64>,
    pub by_status: std::collections::HashMap<String, u64>,
}

/// Annotation export record.
#[derive(Debug, Serialize, ToSchema)]
pub struct AnnotationExport {
    pub id: String,
    pub source_url: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
}
