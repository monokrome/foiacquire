//! Scrape control API endpoints.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

use super::super::AppState;
use super::api_types::{
    ApiResponse, CrawlState, FailedUrl, QueueItem, QueueResponse, RecentUrl, RequestStats,
    RetryResponse, ScraperCrawlStats, ScraperInfo, ScraperStatusResponse,
};
use super::helpers::{internal_error, not_found};

/// List all scrapers/sources with their configuration.
#[utoipa::path(
    get,
    path = "/api/scrapers",
    responses(
        (status = 200, description = "List of scrapers", body = Vec<ScraperInfo>)
    ),
    tag = "Scrapers"
)]
pub async fn list_scrapers(State(state): State<AppState>) -> impl IntoResponse {
    let sources = state.source_repo.get_all().await.unwrap_or_default();
    let source_counts = state
        .doc_repo
        .get_all_source_counts()
        .await
        .unwrap_or_default();
    let crawl_stats = state.crawl_repo.get_all_stats().await.unwrap_or_default();

    let scrapers: Vec<ScraperInfo> = sources
        .into_iter()
        .map(|s| {
            let count = source_counts.get(&s.id).copied().unwrap_or(0);
            let stats = crawl_stats.get(&s.id);
            ScraperInfo {
                id: s.id,
                name: s.name,
                source_type: format!("{:?}", s.source_type),
                base_url: s.base_url,
                last_scraped: s.last_scraped.map(|d| d.to_rfc3339()),
                document_count: count,
                crawl_stats: stats.map(|st| ScraperCrawlStats {
                    urls_discovered: st.urls_discovered,
                    urls_fetched: st.urls_fetched,
                    urls_pending: st.urls_pending,
                    urls_failed: st.urls_failed,
                    has_pending: st.crawl_state.has_pending_urls,
                }),
            }
        })
        .collect();

    ApiResponse::ok(scrapers).into_response()
}

/// Get scrape status for a specific source.
#[utoipa::path(
    get,
    path = "/api/scrapers/{source_id}",
    params(("source_id" = String, Path, description = "Source ID")),
    responses(
        (status = 200, description = "Scraper status", body = ScraperStatusResponse),
        (status = 404, description = "Source not found")
    ),
    tag = "Scrapers"
)]
pub async fn get_scrape_status(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    let source = match state.source_repo.get(&source_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return not_found("Source not found").into_response(),
        Err(e) => return internal_error(e).into_response(),
    };

    let crawl_state = state.crawl_repo.get_crawl_state(&source_id).await.ok();
    let request_stats = state.crawl_repo.get_request_stats(&source_id).await.ok();

    let recent_downloads: Vec<RecentUrl> = state
        .crawl_repo
        .get_recent_downloads(Some(&source_id), 10)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|u| RecentUrl {
            url: u.url,
            source_id: None,
            fetched_at: u.fetched_at.map(|d| d.to_rfc3339()),
            document_id: u.document_id,
        })
        .collect();

    let failed_urls: Vec<FailedUrl> = state
        .crawl_repo
        .get_failed_urls(Some(&source_id), 10)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|u| FailedUrl {
            url: u.url,
            source_id: None,
            error: u.last_error,
            retry_count: u.retry_count,
        })
        .collect();

    ApiResponse::ok(ScraperStatusResponse {
        source_id,
        name: source.name,
        last_scraped: source.last_scraped.map(|d| d.to_rfc3339()),
        crawl_state: crawl_state.map(|s| CrawlState {
            discovered: s.urls_discovered,
            fetched: s.urls_fetched,
            pending: s.urls_pending,
            failed: s.urls_failed,
            has_pending: s.has_pending_urls,
            last_crawl_started: s.last_crawl_started,
            last_crawl_completed: s.last_crawl_completed,
        }),
        request_stats: request_stats.map(|s| RequestStats {
            total_requests: s.total_requests,
            success_200: s.success_200,
            not_modified_304: s.not_modified_304,
            errors: s.errors,
            avg_duration_ms: s.avg_duration_ms,
            total_bytes: s.total_bytes,
        }),
        recent_downloads,
        failed_urls,
    })
    .into_response()
}

/// Query for scrape queue.
#[derive(Debug, Deserialize, IntoParams)]
pub struct QueueQuery {
    pub source: Option<String>,
    pub per_page: Option<usize>,
}

/// List URLs in the scrape queue.
#[utoipa::path(
    get,
    path = "/api/scrapers/queue",
    params(QueueQuery),
    responses(
        (status = 200, description = "Queue listing", body = QueueResponse)
    ),
    tag = "Scrapers"
)]
pub async fn list_queue(
    State(state): State<AppState>,
    Query(params): Query<QueueQuery>,
) -> impl IntoResponse {
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);

    let pending = if let Some(source_id) = &params.source {
        state
            .crawl_repo
            .get_pending_urls(source_id, per_page as u32)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let items: Vec<QueueItem> = pending
        .into_iter()
        .map(|u| QueueItem {
            url: u.url,
            source_id: u.source_id,
            status: format!("{:?}", u.status),
            discovery_method: format!("{:?}", u.discovery_method),
            discovered_at: u.discovered_at.to_rfc3339(),
            retry_count: u.retry_count,
            depth: u.depth,
        })
        .collect();

    ApiResponse::ok(QueueResponse { items, per_page }).into_response()
}

/// Clear failed URLs for retry.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RetryRequest {
    pub source: Option<String>,
}

/// Reset failed URLs for retry.
#[utoipa::path(
    post,
    path = "/api/scrapers/retry",
    request_body = RetryRequest,
    responses(
        (status = 200, description = "Reset result", body = RetryResponse)
    ),
    tag = "Scrapers"
)]
pub async fn retry_failed(
    State(state): State<AppState>,
    Json(body): Json<RetryRequest>,
) -> impl IntoResponse {
    let result = state
        .crawl_repo
        .reset_failed_urls(body.source.as_deref())
        .await;

    match result {
        Ok(count) => ApiResponse::ok(RetryResponse {
            reset_count: count,
            message: format!("Reset {} failed URLs for retry", count),
        })
        .into_response(),
        Err(e) => internal_error(e).into_response(),
    }
}
