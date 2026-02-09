//! Scrape control API endpoints.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use super::super::AppState;

/// List all scrapers/sources with their configuration.
pub async fn list_scrapers(State(state): State<AppState>) -> impl IntoResponse {
    let sources = state.source_repo.get_all().await.unwrap_or_default();
    let source_counts = state
        .doc_repo
        .get_all_source_counts()
        .await
        .unwrap_or_default();
    let crawl_stats = state.crawl_repo.get_all_stats().await.unwrap_or_default();

    let scrapers: Vec<_> = sources
        .into_iter()
        .map(|s| {
            let count = source_counts.get(&s.id).copied().unwrap_or(0);
            let stats = crawl_stats.get(&s.id);
            serde_json::json!({
                "id": s.id,
                "name": s.name,
                "source_type": format!("{:?}", s.source_type),
                "base_url": s.base_url,
                "last_scraped": s.last_scraped.map(|d| d.to_rfc3339()),
                "document_count": count,
                "crawl_stats": stats.map(|st| serde_json::json!({
                    "urls_discovered": st.urls_discovered,
                    "urls_fetched": st.urls_fetched,
                    "urls_pending": st.urls_pending,
                    "urls_failed": st.urls_failed,
                    "has_pending": st.crawl_state.has_pending_urls,
                }))
            })
        })
        .collect();

    Json(scrapers).into_response()
}

/// Get scrape status for a specific source.
pub async fn get_scrape_status(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    // Get source info
    let source = match state.source_repo.get(&source_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "Source not found" })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // Get crawl state
    let crawl_state = state.crawl_repo.get_crawl_state(&source_id).await.ok();
    let request_stats = state.crawl_repo.get_request_stats(&source_id).await.ok();

    // Get recent activity
    let recent_downloads = state
        .crawl_repo
        .get_recent_downloads(Some(&source_id), 10)
        .await
        .unwrap_or_default();
    let failed_urls = state
        .crawl_repo
        .get_failed_urls(Some(&source_id), 10)
        .await
        .unwrap_or_default();

    Json(serde_json::json!({
        "source_id": source_id,
        "name": source.name,
        "last_scraped": source.last_scraped.map(|d| d.to_rfc3339()),
        "crawl_state": crawl_state.map(|s| serde_json::json!({
            "urls_discovered": s.urls_discovered,
            "urls_fetched": s.urls_fetched,
            "urls_pending": s.urls_pending,
            "urls_failed": s.urls_failed,
            "has_pending": s.has_pending_urls,
            "last_crawl_started": s.last_crawl_started,
            "last_crawl_completed": s.last_crawl_completed,
        })),
        "request_stats": request_stats.map(|s| serde_json::json!({
            "total_requests": s.total_requests,
            "success_200": s.success_200,
            "not_modified_304": s.not_modified_304,
            "errors": s.errors,
            "avg_duration_ms": s.avg_duration_ms,
            "total_bytes": s.total_bytes,
        })),
        "recent_downloads": recent_downloads.iter().map(|u| serde_json::json!({
            "url": u.url,
            "fetched_at": u.fetched_at.map(|d| d.to_rfc3339()),
            "document_id": u.document_id,
        })).collect::<Vec<_>>(),
        "failed_urls": failed_urls.iter().map(|u| serde_json::json!({
            "url": u.url,
            "error": u.last_error,
            "retry_count": u.retry_count,
        })).collect::<Vec<_>>(),
    }))
    .into_response()
}

/// Query for scrape queue.
#[derive(Debug, Deserialize)]
pub struct QueueQuery {
    pub source: Option<String>,
    pub per_page: Option<usize>,
}

/// List URLs in the scrape queue.
pub async fn list_queue(
    State(state): State<AppState>,
    Query(params): Query<QueueQuery>,
) -> impl IntoResponse {
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);

    // Get pending URLs from crawl repository
    let pending = if let Some(source_id) = &params.source {
        state
            .crawl_repo
            .get_pending_urls(source_id, per_page as u32)
            .await
            .unwrap_or_default()
    } else {
        // Get from all sources
        Vec::new()
    };

    let items: Vec<_> = pending
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "source_id": u.source_id,
                "status": format!("{:?}", u.status),
                "discovery_method": format!("{:?}", u.discovery_method),
                "discovered_at": u.discovered_at.to_rfc3339(),
                "retry_count": u.retry_count,
                "depth": u.depth,
            })
        })
        .collect();

    Json(serde_json::json!({
        "items": items,
        "per_page": per_page
    }))
    .into_response()
}

/// Clear failed URLs for retry.
#[derive(Debug, Deserialize)]
pub struct RetryRequest {
    pub source: Option<String>,
}

pub async fn retry_failed(
    State(state): State<AppState>,
    Json(body): Json<RetryRequest>,
) -> impl IntoResponse {
    let result = state
        .crawl_repo
        .reset_failed_urls(body.source.as_deref())
        .await;

    match result {
        Ok(count) => Json(serde_json::json!({
            "reset_count": count,
            "message": format!("Reset {} failed URLs for retry", count)
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}
