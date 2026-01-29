//! API endpoint handlers.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;

use super::super::AppState;

/// Health check endpoint for container orchestration.
pub async fn health() -> impl IntoResponse {
    StatusCode::OK
}

/// Parameters for recent documents.
#[derive(Debug, Deserialize)]
pub struct RecentParams {
    pub limit: Option<usize>,
    pub source: Option<String>,
}

/// Source filter parameters.
#[derive(Debug, Deserialize)]
pub struct SourceFilterParams {
    pub source: Option<String>,
}

/// Tag search parameters.
#[derive(Debug, Deserialize)]
pub struct TagSearchParams {
    pub q: Option<String>,
    pub limit: Option<usize>,
}

/// API endpoint to get all sources with document counts.
pub async fn api_sources(State(state): State<AppState>) -> impl IntoResponse {
    let source_counts = match state.stats_cache.get_source_counts() {
        Some(counts) => counts,
        None => {
            let counts = state
                .doc_repo
                .get_all_source_counts()
                .await
                .unwrap_or_default();
            state.stats_cache.set_source_counts(counts.clone());
            counts
        }
    };

    let sources: Vec<_> = state
        .source_repo
        .get_all()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|s| {
            let count = source_counts.get(&s.id).copied().unwrap_or(0);
            serde_json::json!({
                "id": s.id,
                "name": s.name,
                "count": count
            })
        })
        .collect();

    axum::Json(sources).into_response()
}

/// API endpoint to get overall database status.
pub async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    let doc_count = state.doc_repo.count().await.unwrap_or(0);
    let needing_ocr = state.doc_repo.count_needing_ocr(None).await.unwrap_or(0);
    let needing_summary = state
        .doc_repo
        .count_needing_summarization(None)
        .await
        .unwrap_or(0);

    let crawl_stats = state.crawl_repo.get_all_stats().await.unwrap_or_default();

    let mut total_pending = 0u64;
    let mut total_failed = 0u64;
    let mut total_discovered = 0u64;
    let mut source_stats = Vec::new();

    for (source_id, stats) in &crawl_stats {
        total_pending += stats.urls_pending;
        total_failed += stats.urls_failed;
        total_discovered += stats.urls_discovered;
        source_stats.push(serde_json::json!({
            "source_id": source_id,
            "discovered": stats.urls_discovered,
            "fetched": stats.urls_fetched,
            "pending": stats.urls_pending,
            "failed": stats.urls_failed,
            "has_pending": stats.crawl_state.has_pending_urls,
        }));
    }

    let recent_urls: Vec<_> = state
        .crawl_repo
        .get_recent_downloads(None, 10)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "source_id": u.source_id,
                "fetched_at": u.fetched_at.map(|dt| dt.to_rfc3339()),
                "document_id": u.document_id,
            })
        })
        .collect();

    let failed_urls: Vec<_> = state
        .crawl_repo
        .get_failed_urls(None, 10)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "source_id": u.source_id,
                "error": u.last_error,
                "retry_count": u.retry_count,
            })
        })
        .collect();

    let type_stats: Vec<_> = state
        .doc_repo
        .get_type_stats()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(mime, count)| {
            serde_json::json!({
                "mime_type": mime,
                "count": count
            })
        })
        .collect();

    axum::Json(serde_json::json!({
        "documents": {
            "total": doc_count,
            "needing_ocr": needing_ocr,
            "needing_summarization": needing_summary,
        },
        "crawl": {
            "total_discovered": total_discovered,
            "total_pending": total_pending,
            "total_failed": total_failed,
            "sources": source_stats,
        },
        "recent_downloads": recent_urls,
        "recent_failures": failed_urls,
        "type_stats": type_stats,
    }))
}

/// API endpoint to get status for a specific source.
pub async fn api_source_status(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    let doc_count = state
        .doc_repo
        .count_by_source(&source_id)
        .await
        .unwrap_or(0);
    let needing_ocr = state
        .doc_repo
        .count_needing_ocr(Some(&source_id))
        .await
        .unwrap_or(0);
    let needing_summary = state
        .doc_repo
        .count_needing_summarization(Some(&source_id))
        .await
        .unwrap_or(0);

    let crawl_state = state.crawl_repo.get_crawl_state(&source_id).await.ok();
    let request_stats = state.crawl_repo.get_request_stats(&source_id).await.ok();

    let recent_urls: Vec<_> = state
        .crawl_repo
        .get_recent_downloads(Some(&source_id), 20)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "fetched_at": u.fetched_at.map(|dt| dt.to_rfc3339()),
                "document_id": u.document_id,
            })
        })
        .collect();

    let failed_urls: Vec<_> = state
        .crawl_repo
        .get_failed_urls(Some(&source_id), 20)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "error": u.last_error,
                "retry_count": u.retry_count,
            })
        })
        .collect();

    let type_stats: Vec<_> = state
        .doc_repo
        .get_type_stats()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(mime, count)| {
            serde_json::json!({
                "mime_type": mime,
                "count": count
            })
        })
        .collect();

    axum::Json(serde_json::json!({
        "source_id": source_id,
        "documents": {
            "total": doc_count,
            "needing_ocr": needing_ocr,
            "needing_summarization": needing_summary,
        },
        "crawl": crawl_state.map(|s| serde_json::json!({
            "discovered": s.urls_discovered,
            "fetched": s.urls_fetched,
            "pending": s.urls_pending,
            "failed": s.urls_failed,
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
        "recent_downloads": recent_urls,
        "recent_failures": failed_urls,
        "type_stats": type_stats,
    }))
}

/// API endpoint to get recent documents.
pub async fn api_recent_docs(
    State(state): State<AppState>,
    Query(params): Query<RecentParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20).min(100);
    let source_id = params.source.as_deref();

    match state.doc_repo.get_recent(limit as u32).await {
        Ok(docs) => {
            let doc_list: Vec<_> = docs
                .into_iter()
                .filter(|d| source_id.is_none() || Some(d.source_id.as_str()) == source_id)
                .map(|d| {
                    let version = d.current_version();
                    serde_json::json!({
                        "id": d.id,
                        "title": d.title,
                        "source_id": d.source_id,
                        "synopsis": d.synopsis,
                        "tags": d.tags,
                        "status": format!("{:?}", d.status),
                        "updated_at": d.updated_at.to_rfc3339(),
                        "mime_type": version.map(|v| v.mime_type.as_str()),
                        "file_size": version.map(|v| v.file_size),
                    })
                })
                .collect();
            axum::Json(doc_list).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// API endpoint to get document type statistics.
pub async fn api_type_stats(
    State(state): State<AppState>,
    Query(params): Query<SourceFilterParams>,
) -> impl IntoResponse {
    use crate::utils::MimeCategory;

    let stats = state
        .doc_repo
        .get_category_stats(params.source.as_deref())
        .await
        .unwrap_or_default();

    let stats_json: Vec<_> = stats
        .into_iter()
        .map(|(category, count)| {
            // Look up display name from MimeCategory
            let display_name = MimeCategory::from_id(&category)
                .map(|c| c.display_name())
                .unwrap_or(&category);
            serde_json::json!({
                "category": category,
                "name": display_name,
                "count": count
            })
        })
        .collect();
    axum::Json(stats_json).into_response()
}

/// API endpoint for tag autocomplete.
pub async fn api_search_tags(
    State(state): State<AppState>,
    Query(params): Query<TagSearchParams>,
) -> impl IntoResponse {
    let query = params.q.unwrap_or_default();
    let limit = params.limit.unwrap_or(20).clamp(1, 200);

    match state.doc_repo.search_tags(&query).await {
        Ok(tags) => {
            let result: Vec<_> = tags
                .iter()
                .take(limit)
                .map(|tag| serde_json::json!({ "tag": tag, "count": 0 }))
                .collect();
            axum::Json(result).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
