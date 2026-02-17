//! Full-text search API endpoint for page content.

use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::super::AppState;
use super::helpers::{bad_request, internal_error, paginate, PaginatedResponse};

#[derive(Debug, Deserialize, IntoParams)]
pub struct SearchQuery {
    /// Full-text search query
    pub q: String,
    /// Filter by source
    pub source: Option<String>,
    /// Filter to a single document
    pub document_id: Option<String>,
    /// Page number (1-indexed)
    pub page: Option<usize>,
    /// Items per page (default: 50, max: 200)
    pub per_page: Option<usize>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SearchResult {
    pub document_id: String,
    pub title: String,
    pub source_id: String,
    pub page_number: i32,
    pub headline: String,
}

/// Search document page content.
///
/// Uses Postgres full-text search (tsvector/tsquery) with headline snippets,
/// or LIKE fallback on SQLite. Returns page-level matches — a document can
/// appear multiple times with different page numbers and snippets.
#[utoipa::path(
    get,
    path = "/api/search",
    params(SearchQuery),
    responses(
        (status = 200, description = "Paginated search results", body = PaginatedResponse<SearchResult>),
        (status = 400, description = "Missing or empty search query")
    ),
    tag = "Search"
)]
pub async fn search_content(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    let q = params.q.trim();
    if q.is_empty() {
        return bad_request("Search query 'q' cannot be empty").into_response();
    }

    let (page, per_page, offset) = paginate(params.page, params.per_page);

    let total = match state
        .doc_repo
        .count_page_content_matches(q, params.source.as_deref(), params.document_id.as_deref())
        .await
    {
        Ok(c) => c,
        Err(e) => return internal_error(e).into_response(),
    };

    let rows = match state
        .doc_repo
        .search_page_content(
            q,
            params.source.as_deref(),
            params.document_id.as_deref(),
            per_page,
            offset,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return internal_error(e).into_response(),
    };

    let items: Vec<SearchResult> = rows
        .into_iter()
        .map(|r| SearchResult {
            document_id: r.document_id,
            title: r.title,
            source_id: r.source_id,
            page_number: r.page_number,
            headline: r.headline,
        })
        .collect();

    Json(PaginatedResponse::new(items, page, per_page, total)).into_response()
}
