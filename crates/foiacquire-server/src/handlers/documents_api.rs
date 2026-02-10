//! Documents API endpoints for programmatic access.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::super::AppState;
use super::api_types::ApiResponse;
use super::helpers::{
    internal_error, not_found, paginate, parse_csv_param, DocumentSummary, PaginatedResponse,
};
use foiacquire::repository::diesel_document::BrowseParams;

/// Query parameters for document search/listing.
#[derive(Debug, Deserialize, IntoParams)]
pub struct DocumentsQuery {
    /// Filter by source ID
    pub source: Option<String>,
    /// Filter by document status (pending, downloaded, ocr_complete, indexed, failed)
    pub status: Option<String>,
    /// Filter by MIME type categories (comma-separated: documents,spreadsheets,images)
    pub types: Option<String>,
    /// Filter by tags (comma-separated)
    pub tags: Option<String>,
    /// Full-text search query
    pub q: Option<String>,
    /// Page number (1-indexed)
    pub page: Option<usize>,
    /// Items per page (default: 50, max: 200)
    pub per_page: Option<usize>,
    /// Sort field (updated_at, created_at, title, file_size)
    pub sort: Option<String>,
    /// Sort order (asc, desc)
    pub order: Option<String>,
}

/// List/search documents with filters and pagination.
#[utoipa::path(
    get,
    path = "/api/documents",
    params(DocumentsQuery),
    responses(
        (status = 200, description = "Paginated list of documents", body = PaginatedResponse<DocumentSummary>)
    ),
    tag = "Documents"
)]
pub async fn list_documents(
    State(state): State<AppState>,
    Query(params): Query<DocumentsQuery>,
) -> impl IntoResponse {
    let (page, per_page, offset) = paginate(params.page, params.per_page);
    let types = parse_csv_param(params.types.as_ref());
    let tags = parse_csv_param(params.tags.as_ref());

    let documents = match state
        .doc_repo
        .browse(BrowseParams {
            source_id: params.source.as_deref(),
            status: params.status.as_deref(),
            categories: &types,
            tags: &tags,
            search_query: params.q.as_deref(),
            sort_field: params.sort.as_deref(),
            sort_order: params.order.as_deref(),
            limit: per_page as u32,
            offset: offset as u32,
        })
        .await
    {
        Ok(docs) => docs,
        Err(e) => return internal_error(e).into_response(),
    };

    let total = state
        .doc_repo
        .browse_count(
            params.source.as_deref(),
            params.status.as_deref(),
            &types,
            &tags,
            params.q.as_deref(),
        )
        .await
        .unwrap_or(documents.len() as u64);

    let items: Vec<DocumentSummary> = documents.into_iter().map(DocumentSummary::from).collect();

    Json(PaginatedResponse::new(items, page, per_page, total)).into_response()
}

/// Get a single document by ID.
#[utoipa::path(
    get,
    path = "/api/documents/{doc_id}",
    params(("doc_id" = String, Path, description = "Document ID")),
    responses(
        (status = 200, description = "Document details", body = DocumentSummary),
        (status = 404, description = "Document not found")
    ),
    tag = "Documents"
)]
pub async fn get_document(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
) -> impl IntoResponse {
    match state.doc_repo.get(&doc_id).await {
        Ok(Some(doc)) => ApiResponse::ok(DocumentSummary::from(doc)).into_response(),
        Ok(None) => not_found("Document not found").into_response(),
        Err(e) => internal_error(e).into_response(),
    }
}

/// Get document content/text.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ContentQuery {
    /// Version ID (optional, defaults to current)
    pub version: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DocumentContentResponse {
    pub id: String,
    pub extracted_text: Option<String>,
    pub page_count: Option<u32>,
    pub pages: Vec<PageContent>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PageContent {
    pub page_number: u32,
    pub text: Option<String>,
}

/// Get document text content (extracted text and OCR results).
#[utoipa::path(
    get,
    path = "/api/documents/{doc_id}/content",
    params(
        ("doc_id" = String, Path, description = "Document ID"),
        ContentQuery,
    ),
    responses(
        (status = 200, description = "Document text content", body = DocumentContentResponse),
        (status = 404, description = "Document not found")
    ),
    tag = "Documents"
)]
pub async fn get_document_content(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
    Query(params): Query<ContentQuery>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id).await {
        Ok(Some(d)) => d,
        Ok(None) => return not_found("Document not found").into_response(),
        Err(e) => return internal_error(e).into_response(),
    };

    let version_id = params
        .version
        .or_else(|| doc.current_version().map(|v| v.id))
        .unwrap_or(0);

    let pages = state
        .doc_repo
        .get_pages(&doc_id, version_id as i32)
        .await
        .unwrap_or_default();

    let page_contents: Vec<PageContent> = pages
        .into_iter()
        .map(|p| PageContent {
            page_number: p.page_number,
            text: p.final_text.or(p.ocr_text).or(p.pdf_text),
        })
        .collect();

    let page_count = doc.current_version().and_then(|v| v.page_count);

    ApiResponse::ok(DocumentContentResponse {
        id: doc.id,
        extracted_text: doc.extracted_text,
        page_count,
        pages: page_contents,
    })
    .into_response()
}
