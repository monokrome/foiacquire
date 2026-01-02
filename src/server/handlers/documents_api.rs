//! Documents API endpoints for programmatic access.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use super::super::AppState;

/// Query parameters for document search/listing.
#[derive(Debug, Deserialize)]
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

/// Document response format for API.
#[derive(Debug, Serialize)]
pub struct DocumentResponse {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub source_url: String,
    pub status: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
    pub discovery_method: String,
    pub current_version: Option<VersionSummary>,
}

/// Version summary for document responses.
#[derive(Debug, Serialize)]
pub struct VersionSummary {
    pub id: i64,
    pub content_hash: String,
    pub file_size: u64,
    pub mime_type: String,
    pub acquired_at: String,
    pub original_filename: Option<String>,
    pub page_count: Option<u32>,
}

/// Paginated response wrapper.
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub page: usize,
    pub per_page: usize,
    pub total: u64,
    pub total_pages: u64,
}

/// List/search documents with filters and pagination.
pub async fn list_documents(
    State(state): State<AppState>,
    Query(params): Query<DocumentsQuery>,
) -> impl IntoResponse {
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    let page = params.page.unwrap_or(1).clamp(1, 100_000);
    let offset = page.saturating_sub(1) * per_page;

    let types: Vec<String> = params
        .types
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let tags: Vec<String> = params
        .tags
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // Get documents with filters
    let documents = match state
        .doc_repo
        .browse(
            params.source.as_deref(),
            None,
            &types,
            &tags,
            per_page as u32,
            offset as u32,
        )
        .await
    {
        Ok(docs) => docs,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // Get total count
    let total = state
        .doc_repo
        .browse_count(params.source.as_deref(), None, &types, &tags)
        .await
        .unwrap_or(documents.len() as u64);

    let items: Vec<DocumentResponse> = documents
        .into_iter()
        .map(|doc| {
            let current_version = doc.current_version().map(|v| VersionSummary {
                id: v.id,
                content_hash: v.content_hash.clone(),
                file_size: v.file_size,
                mime_type: v.mime_type.clone(),
                acquired_at: v.acquired_at.to_rfc3339(),
                original_filename: v.original_filename.clone(),
                page_count: v.page_count,
            });

            DocumentResponse {
                id: doc.id,
                source_id: doc.source_id,
                title: doc.title,
                source_url: doc.source_url,
                status: doc.status.as_str().to_string(),
                synopsis: doc.synopsis,
                tags: doc.tags,
                created_at: doc.created_at.to_rfc3339(),
                updated_at: doc.updated_at.to_rfc3339(),
                discovery_method: doc.discovery_method,
                current_version,
            }
        })
        .collect();

    let total_pages = (total + per_page as u64 - 1) / per_page as u64;

    Json(PaginatedResponse {
        items,
        page,
        per_page,
        total,
        total_pages,
    })
    .into_response()
}

/// Get a single document by ID.
pub async fn get_document(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
) -> impl IntoResponse {
    match state.doc_repo.get(&doc_id).await {
        Ok(Some(doc)) => {
            let current_version = doc.current_version().map(|v| VersionSummary {
                id: v.id,
                content_hash: v.content_hash.clone(),
                file_size: v.file_size,
                mime_type: v.mime_type.clone(),
                acquired_at: v.acquired_at.to_rfc3339(),
                original_filename: v.original_filename.clone(),
                page_count: v.page_count,
            });

            Json(DocumentResponse {
                id: doc.id,
                source_id: doc.source_id,
                title: doc.title,
                source_url: doc.source_url,
                status: doc.status.as_str().to_string(),
                synopsis: doc.synopsis,
                tags: doc.tags,
                created_at: doc.created_at.to_rfc3339(),
                updated_at: doc.updated_at.to_rfc3339(),
                discovery_method: doc.discovery_method,
                current_version,
            })
            .into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Document not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Get document content/text.
#[derive(Debug, Deserialize)]
pub struct ContentQuery {
    /// Version ID (optional, defaults to current)
    pub version: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct DocumentContentResponse {
    pub id: String,
    pub extracted_text: Option<String>,
    pub page_count: Option<u32>,
    pub pages: Vec<PageContent>,
}

#[derive(Debug, Serialize)]
pub struct PageContent {
    pub page_number: u32,
    pub text: Option<String>,
}

/// Get document text content (extracted text and OCR results).
pub async fn get_document_content(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
    Query(params): Query<ContentQuery>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "Document not found" })),
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

    let version_id = params
        .version
        .or_else(|| doc.current_version().map(|v| v.id))
        .unwrap_or(0);

    // Get pages with OCR text
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

    Json(DocumentContentResponse {
        id: doc.id,
        extracted_text: doc.extracted_text,
        page_count,
        pages: page_contents,
    })
    .into_response()
}
