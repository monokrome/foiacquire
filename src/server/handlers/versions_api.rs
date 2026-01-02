//! Document versions API endpoints.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Serialize;

use super::super::AppState;

/// Full version details for API response.
#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub id: i64,
    pub content_hash: String,
    pub content_hash_blake3: Option<String>,
    pub file_path: String,
    pub file_size: u64,
    pub mime_type: String,
    pub acquired_at: String,
    pub source_url: Option<String>,
    pub original_filename: Option<String>,
    pub server_date: Option<String>,
    pub page_count: Option<u32>,
    pub archive_snapshot_id: Option<i32>,
    pub earliest_archived_at: Option<String>,
}

/// Get all versions of a document.
pub async fn list_versions(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
) -> impl IntoResponse {
    match state.doc_repo.get(&doc_id).await {
        Ok(Some(doc)) => {
            let versions: Vec<VersionResponse> = doc
                .versions
                .into_iter()
                .map(|v| VersionResponse {
                    id: v.id,
                    content_hash: v.content_hash,
                    content_hash_blake3: v.content_hash_blake3,
                    file_path: v.file_path.to_string_lossy().to_string(),
                    file_size: v.file_size,
                    mime_type: v.mime_type,
                    acquired_at: v.acquired_at.to_rfc3339(),
                    source_url: v.source_url,
                    original_filename: v.original_filename,
                    server_date: v.server_date.map(|d| d.to_rfc3339()),
                    page_count: v.page_count,
                    archive_snapshot_id: v.archive_snapshot_id,
                    earliest_archived_at: v.earliest_archived_at.map(|d| d.to_rfc3339()),
                })
                .collect();

            Json(serde_json::json!({
                "document_id": doc_id,
                "version_count": versions.len(),
                "versions": versions
            }))
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

/// Get a specific version of a document.
pub async fn get_version(
    State(state): State<AppState>,
    Path((doc_id, version_id)): Path<(String, i64)>,
) -> impl IntoResponse {
    match state.doc_repo.get(&doc_id).await {
        Ok(Some(doc)) => {
            if let Some(version) = doc.versions.into_iter().find(|v| v.id == version_id) {
                Json(VersionResponse {
                    id: version.id,
                    content_hash: version.content_hash,
                    content_hash_blake3: version.content_hash_blake3,
                    file_path: version.file_path.to_string_lossy().to_string(),
                    file_size: version.file_size,
                    mime_type: version.mime_type,
                    acquired_at: version.acquired_at.to_rfc3339(),
                    source_url: version.source_url,
                    original_filename: version.original_filename,
                    server_date: version.server_date.map(|d| d.to_rfc3339()),
                    page_count: version.page_count,
                    archive_snapshot_id: version.archive_snapshot_id,
                    earliest_archived_at: version.earliest_archived_at.map(|d| d.to_rfc3339()),
                })
                .into_response()
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "Version not found" })),
                )
                    .into_response()
            }
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

/// Find documents with matching content hash (duplicates).
pub async fn find_by_hash(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    match state.doc_repo.find_sources_by_hash(&hash, None).await {
        Ok(sources) => Json(serde_json::json!({
            "hash": hash,
            "sources": sources
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}
