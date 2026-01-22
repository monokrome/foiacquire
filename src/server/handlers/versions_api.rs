//! Document versions API endpoints.

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use serde::Serialize;

use super::super::AppState;
use super::helpers::{internal_error, not_found};

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

impl From<crate::models::DocumentVersion> for VersionResponse {
    fn from(v: crate::models::DocumentVersion) -> Self {
        Self {
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
        }
    }
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
                .map(VersionResponse::from)
                .collect();

            Json(serde_json::json!({
                "document_id": doc_id,
                "version_count": versions.len(),
                "versions": versions
            }))
            .into_response()
        }
        Ok(None) => not_found("Document not found").into_response(),
        Err(e) => internal_error(e).into_response(),
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
                Json(VersionResponse::from(version)).into_response()
            } else {
                not_found("Version not found").into_response()
            }
        }
        Ok(None) => not_found("Document not found").into_response(),
        Err(e) => internal_error(e).into_response(),
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
        Err(e) => internal_error(e).into_response(),
    }
}
