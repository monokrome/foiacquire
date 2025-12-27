//! Document detail and versions handlers.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::templates;
use super::super::AppState;
use super::helpers::{find_sources_with_hash, VersionInfo};
use crate::models::VirtualFile;

/// Query params for document detail navigation context.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DocumentDetailParams {
    pub types: Option<String>,
    pub tags: Option<String>,
    pub source: Option<String>,
    pub q: Option<String>,
}

/// Document detail page.
pub async fn document_detail(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
    Query(params): Query<DocumentDetailParams>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return Html(templates::base_template(
                "Not Found",
                "<p>Document not found.</p>",
                None,
            ));
        }
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load document: {}</p>", e),
                None,
            ));
        }
    };

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

    let source_for_nav = params.source.as_ref().map(|s| s.as_str()).unwrap_or("");
    let navigation = state
        .doc_repo
        .get_document_navigation(&doc_id, source_for_nav)
        .await
        .ok();

    let nav_query_string = {
        let mut qs_parts = Vec::new();
        if let Some(ref t) = params.types {
            qs_parts.push(format!("types={}", urlencoding::encode(t)));
        }
        if let Some(ref t) = params.tags {
            qs_parts.push(format!("tags={}", urlencoding::encode(t)));
        }
        if let Some(ref s) = params.source {
            qs_parts.push(format!("source={}", urlencoding::encode(s)));
        }
        if let Some(ref q) = params.q {
            qs_parts.push(format!("q={}", urlencoding::encode(q)));
        }
        if qs_parts.is_empty() {
            String::new()
        } else {
            format!("?{}", qs_parts.join("&"))
        }
    };

    let versions: Vec<_> = doc
        .versions
        .iter()
        .map(|v| {
            let relative_path = v
                .file_path
                .strip_prefix(&state.documents_dir)
                .unwrap_or(&v.file_path)
                .to_string_lossy()
                .to_string();
            (
                v.content_hash.clone(),
                relative_path,
                v.file_size,
                v.acquired_at,
                v.original_filename.clone(),
                v.server_date,
            )
        })
        .collect();

    let other_sources = if let Some(version) = doc.current_version() {
        find_sources_with_hash(&state, &version.content_hash, &doc.source_id).await
    } else {
        vec![]
    };

    let current_version = doc.current_version();
    let current_version_id = current_version.map(|v| v.id);

    let virtual_files: Vec<VirtualFile> = if let Some(vid) = current_version_id {
        state
            .doc_repo
            .get_virtual_files(&doc_id, vid as i32)
            .await
            .unwrap_or_default()
    } else {
        vec![]
    };

    let page_count: Option<u32> = match current_version_id {
        Some(vid) => state.doc_repo.count_pages(&doc_id, vid as i32).await.ok(),
        None => None,
    };

    let content = templates::document_detail(
        &doc.id,
        &doc.title,
        &doc.source_id,
        &doc.source_url,
        &versions,
        &other_sources,
        doc.extracted_text.as_deref(),
        doc.synopsis.as_deref(),
        &virtual_files,
        navigation
            .as_ref()
            .and_then(|n| n.prev_id.as_ref())
            .map(|s| s.as_str()),
        navigation
            .as_ref()
            .and_then(|n| n.prev_title.as_ref())
            .map(|s| s.as_str()),
        navigation
            .as_ref()
            .and_then(|n| n.next_id.as_ref())
            .map(|s| s.as_str()),
        navigation
            .as_ref()
            .and_then(|n| n.next_title.as_ref())
            .map(|s| s.as_str()),
        navigation.as_ref().map(|n| n.position).unwrap_or(0),
        navigation.as_ref().map(|n| n.total).unwrap_or(0),
        &nav_query_string,
        page_count,
        current_version_id,
    );

    Html(templates::base_template(&doc.title, &content, None))
}

/// Get document versions as JSON.
pub async fn document_versions(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "Document not found").into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let versions: Vec<_> = doc
        .versions
        .iter()
        .map(|v| VersionInfo {
            content_hash: v.content_hash.clone(),
            file_size: v.file_size,
            mime_type: v.mime_type.clone(),
            acquired_at: v.acquired_at.to_rfc3339(),
        })
        .collect();

    axum::Json(versions).into_response()
}
