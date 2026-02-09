//! Document detail and versions handlers.

use askama::Template;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::template_structs::{
    DocumentDetailTemplate, ErrorTemplate, VersionItem, VirtualFileRow,
};
use super::super::AppState;
use super::helpers::{find_sources_with_hash, VersionInfo};
use foiacquire::utils::format_size;

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
            let template = ErrorTemplate {
                title: "Not Found",
                message: "Document not found.",
            };
            return Html(
                template
                    .render()
                    .unwrap_or_else(|_| "Not found".to_string()),
            );
        }
        Err(e) => {
            let msg = format!("Failed to load document: {}", e);
            let template = ErrorTemplate {
                title: "Error",
                message: &msg,
            };
            return Html(template.render().unwrap_or(msg));
        }
    };

    let source_for_nav = params.source.as_deref().unwrap_or("");
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

    let versions: Vec<VersionItem> = doc
        .versions
        .iter()
        .map(|v| {
            let relative_path = v
                .file_path
                .strip_prefix(&state.documents_dir)
                .unwrap_or(&v.file_path)
                .to_string_lossy()
                .to_string();

            let date_str = v
                .server_date
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| v.acquired_at.format("%Y-%m-%d").to_string());

            let filename = v
                .original_filename
                .clone()
                .unwrap_or_else(|| "unknown".to_string());

            VersionItem {
                path: relative_path,
                filename,
                size_str: format_size(v.file_size),
                date_str,
            }
        })
        .collect();

    let other_sources = if let Some(version) = doc.current_version() {
        find_sources_with_hash(&state, &version.content_hash, &doc.source_id).await
    } else {
        vec![]
    };

    let current_version = doc.current_version();
    let current_version_id = current_version.map(|v| v.id);

    let virtual_files: Vec<VirtualFileRow> = if let Some(vid) = current_version_id {
        state
            .doc_repo
            .get_virtual_files(&doc_id, vid as i32)
            .await
            .unwrap_or_default()
            .iter()
            .map(VirtualFileRow::from_virtual_file)
            .collect()
    } else {
        vec![]
    };

    let page_count: Option<u32> = match current_version_id {
        Some(vid) => state.doc_repo.count_pages(&doc_id, vid as i32).await.ok(),
        None => None,
    };

    // Navigation helpers
    let (has_prev, prev_id_val, prev_title_val, prev_title_truncated) =
        if let Some(ref nav) = navigation {
            if let (Some(id), Some(title)) = (&nav.prev_id, &nav.prev_title) {
                let truncated: String = title.chars().take(40).collect();
                let truncated = if title.len() > 40 {
                    format!("{}...", truncated)
                } else {
                    truncated
                };
                (true, id.clone(), title.clone(), truncated)
            } else {
                (false, String::new(), String::new(), String::new())
            }
        } else {
            (false, String::new(), String::new(), String::new())
        };

    let (has_next, next_id_val, next_title_val, next_title_truncated) =
        if let Some(ref nav) = navigation {
            if let (Some(id), Some(title)) = (&nav.next_id, &nav.next_title) {
                let truncated: String = title.chars().take(40).collect();
                let truncated = if title.len() > 40 {
                    format!("{}...", truncated)
                } else {
                    truncated
                };
                (true, id.clone(), title.clone(), truncated)
            } else {
                (false, String::new(), String::new(), String::new())
            }
        } else {
            (false, String::new(), String::new(), String::new())
        };

    let template = DocumentDetailTemplate {
        title: &doc.title,
        doc_id: &doc.id,
        source_id: &doc.source_id,
        source_url: &doc.source_url,
        versions,
        has_versions: !doc.versions.is_empty(),
        other_sources,
        has_other_sources: !doc.versions.is_empty()
            && doc.current_version().is_some()
            && !find_sources_with_hash(
                &state,
                &doc.current_version().unwrap().content_hash,
                &doc.source_id,
            )
            .await
            .is_empty(),
        has_extracted_text: doc.extracted_text.is_some(),
        extracted_text_val: doc.extracted_text.clone().unwrap_or_default(),
        virtual_files: virtual_files.clone(),
        has_virtual_files: !virtual_files.is_empty(),
        virtual_files_count: virtual_files.len(),
        has_prev,
        prev_id_val,
        prev_title_val,
        prev_title_truncated,
        has_next,
        next_id_val,
        next_title_val,
        next_title_truncated,
        position: navigation.as_ref().map(|n| n.position).unwrap_or(0),
        total: navigation.as_ref().map(|n| n.total).unwrap_or(0),
        nav_query_string,
        has_pages: page_count.is_some() && page_count.unwrap() > 0,
        page_count_val: page_count.unwrap_or(0),
        version_id_val: current_version_id.unwrap_or(0),
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
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
