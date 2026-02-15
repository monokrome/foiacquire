//! Page rendering and API handlers.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::super::AppState;

/// Parameters for pages view/API.
#[derive(Debug, Deserialize, IntoParams)]
pub struct PagesParams {
    pub version: Option<i64>,
    pub offset: Option<u32>,
    pub limit: Option<u32>,
}

/// Single page data for API response.
#[derive(Debug, Serialize, ToSchema)]
pub struct PageData {
    pub page_number: u32,
    pub ocr_text: Option<String>,
    pub pdf_text: Option<String>,
    pub final_text: Option<String>,
    pub image_base64: Option<String>,
    pub ocr_status: String,
    pub deepseek_text: Option<String>,
}

/// Pages API response.
#[derive(Debug, Serialize, ToSchema)]
pub struct PagesResponse {
    pub pages: Vec<PageData>,
    pub total_pages: u32,
    pub has_more: bool,
    pub document_id: String,
    pub version_id: i64,
}

/// API endpoint to get paginated pages with rendered images and OCR text.
#[utoipa::path(
    get,
    path = "/api/documents/{doc_id}/pages",
    params(
        ("doc_id" = String, Path, description = "Document ID"),
        PagesParams,
    ),
    responses(
        (status = 200, description = "Paginated page data", body = PagesResponse),
        (status = 404, description = "Document not found")
    ),
    tag = "Pages"
)]
pub async fn api_document_pages(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
    Query(params): Query<PagesParams>,
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

    let version_id = params
        .version
        .or_else(|| doc.current_version().map(|v| v.id));
    let version_id = match version_id {
        Some(id) => id,
        None => {
            return (StatusCode::NOT_FOUND, "No version found").into_response();
        }
    };

    let version = doc.versions.iter().find(|v| v.id == version_id);
    let version = match version {
        Some(v) => v,
        None => {
            return (StatusCode::NOT_FOUND, "Version not found").into_response();
        }
    };

    let all_pages: Vec<foia::models::DocumentPage> =
        match state.doc_repo.get_pages(&doc_id, version_id as i32).await {
            Ok(p) => p,
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

    let total_pages = all_pages.len() as u32;
    let offset = params.offset.unwrap_or(0).min(100_000);
    let limit = params.limit.unwrap_or(5).clamp(1, 20);

    let start = (offset as usize).min(all_pages.len());
    let selected_pages: Vec<_> = all_pages
        .into_iter()
        .skip(start)
        .take(limit as usize)
        .collect();

    let page_ids: Vec<i64> = selected_pages.iter().map(|p| p.id).collect();
    let all_ocr_results = state
        .doc_repo
        .get_pages_ocr_results_bulk(&page_ids)
        .await
        .unwrap_or_default();

    let mut deepseek_map: std::collections::HashMap<i64, Option<String>> =
        std::collections::HashMap::new();
    for (page_id, ocr_results) in all_ocr_results {
        for result in ocr_results {
            let backend = result.backend;
            let text = result.text;
            if backend == "deepseek" {
                deepseek_map.insert(page_id, text);
                break;
            }
        }
    }

    let is_pdf = version.mime_type.contains("pdf");
    let pdf_path = version.resolve_path(&state.documents_dir, &doc.source_url, &doc.title);

    let page_data_list: Vec<PageData> = if is_pdf {
        let mut handles = Vec::new();
        for page in selected_pages {
            let path = pdf_path.clone();
            let page_num = page.page_number;
            let page_id = page.id;
            let ocr_text = page.ocr_text;
            let pdf_text = page.pdf_text;
            let final_text = page.final_text;
            let ocr_status = page.ocr_status.as_str().to_string();
            let deepseek_text = deepseek_map.get(&page_id).cloned().flatten();

            let handle = tokio::task::spawn_blocking(move || {
                let image_base64 = render_pdf_page_to_base64(&path, page_num);
                PageData {
                    page_number: page_num,
                    ocr_text,
                    pdf_text,
                    final_text,
                    image_base64,
                    ocr_status,
                    deepseek_text,
                }
            });
            handles.push(handle);
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            if let Ok(page_data) = handle.await {
                results.push(page_data);
            }
        }
        results.sort_by_key(|p| p.page_number);
        results
    } else {
        selected_pages
            .into_iter()
            .map(|page| {
                let deepseek_text = deepseek_map.get(&page.id).cloned().flatten();
                PageData {
                    page_number: page.page_number,
                    ocr_text: page.ocr_text,
                    pdf_text: page.pdf_text,
                    final_text: page.final_text,
                    image_base64: None,
                    ocr_status: page.ocr_status.as_str().to_string(),
                    deepseek_text,
                }
            })
            .collect()
    };

    let has_more = (start + limit as usize) < total_pages as usize;

    axum::Json(PagesResponse {
        pages: page_data_list,
        total_pages,
        has_more,
        document_id: doc_id,
        version_id,
    })
    .into_response()
}

fn render_pdf_page_to_base64(pdf_path: &std::path::Path, page_number: u32) -> Option<String> {
    use base64::Engine;
    use std::process::Command;

    let temp_dir = std::env::temp_dir();
    let output_prefix = temp_dir.join(format!("foia_page_{}", uuid::Uuid::new_v4()));
    let output_path = output_prefix.with_extension("png");

    struct CleanupGuard<'a>(&'a std::path::Path);
    impl Drop for CleanupGuard<'_> {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0);
        }
    }
    let _cleanup = CleanupGuard(&output_path);

    let status = Command::new("pdftoppm")
        .args([
            "-png",
            "-r",
            "150",
            "-f",
            &page_number.to_string(),
            "-l",
            &page_number.to_string(),
            "-singlefile",
        ])
        .arg(pdf_path)
        .arg(&output_prefix)
        .status();

    if status.map(|s| s.success()).unwrap_or(false) {
        if let Ok(image_data) = std::fs::read(&output_path) {
            let base64_str = base64::engine::general_purpose::STANDARD.encode(&image_data);
            return Some(format!("data:image/png;base64,{}", base64_str));
        }
    }

    None
}
