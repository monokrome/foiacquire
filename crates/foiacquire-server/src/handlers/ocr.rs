//! Re-OCR API handlers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::super::{AppState, DeepSeekJobStatus};

/// Request body for re-OCR API.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ReOcrRequest {
    #[serde(default = "default_backend")]
    pub backend: String,
}

fn default_backend() -> String {
    "deepseek".to_string()
}

/// Response for re-OCR API.
#[derive(Debug, Serialize, ToSchema)]
pub struct ReOcrResponse {
    pub document_id: String,
    pub backend: String,
    pub pages_processed: u32,
    pub pages_total: u32,
    pub status: String,
    pub message: Option<String>,
}

/// Trigger re-OCR for a document using an alternative backend.
#[utoipa::path(
    post,
    path = "/api/documents/{document_id}/reocr",
    params(("document_id" = String, Path, description = "Document ID")),
    request_body = ReOcrRequest,
    responses(
        (status = 200, description = "OCR job started", body = ReOcrResponse),
        (status = 404, description = "Document not found"),
        (status = 409, description = "Another OCR job is running")
    ),
    tag = "OCR"
)]
pub async fn api_reocr_document(
    State(state): State<AppState>,
    Path(document_id): Path<String>,
    axum::Json(request): axum::Json<ReOcrRequest>,
) -> impl IntoResponse {
    use foiacquire_analysis::ocr::{DeepSeekBackend, OcrBackend, OcrConfig};

    if request.backend != "deepseek" {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "error".to_string(),
            message: Some("Only 'deepseek' backend is currently supported".to_string()),
        })
        .into_response();
    }

    {
        let job_status = state.deepseek_job.read().await;
        if job_status.document_id.is_some() && !job_status.completed {
            return (
                StatusCode::CONFLICT,
                axum::Json(ReOcrResponse {
                    document_id: document_id.clone(),
                    backend: request.backend,
                    pages_processed: job_status.pages_processed,
                    pages_total: job_status.total_pages,
                    status: "busy".to_string(),
                    message: Some(format!(
                        "DeepSeek OCR is already running on document '{}' ({}/{} pages)",
                        job_status
                            .document_id
                            .as_ref()
                            .unwrap_or(&"unknown".to_string()),
                        job_status.pages_processed,
                        job_status.total_pages
                    )),
                }),
            )
                .into_response();
        }
    }

    let doc = match state.doc_repo.get(&document_id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(ReOcrResponse {
                    document_id,
                    backend: request.backend,
                    pages_processed: 0,
                    pages_total: 0,
                    status: "error".to_string(),
                    message: Some("Document not found".to_string()),
                }),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(ReOcrResponse {
                    document_id,
                    backend: request.backend,
                    pages_processed: 0,
                    pages_total: 0,
                    status: "error".to_string(),
                    message: Some(format!("Database error: {}", e)),
                }),
            )
                .into_response();
        }
    };

    let version = match doc.versions.last() {
        Some(v) => v,
        None => {
            return axum::Json(ReOcrResponse {
                document_id,
                backend: request.backend,
                pages_processed: 0,
                pages_total: 0,
                status: "error".to_string(),
                message: Some("Document has no versions".to_string()),
            })
            .into_response();
        }
    };

    if version.mime_type != "application/pdf" {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "error".to_string(),
            message: Some("Only PDF documents are supported for re-OCR".to_string()),
        })
        .into_response();
    }

    let pdf_path = state.documents_dir.join(&version.file_path);

    let config = OcrConfig {
        use_gpu: true,
        ..Default::default()
    };
    let backend = DeepSeekBackend::with_config(config);

    if !backend.is_available() {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "error".to_string(),
            message: Some(format!(
                "DeepSeek backend not available. {}",
                backend.availability_hint()
            )),
        })
        .into_response();
    }

    let pages_needing_ocr = match state
        .doc_repo
        .get_pages_without_backend(&document_id, "deepseek")
        .await
    {
        Ok(pages) => pages,
        Err(e) => {
            return axum::Json(ReOcrResponse {
                document_id,
                backend: request.backend,
                pages_processed: 0,
                pages_total: 0,
                status: "error".to_string(),
                message: Some(format!("Failed to get pages: {}", e)),
            })
            .into_response();
        }
    };

    if pages_needing_ocr.is_empty() {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "complete".to_string(),
            message: Some("All pages already have DeepSeek OCR results".to_string()),
        })
        .into_response();
    }

    let total_pages = pages_needing_ocr.len() as u32;

    {
        let mut job_status = state.deepseek_job.write().await;
        *job_status = DeepSeekJobStatus {
            document_id: Some(document_id.clone()),
            pages_processed: 0,
            total_pages,
            error: None,
            completed: false,
        };
    }

    let job_state = state.clone();
    let job_doc_id = document_id.clone();

    tokio::spawn(async move {
        let mut processed = 0u32;

        for page in pages_needing_ocr {
            let page_id = page.id;
            let page_number = page.page_number;
            let pdf_path_clone = pdf_path.clone();
            let ocr_result = tokio::task::spawn_blocking(move || {
                let config = OcrConfig {
                    use_gpu: true,
                    ..Default::default()
                };
                let backend = DeepSeekBackend::with_config(config);
                backend.ocr_pdf_page(&pdf_path_clone, page_number)
            })
            .await;

            match ocr_result {
                Ok(Ok(result)) => {
                    if let Err(e) = job_state
                        .doc_repo
                        .store_page_ocr_result(
                            page_id,
                            "deepseek",
                            result.model.as_deref(),
                            Some(&result.text),
                            result.confidence,
                            None,
                            None,
                        )
                        .await
                    {
                        tracing::error!(
                            "Failed to store OCR result for page {}: {}",
                            page_number,
                            e
                        );
                    } else {
                        processed += 1;
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!("OCR failed for page {}: {:?}", page_number, e);
                    let _ = job_state
                        .doc_repo
                        .store_page_ocr_result(page_id, "deepseek", None, None, None, None, None)
                        .await;
                }
                Err(e) => {
                    tracing::error!("Task panic for page {}: {:?}", page_number, e);
                }
            }

            {
                let mut job_status = job_state.deepseek_job.write().await;
                job_status.pages_processed = processed;
            }
        }

        {
            let mut job_status = job_state.deepseek_job.write().await;
            job_status.pages_processed = processed;
            job_status.completed = true;
        }

        tracing::info!(
            "DeepSeek OCR complete for {}: {}/{} pages",
            job_doc_id,
            processed,
            total_pages
        );
    });

    axum::Json(ReOcrResponse {
        document_id,
        backend: request.backend,
        pages_processed: 0,
        pages_total: total_pages,
        status: "started".to_string(),
        message: Some(format!(
            "DeepSeek OCR started for {} pages. Poll /api/documents/reocr/status for progress.",
            total_pages
        )),
    })
    .into_response()
}

/// Get the current status of a DeepSeek OCR job.
#[utoipa::path(
    get,
    path = "/api/documents/reocr/status",
    responses(
        (status = 200, description = "OCR job status", body = ReOcrResponse)
    ),
    tag = "OCR"
)]
pub async fn api_reocr_status(State(state): State<AppState>) -> impl IntoResponse {
    let job_status = state.deepseek_job.read().await;

    let (status, document_id) = if job_status.document_id.is_none() {
        ("idle".to_string(), String::new())
    } else if job_status.completed {
        (
            "complete".to_string(),
            job_status.document_id.clone().unwrap_or_default(),
        )
    } else {
        (
            "running".to_string(),
            job_status.document_id.clone().unwrap_or_default(),
        )
    };

    axum::Json(ReOcrResponse {
        document_id,
        backend: "deepseek".to_string(),
        pages_processed: job_status.pages_processed,
        pages_total: job_status.total_pages,
        status,
        message: job_status.error.clone(),
    })
}
