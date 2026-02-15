//! Export API endpoints for bulk data export.

use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::io::Write;
use utoipa::{IntoParams, ToSchema};

use super::super::AppState;
use super::api_types::{AnnotationExport, ApiResponse, ExportStatsResponse};
use super::helpers::{internal_error, parse_csv_param};
use foia::repository::diesel_document::BrowseParams;

/// Export format options.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    #[default]
    Json,
    Jsonl,
    Csv,
}

/// Query params for export.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ExportQuery {
    /// Export format (json, jsonl, csv)
    #[serde(default)]
    pub format: ExportFormat,
    /// Filter by source ID
    pub source: Option<String>,
    /// Filter by tags (comma-separated)
    pub tags: Option<String>,
    /// Filter by types (comma-separated)
    pub types: Option<String>,
    /// Include full text content
    #[serde(default)]
    pub include_text: bool,
    /// Maximum documents to export (default: 10000)
    pub limit: Option<usize>,
}

/// Document export record.
#[derive(Debug, Serialize, ToSchema)]
pub struct ExportDocument {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub source_url: String,
    pub status: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
    pub page_count: Option<u32>,
    pub content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extracted_text: Option<String>,
}

/// Export documents in various formats.
#[utoipa::path(
    get,
    path = "/api/export/documents",
    params(ExportQuery),
    responses(
        (status = 200, description = "Exported documents (format varies by query param)", content_type = "application/json")
    ),
    tag = "Export"
)]
pub async fn export_documents(
    State(state): State<AppState>,
    Query(params): Query<ExportQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10_000).min(100_000);
    let types = parse_csv_param(params.types.as_ref());
    let tags = parse_csv_param(params.tags.as_ref());

    let documents = match state
        .doc_repo
        .browse(BrowseParams {
            source_id: params.source.as_deref(),
            categories: &types,
            tags: &tags,
            limit: limit as u32,
            ..Default::default()
        })
        .await
    {
        Ok(docs) => docs,
        Err(e) => return internal_error(e).into_response(),
    };

    let export_docs: Vec<ExportDocument> = documents
        .into_iter()
        .map(|doc| {
            let (mime_type, file_size, page_count, content_hash) =
                if let Some(v) = doc.current_version() {
                    (
                        Some(v.mime_type.clone()),
                        Some(v.file_size),
                        v.page_count,
                        Some(v.content_hash.clone()),
                    )
                } else {
                    (None, None, None, None)
                };
            ExportDocument {
                id: doc.id,
                source_id: doc.source_id,
                title: doc.title,
                source_url: doc.source_url,
                status: doc.status.as_str().to_string(),
                synopsis: doc.synopsis,
                tags: doc.tags,
                created_at: doc.created_at.to_rfc3339(),
                updated_at: doc.updated_at.to_rfc3339(),
                mime_type,
                file_size,
                page_count,
                content_hash,
                extracted_text: if params.include_text {
                    doc.extracted_text
                } else {
                    None
                },
            }
        })
        .collect();

    match params.format {
        ExportFormat::Json => {
            let json = serde_json::to_string_pretty(&export_docs).unwrap_or_default();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"documents.json\"",
                )
                .body(Body::from(json))
                .unwrap()
                .into_response()
        }
        ExportFormat::Jsonl => {
            let mut output = Vec::new();
            for doc in &export_docs {
                if let Ok(line) = serde_json::to_string(doc) {
                    writeln!(output, "{}", line).ok();
                }
            }
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/x-ndjson")
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"documents.jsonl\"",
                )
                .body(Body::from(output))
                .unwrap()
                .into_response()
        }
        ExportFormat::Csv => {
            let mut output = Vec::new();
            writeln!(
                output,
                "id,source_id,title,source_url,status,synopsis,tags,created_at,updated_at,mime_type,file_size,page_count,content_hash"
            )
            .ok();

            for doc in &export_docs {
                let tags_str = doc.tags.join(";");
                let synopsis_escaped = doc
                    .synopsis
                    .as_ref()
                    .map(|s| escape_csv(s))
                    .unwrap_or_default();
                let title_escaped = escape_csv(&doc.title);

                writeln!(
                    output,
                    "{},{},{},{},{},{},{},{},{},{},{},{},{}",
                    doc.id,
                    doc.source_id,
                    title_escaped,
                    escape_csv(&doc.source_url),
                    doc.status,
                    synopsis_escaped,
                    escape_csv(&tags_str),
                    doc.created_at,
                    doc.updated_at,
                    doc.mime_type.as_deref().unwrap_or(""),
                    doc.file_size.unwrap_or(0),
                    doc.page_count.unwrap_or(0),
                    doc.content_hash.as_deref().unwrap_or("")
                )
                .ok();
            }

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/csv")
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"documents.csv\"",
                )
                .body(Body::from(output))
                .unwrap()
                .into_response()
        }
    }
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Export metadata statistics.
#[utoipa::path(
    get,
    path = "/api/export/stats",
    responses(
        (status = 200, description = "Export statistics", body = ExportStatsResponse)
    ),
    tag = "Export"
)]
pub async fn export_stats(State(state): State<AppState>) -> impl IntoResponse {
    let total = state.doc_repo.count().await.unwrap_or(0);
    let type_stats = state.doc_repo.get_type_stats().await.unwrap_or_default();
    let source_counts = state
        .doc_repo
        .get_all_source_counts()
        .await
        .unwrap_or_default();
    let status_counts = state
        .doc_repo
        .count_all_by_status()
        .await
        .unwrap_or_default();

    ApiResponse::ok(ExportStatsResponse {
        total_documents: total,
        by_type: type_stats,
        by_source: source_counts,
        by_status: status_counts,
    })
    .into_response()
}

/// Export annotations only (for backup/transfer).
#[utoipa::path(
    get,
    path = "/api/export/annotations",
    params(ExportQuery),
    responses(
        (status = 200, description = "Exported annotations", content_type = "application/json")
    ),
    tag = "Export"
)]
pub async fn export_annotations(
    State(state): State<AppState>,
    Query(params): Query<ExportQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10_000).min(100_000);

    let documents = match state
        .doc_repo
        .browse(BrowseParams {
            source_id: params.source.as_deref(),
            categories: &[],
            tags: &[],
            limit: limit as u32,
            ..Default::default()
        })
        .await
    {
        Ok(docs) => docs,
        Err(e) => return internal_error(e).into_response(),
    };

    let annotations: Vec<AnnotationExport> = documents
        .into_iter()
        .filter(|d| d.synopsis.is_some() || !d.tags.is_empty())
        .map(|d| AnnotationExport {
            id: d.id,
            source_url: d.source_url,
            synopsis: d.synopsis,
            tags: d.tags,
        })
        .collect();

    match params.format {
        ExportFormat::Jsonl => {
            let mut output = Vec::new();
            for ann in &annotations {
                if let Ok(line) = serde_json::to_string(ann) {
                    writeln!(output, "{}", line).ok();
                }
            }
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/x-ndjson")
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"annotations.jsonl\"",
                )
                .body(Body::from(output))
                .unwrap()
                .into_response()
        }
        _ => {
            let json = serde_json::to_string_pretty(&annotations).unwrap_or_default();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"annotations.json\"",
                )
                .body(Body::from(json))
                .unwrap()
                .into_response()
        }
    }
}
