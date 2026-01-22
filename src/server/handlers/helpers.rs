//! Helper types and utility functions for handlers.

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use super::super::AppState;
use crate::models::{Document, DocumentVersion};

/// Create an internal server error response.
pub fn internal_error(e: impl std::fmt::Display) -> impl IntoResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": e.to_string() })),
    )
}

/// Create a not found error response.
pub fn not_found(message: &str) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": message })),
    )
}

/// Create a bad request error response.
#[allow(dead_code)]
pub fn bad_request(message: &str) -> impl IntoResponse {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": message })),
    )
}

/// Version summary for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct VersionSummary {
    pub id: i64,
    pub content_hash: String,
    pub file_size: u64,
    pub mime_type: String,
    pub acquired_at: String,
    pub original_filename: Option<String>,
    pub page_count: Option<u32>,
}

impl From<&DocumentVersion> for VersionSummary {
    fn from(v: &DocumentVersion) -> Self {
        Self {
            id: v.id,
            content_hash: v.content_hash.clone(),
            file_size: v.file_size,
            mime_type: v.mime_type.clone(),
            acquired_at: v.acquired_at.to_rfc3339(),
            original_filename: v.original_filename.clone(),
            page_count: v.page_count,
        }
    }
}

/// Document summary for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct DocumentSummary {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_version: Option<VersionSummary>,
}

impl From<Document> for DocumentSummary {
    fn from(doc: Document) -> Self {
        let current_version = doc.current_version().map(VersionSummary::from);
        Self {
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
    }
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

impl<T> PaginatedResponse<T> {
    pub fn new(items: Vec<T>, page: usize, per_page: usize, total: u64) -> Self {
        let total_pages = total.div_ceil(per_page as u64);
        Self {
            items,
            page,
            per_page,
            total,
            total_pages,
        }
    }
}

/// Parse a comma-separated query parameter into a Vec of trimmed, non-empty strings.
///
/// # Example
/// ```ignore
/// let types = parse_csv_param(params.types.as_ref());
/// // "pdf, docx,  " -> ["pdf", "docx"]
/// ```
pub fn parse_csv_param(param: Option<&String>) -> Vec<String> {
    parse_csv_param_limit(param, None)
}

/// Parse a comma-separated query parameter with an optional limit on items.
pub fn parse_csv_param_limit(param: Option<&String>, limit: Option<usize>) -> Vec<String> {
    param
        .map(|t| {
            let iter = t
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            match limit {
                Some(n) => iter.take(n).collect(),
                None => iter.collect(),
            }
        })
        .unwrap_or_default()
}

/// Calculate pagination offset from page and per_page values.
/// Returns (page, per_page, offset) with clamped values.
pub fn paginate(page: Option<usize>, per_page: Option<usize>) -> (usize, usize, usize) {
    let per_page = per_page.unwrap_or(50).clamp(1, 200);
    let page = page.unwrap_or(1).clamp(1, 100_000);
    let offset = page.saturating_sub(1) * per_page;
    (page, per_page, offset)
}

/// Query params for date range filtering.
#[derive(Debug, Deserialize)]
pub struct DateRangeParams {
    pub start: Option<String>,
    pub end: Option<String>,
}

/// Timeline response structure.
#[derive(Debug, Serialize)]
pub struct TimelineResponse {
    pub buckets: Vec<TimelineBucket>,
    pub total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Single bucket in timeline data.
#[derive(Debug, Serialize)]
pub struct TimelineBucket {
    pub date: String,
    pub timestamp: i64,
    pub count: u64,
}

/// Version info for API response.
#[derive(Debug, Serialize)]
pub struct VersionInfo {
    pub content_hash: String,
    pub file_size: u64,
    pub mime_type: String,
    pub acquired_at: String,
}

/// Find sources that have a document with the given content hash.
pub async fn find_sources_with_hash(
    state: &AppState,
    content_hash: &str,
    exclude_source: &str,
) -> Vec<String> {
    match state
        .doc_repo
        .find_sources_by_hash(content_hash, Some(exclude_source))
        .await
    {
        Ok(results) => {
            let mut sources: Vec<String> = results
                .into_iter()
                .map(|(source_id, _, _)| source_id)
                .collect();
            sources.sort();
            sources.dedup();
            sources
        }
        Err(_) => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_param_none() {
        let result = parse_csv_param(None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_csv_param_empty() {
        let empty = String::new();
        let result = parse_csv_param(Some(&empty));
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_csv_param_single() {
        let single = "pdf".to_string();
        let result = parse_csv_param(Some(&single));
        assert_eq!(result, vec!["pdf"]);
    }

    #[test]
    fn test_parse_csv_param_multiple() {
        let multiple = "pdf, docx, xlsx".to_string();
        let result = parse_csv_param(Some(&multiple));
        assert_eq!(result, vec!["pdf", "docx", "xlsx"]);
    }

    #[test]
    fn test_parse_csv_param_trims_whitespace() {
        let spaced = "  pdf  ,  docx  ,   ".to_string();
        let result = parse_csv_param(Some(&spaced));
        assert_eq!(result, vec!["pdf", "docx"]);
    }

    #[test]
    fn test_parse_csv_param_limit() {
        let many = "a,b,c,d,e,f".to_string();
        let result = parse_csv_param_limit(Some(&many), Some(3));
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_paginate_defaults() {
        let (page, per_page, offset) = paginate(None, None);
        assert_eq!(page, 1);
        assert_eq!(per_page, 50);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_paginate_custom() {
        let (page, per_page, offset) = paginate(Some(3), Some(20));
        assert_eq!(page, 3);
        assert_eq!(per_page, 20);
        assert_eq!(offset, 40);
    }

    #[test]
    fn test_paginate_clamps_max() {
        let (page, per_page, _) = paginate(Some(999_999), Some(500));
        assert_eq!(page, 100_000);
        assert_eq!(per_page, 200);
    }

    #[test]
    fn test_paginate_clamps_min() {
        let (page, per_page, _) = paginate(Some(0), Some(0));
        assert_eq!(page, 1);
        assert_eq!(per_page, 1);
    }
}
