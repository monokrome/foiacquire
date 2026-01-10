//! Helper types and utility functions for handlers.

use serde::{Deserialize, Serialize};

use super::super::AppState;

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
