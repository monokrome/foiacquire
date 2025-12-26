//! Helper types and utility functions for handlers.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::super::AppState;
use crate::models::Document;
use crate::repository::DocumentSummary;

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

/// Build timeline data from documents.
pub fn build_timeline_data(documents: &[Document]) -> TimelineResponse {
    use std::collections::BTreeMap;

    let mut date_counts: BTreeMap<String, u64> = BTreeMap::new();

    for doc in documents {
        if let Some(version) = doc.current_version() {
            let date = version.acquired_at.format("%Y-%m-%d").to_string();
            *date_counts.entry(date).or_default() += 1;
        }
    }

    let buckets: Vec<_> = date_counts
        .into_iter()
        .map(|(date, count)| {
            let timestamp = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|dt| dt.and_utc().timestamp())
                .unwrap_or(0);
            TimelineBucket {
                date,
                timestamp,
                count,
            }
        })
        .collect();

    let total = buckets.iter().map(|b| b.count).sum();

    TimelineResponse {
        buckets,
        total,
        error: None,
    }
}

/// Build timeline from lightweight summaries.
pub fn build_timeline_from_summaries(summaries: &[DocumentSummary]) -> TimelineResponse {
    use std::collections::BTreeMap;

    let mut date_counts: BTreeMap<String, u64> = BTreeMap::new();

    for summary in summaries {
        if let Some(ref version) = summary.current_version {
            let date = version.acquired_at.format("%Y-%m-%d").to_string();
            *date_counts.entry(date).or_default() += 1;
        }
    }

    let buckets: Vec<_> = date_counts
        .into_iter()
        .map(|(date, count)| {
            let timestamp = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|dt| dt.and_utc().timestamp())
                .unwrap_or(0);
            TimelineBucket {
                date,
                timestamp,
                count,
            }
        })
        .collect();

    let total = buckets.iter().map(|b| b.count).sum();

    TimelineResponse {
        buckets,
        total,
        error: None,
    }
}

/// Find documents that exist in multiple sources.
pub fn find_cross_source_duplicates(
    state: &AppState,
    documents: &[Document],
) -> HashMap<String, Vec<String>> {
    let mut result: HashMap<String, Vec<String>> = HashMap::new();

    let hashes = match state.doc_repo.get_content_hashes() {
        Ok(h) => h,
        Err(_) => return result,
    };

    let mut hash_to_sources: HashMap<String, Vec<String>> = HashMap::new();
    for (_, source_id, content_hash, _) in &hashes {
        hash_to_sources
            .entry(content_hash.clone())
            .or_default()
            .push(source_id.clone());
    }

    for doc in documents {
        if let Some(version) = doc.current_version() {
            if let Some(sources) = hash_to_sources.get(&version.content_hash) {
                if sources.len() > 1 {
                    result.insert(version.content_hash.clone(), sources.clone());
                }
            }
        }
    }

    result
}

/// Find sources that have a document with the given content hash.
pub fn find_sources_with_hash(
    state: &AppState,
    content_hash: &str,
    exclude_source: &str,
) -> Vec<String> {
    match state
        .doc_repo
        .find_sources_by_hash(content_hash, Some(exclude_source))
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

/// Map MIME type to category.
pub fn mime_to_category(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "documents",
        m if m.contains("word") || m == "application/msword" => "documents",
        m if m.contains("rfc822") || m.contains("message") => "documents",
        m if m.starts_with("text/") && !m.contains("csv") => "documents",
        m if m.contains("excel")
            || m.contains("spreadsheet")
            || m == "text/csv"
            || m == "application/json"
            || m == "application/xml" =>
        {
            "data"
        }
        m if m.starts_with("image/") => "images",
        _ => "other",
    }
}
