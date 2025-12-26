//! Row parsing helpers for crawl repository.

use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::models::{CrawlRequest, CrawlUrl, DiscoveryMethod, UrlStatus};

/// Parse a database row into a CrawlUrl.
pub fn row_to_crawl_url(row: &rusqlite::Row) -> rusqlite::Result<CrawlUrl> {
    let context_str: String = row.get("discovery_context")?;
    let discovery_context: HashMap<String, serde_json::Value> =
        serde_json::from_str(&context_str).unwrap_or_default();

    Ok(CrawlUrl {
        url: row.get("url")?,
        source_id: row.get("source_id")?,
        status: UrlStatus::from_str(&row.get::<_, String>("status")?)
            .unwrap_or(UrlStatus::Discovered),
        discovery_method: DiscoveryMethod::from_str(&row.get::<_, String>("discovery_method")?)
            .unwrap_or(DiscoveryMethod::Seed),
        parent_url: row.get("parent_url")?,
        discovery_context,
        depth: row.get::<_, i32>("depth")? as u32,
        discovered_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("discovered_at")?)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        fetched_at: row
            .get::<_, Option<String>>("fetched_at")?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc)),
        retry_count: row.get::<_, i32>("retry_count")? as u32,
        last_error: row.get("last_error")?,
        next_retry_at: row
            .get::<_, Option<String>>("next_retry_at")?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc)),
        etag: row.get("etag")?,
        last_modified: row.get("last_modified")?,
        content_hash: row.get("content_hash")?,
        document_id: row.get("document_id")?,
    })
}

/// Parse a database row into a CrawlRequest.
pub fn row_to_crawl_request(row: &rusqlite::Row) -> rusqlite::Result<CrawlRequest> {
    let request_headers_str: String = row.get("request_headers")?;
    let response_headers_str: String = row.get("response_headers")?;

    Ok(CrawlRequest {
        id: Some(row.get("id")?),
        source_id: row.get("source_id")?,
        url: row.get("url")?,
        method: row.get("method")?,
        request_headers: serde_json::from_str(&request_headers_str).unwrap_or_default(),
        request_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("request_at")?)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        response_status: row
            .get::<_, Option<i32>>("response_status")?
            .map(|s| s as u16),
        response_headers: serde_json::from_str(&response_headers_str).unwrap_or_default(),
        response_at: row
            .get::<_, Option<String>>("response_at")?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc)),
        response_size: row
            .get::<_, Option<i64>>("response_size")?
            .map(|s| s as u64),
        duration_ms: row.get::<_, Option<i64>>("duration_ms")?.map(|d| d as u64),
        error: row.get("error")?,
        was_conditional: row.get::<_, i32>("was_conditional")? != 0,
        was_not_modified: row.get::<_, i32>("was_not_modified")? != 0,
    })
}
