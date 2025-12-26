//! Request logging for crawl repository.

use rusqlite::params;

use super::helpers::row_to_crawl_request;
use super::{CrawlRepository, Result};
use crate::models::CrawlRequest;

impl CrawlRepository {
    /// Log an HTTP request and return its ID.
    pub fn log_request(&self, request: &CrawlRequest) -> Result<i64> {
        let conn = self.connect()?;

        conn.execute(
            r#"
            INSERT INTO crawl_requests (
                source_id, url, method, request_headers, request_at,
                response_status, response_headers, response_at,
                response_size, duration_ms, error,
                was_conditional, was_not_modified
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            params![
                request.source_id,
                request.url,
                request.method,
                serde_json::to_string(&request.request_headers)?,
                request.request_at.to_rfc3339(),
                request.response_status.map(|s| s as i32),
                serde_json::to_string(&request.response_headers)?,
                request.response_at.map(|dt| dt.to_rfc3339()),
                request.response_size.map(|s| s as i64),
                request.duration_ms.map(|d| d as i64),
                request.error,
                request.was_conditional as i32,
                request.was_not_modified as i32,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Get the most recent request for a URL.
    pub fn get_last_request(&self, source_id: &str, url: &str) -> Result<Option<CrawlRequest>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_requests
            WHERE source_id = ? AND url = ?
            ORDER BY request_at DESC
            LIMIT 1
        "#,
        )?;

        super::super::to_option(stmt.query_row(params![source_id, url], row_to_crawl_request))
    }
}
