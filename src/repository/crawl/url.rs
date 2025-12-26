//! URL CRUD operations for crawl repository.

use chrono::{DateTime, Utc};
use rusqlite::params;

use super::helpers::row_to_crawl_url;
use super::{CrawlRepository, Result};
use crate::models::CrawlUrl;

impl CrawlRepository {
    /// Add a discovered URL if not already known.
    pub fn add_url(&self, crawl_url: &CrawlUrl) -> Result<bool> {
        let conn = self.connect()?;

        let result = conn.execute(
            r#"
            INSERT OR IGNORE INTO crawl_urls (
                url, source_id, status, discovery_method, parent_url,
                discovery_context, depth, discovered_at, retry_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                crawl_url.url,
                crawl_url.source_id,
                crawl_url.status.as_str(),
                crawl_url.discovery_method.as_str(),
                crawl_url.parent_url,
                serde_json::to_string(&crawl_url.discovery_context)?,
                crawl_url.depth,
                crawl_url.discovered_at.to_rfc3339(),
                crawl_url.retry_count,
            ],
        )?;

        Ok(result > 0)
    }

    /// Get a specific URL's crawl state.
    pub fn get_url(&self, source_id: &str, url: &str) -> Result<Option<CrawlUrl>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT * FROM crawl_urls WHERE source_id = ? AND url = ?")?;
        super::super::to_option(stmt.query_row(params![source_id, url], row_to_crawl_url))
    }

    /// Check if a URL has already been discovered.
    pub fn url_exists(&self, source_id: &str, url: &str) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM crawl_urls WHERE source_id = ? AND url = ?",
            params![source_id, url],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Update an existing URL's state.
    pub fn update_url(&self, crawl_url: &CrawlUrl) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            r#"
            UPDATE crawl_urls SET
                status = ?1,
                fetched_at = ?2,
                retry_count = ?3,
                last_error = ?4,
                next_retry_at = ?5,
                etag = ?6,
                last_modified = ?7,
                content_hash = ?8,
                document_id = ?9
            WHERE source_id = ?10 AND url = ?11
            "#,
            params![
                crawl_url.status.as_str(),
                crawl_url.fetched_at.map(|dt| dt.to_rfc3339()),
                crawl_url.retry_count,
                crawl_url.last_error,
                crawl_url.next_retry_at.map(|dt| dt.to_rfc3339()),
                crawl_url.etag,
                crawl_url.last_modified,
                crawl_url.content_hash,
                crawl_url.document_id,
                crawl_url.source_id,
                crawl_url.url,
            ],
        )?;

        Ok(())
    }

    /// Mark a URL for refresh by changing its status back to 'discovered'.
    pub fn mark_url_for_refresh(&self, source_id: &str, url: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            UPDATE crawl_urls
            SET status = 'discovered'
            WHERE source_id = ? AND url = ?
            "#,
            params![source_id, url],
        )?;
        Ok(())
    }

    /// Get URLs that haven't been checked since a given time.
    pub fn get_urls_needing_refresh(
        &self,
        source_id: &str,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_urls
            WHERE source_id = ?
            AND status = 'fetched'
            AND fetched_at < ?
            ORDER BY fetched_at ASC
            LIMIT ?
        "#,
        )?;

        let urls = stmt
            .query_map(params![source_id, older_than.to_rfc3339(), limit], |row| {
                row_to_crawl_url(row)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(urls)
    }

    /// Get recently fetched URLs (successfully completed).
    pub fn get_recent_downloads(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_urls
            WHERE (?1 IS NULL OR source_id = ?1) AND status = 'fetched'
            ORDER BY fetched_at DESC
            LIMIT ?2
        "#,
        )?;
        let urls = stmt
            .query_map(params![source_id, limit], row_to_crawl_url)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(urls)
    }

    /// Get failed URLs with their error messages.
    pub fn get_failed_urls(&self, source_id: Option<&str>, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_urls
            WHERE (?1 IS NULL OR source_id = ?1) AND status IN ('failed', 'exhausted')
            ORDER BY fetched_at DESC NULLS LAST
            LIMIT ?2
        "#,
        )?;
        let urls = stmt
            .query_map(params![source_id, limit], row_to_crawl_url)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(urls)
    }

    /// Clear pending crawl state for a source (keeps fetched URLs).
    pub fn clear_source(&self, source_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM crawl_urls WHERE source_id = ? AND status IN ('discovered', 'fetching', 'failed')",
            params![source_id]
        )?;
        conn.execute(
            "DELETE FROM crawl_requests WHERE source_id = ?",
            params![source_id],
        )?;
        Ok(())
    }

    /// Clear ALL crawl state for a source (including fetched URLs).
    pub fn clear_source_all(&self, source_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM crawl_urls WHERE source_id = ?",
            params![source_id],
        )?;
        conn.execute(
            "DELETE FROM crawl_requests WHERE source_id = ?",
            params![source_id],
        )?;
        conn.execute(
            "DELETE FROM crawl_config WHERE source_id = ?",
            params![source_id],
        )?;
        Ok(())
    }
}
