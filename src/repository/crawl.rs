//! Crawl state repository for tracking URL discovery and request history.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::Result;
use crate::models::{CrawlRequest, CrawlState, CrawlUrl, DiscoveryMethod, RequestStats, UrlStatus};

/// SQLite-backed repository for crawl state.
pub struct CrawlRepository {
    db_path: PathBuf,
}

impl CrawlRepository {
    /// Create a new crawl repository.
    pub fn new(db_path: &Path) -> Result<Self> {
        let repo = Self {
            db_path: db_path.to_path_buf(),
        };
        repo.init_schema()?;
        Ok(repo)
    }

    fn connect(&self) -> Result<Connection> {
        super::connect(&self.db_path)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
            -- URLs discovered during crawling
            CREATE TABLE IF NOT EXISTS crawl_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                source_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'discovered',

                -- Discovery context
                discovery_method TEXT NOT NULL DEFAULT 'seed',
                parent_url TEXT,
                discovery_context TEXT NOT NULL DEFAULT '{}',
                depth INTEGER NOT NULL DEFAULT 0,

                -- Timing
                discovered_at TEXT NOT NULL,
                fetched_at TEXT,

                -- Retry tracking
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,

                -- HTTP caching
                etag TEXT,
                last_modified TEXT,

                -- Content linkage
                content_hash TEXT,
                document_id TEXT,

                UNIQUE(source_id, url)
            );

            -- HTTP request audit log
            CREATE TABLE IF NOT EXISTS crawl_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                url TEXT NOT NULL,
                method TEXT NOT NULL DEFAULT 'GET',

                -- Request
                request_headers TEXT NOT NULL DEFAULT '{}',
                request_at TEXT NOT NULL,

                -- Response
                response_status INTEGER,
                response_headers TEXT NOT NULL DEFAULT '{}',
                response_at TEXT,
                response_size INTEGER,

                -- Timing
                duration_ms INTEGER,

                -- Error
                error TEXT,

                -- Conditional request tracking
                was_conditional INTEGER NOT NULL DEFAULT 0,
                was_not_modified INTEGER NOT NULL DEFAULT 0
            );

            -- Config hash tracking to detect when scraper config changes
            CREATE TABLE IF NOT EXISTS crawl_config (
                source_id TEXT PRIMARY KEY,
                config_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            -- Indexes for efficient queries
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_source_status
                ON crawl_urls(source_id, status);
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_parent
                ON crawl_urls(parent_url);
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_discovered
                ON crawl_urls(discovered_at);
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_retry
                ON crawl_urls(next_retry_at) WHERE status = 'failed';
            CREATE INDEX IF NOT EXISTS idx_crawl_requests_source
                ON crawl_requests(source_id, request_at);
            CREATE INDEX IF NOT EXISTS idx_crawl_requests_url
                ON crawl_requests(url);
        "#,
        )?;
        Ok(())
    }

    /// Check if the scraper config has changed since last crawl.
    /// Returns (has_changed, should_clear) - should_clear is true if there are pending URLs.
    pub fn check_config_changed(
        &self,
        source_id: &str,
        config: &impl serde::Serialize,
    ) -> Result<(bool, bool)> {
        let conn = self.connect()?;

        // Compute hash of current config
        let config_json = serde_json::to_string(config).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(config_json.as_bytes());
        let current_hash = hex::encode(hasher.finalize());

        // Get stored hash
        let stored_hash: Option<String> = conn
            .query_row(
                "SELECT config_hash FROM crawl_config WHERE source_id = ?",
                params![source_id],
                |row| row.get(0),
            )
            .ok();

        let has_changed = stored_hash.as_ref() != Some(&current_hash);

        // Check if there are pending URLs that would be affected
        let pending_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM crawl_urls WHERE source_id = ? AND status IN ('discovered', 'fetching')",
            params![source_id],
            |row| row.get(0),
        ).unwrap_or(0);

        Ok((has_changed, has_changed && pending_count > 0))
    }

    /// Store the current config hash for a source.
    pub fn store_config_hash(&self, source_id: &str, config: &impl serde::Serialize) -> Result<()> {
        let conn = self.connect()?;

        let config_json = serde_json::to_string(config).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(config_json.as_bytes());
        let config_hash = hex::encode(hasher.finalize());

        conn.execute(
            "INSERT OR REPLACE INTO crawl_config (source_id, config_hash, updated_at) VALUES (?, ?, ?)",
            params![source_id, config_hash, Utc::now().to_rfc3339()],
        )?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // URL Management
    // -------------------------------------------------------------------------

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

        let crawl_url = stmt.query_row(params![source_id, url], |row| self.row_to_crawl_url(row));

        match crawl_url {
            Ok(u) => Ok(Some(u)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
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
    /// This keeps the etag/last_modified for conditional GET requests.
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

    /// Get URLs that need to be fetched.
    pub fn get_pending_urls(&self, source_id: &str, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_urls
            WHERE source_id = ?
            AND status IN ('discovered', 'fetching')
            ORDER BY depth ASC, discovered_at ASC
            LIMIT ?
        "#,
        )?;

        let urls = stmt
            .query_map(params![source_id, limit], |row| self.row_to_crawl_url(row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(urls)
    }

    /// Atomically claim a pending URL for processing.
    /// Uses BEGIN IMMEDIATE for multi-process safety.
    /// Returns None if no pending URLs are available.
    pub fn claim_pending_url(&self, source_id: Option<&str>) -> Result<Option<CrawlUrl>> {
        let conn = self.connect()?;

        // Use BEGIN IMMEDIATE for multi-process coordination
        conn.execute("BEGIN IMMEDIATE", [])?;

        let result: std::result::Result<Option<CrawlUrl>, super::RepositoryError> = (|| {
            // Find a pending URL
            let query_result = if let Some(sid) = source_id {
                conn.query_row(
                    r#"
                    SELECT * FROM crawl_urls
                    WHERE source_id = ? AND status = 'discovered'
                    ORDER BY depth ASC, discovered_at ASC
                    LIMIT 1
                    "#,
                    params![sid],
                    |row| self.row_to_crawl_url(row),
                )
            } else {
                conn.query_row(
                    r#"
                    SELECT * FROM crawl_urls
                    WHERE status = 'discovered'
                    ORDER BY depth ASC, discovered_at ASC
                    LIMIT 1
                    "#,
                    [],
                    |row| self.row_to_crawl_url(row),
                )
            };

            match query_result {
                Ok(mut crawl_url) => {
                    // Mark as fetching
                    conn.execute(
                        "UPDATE crawl_urls SET status = 'fetching' WHERE source_id = ? AND url = ?",
                        params![crawl_url.source_id, crawl_url.url],
                    )?;
                    crawl_url.status = UrlStatus::Fetching;
                    Ok(Some(crawl_url))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })();

        if result.is_ok() {
            conn.execute("COMMIT", [])?;
        } else {
            let _ = conn.execute("ROLLBACK", []);
        }

        result
    }

    /// Atomically claim multiple pending URLs for processing.
    /// Returns up to `limit` URLs, all marked as 'fetching'.
    pub fn claim_pending_urls(&self, source_id: Option<&str>, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;

        // Use BEGIN IMMEDIATE for multi-process coordination
        conn.execute("BEGIN IMMEDIATE", [])?;

        let result: std::result::Result<Vec<CrawlUrl>, super::RepositoryError> = (|| {
            // Find pending URLs
            let urls: Vec<CrawlUrl> = if let Some(sid) = source_id {
                let mut stmt = conn.prepare(
                    r#"
                    SELECT * FROM crawl_urls
                    WHERE source_id = ? AND status = 'discovered'
                    ORDER BY depth ASC, discovered_at ASC
                    LIMIT ?
                "#,
                )?;
                let collected: Vec<CrawlUrl> = stmt
                    .query_map(params![sid, limit], |row| self.row_to_crawl_url(row))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                collected
            } else {
                let mut stmt = conn.prepare(
                    r#"
                    SELECT * FROM crawl_urls
                    WHERE status = 'discovered'
                    ORDER BY depth ASC, discovered_at ASC
                    LIMIT ?
                "#,
                )?;
                let collected: Vec<CrawlUrl> = stmt
                    .query_map(params![limit], |row| self.row_to_crawl_url(row))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                collected
            };

            // Mark all as fetching
            for url in &urls {
                conn.execute(
                    "UPDATE crawl_urls SET status = 'fetching' WHERE source_id = ? AND url = ?",
                    params![url.source_id, url.url],
                )?;
            }

            Ok(urls
                .into_iter()
                .map(|mut u| {
                    u.status = UrlStatus::Fetching;
                    u
                })
                .collect())
        })();

        if result.is_ok() {
            conn.execute("COMMIT", [])?;
        } else {
            let _ = conn.execute("ROLLBACK", []);
        }

        result
    }

    /// Get failed URLs that are ready for retry.
    pub fn get_retryable_urls(&self, source_id: &str, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;
        let now = Utc::now().to_rfc3339();

        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_urls
            WHERE source_id = ?
            AND status = 'failed'
            AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY retry_count ASC, discovered_at ASC
            LIMIT ?
        "#,
        )?;

        let urls = stmt
            .query_map(params![source_id, now, limit], |row| {
                self.row_to_crawl_url(row)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(urls)
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
                self.row_to_crawl_url(row)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(urls)
    }

    // -------------------------------------------------------------------------
    // Request Logging
    // -------------------------------------------------------------------------

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

        let request = stmt.query_row(params![source_id, url], |row| {
            self.row_to_crawl_request(row)
        });

        match request {
            Ok(r) => Ok(Some(r)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // -------------------------------------------------------------------------
    // Crawl State Analysis
    // -------------------------------------------------------------------------

    /// Get aggregate crawl state for a source.
    pub fn get_crawl_state(&self, source_id: &str) -> Result<CrawlState> {
        let conn = self.connect()?;

        // Count URLs by status
        let mut status_counts: HashMap<String, u64> = HashMap::new();
        {
            let mut stmt = conn.prepare(
                r#"
                SELECT status, COUNT(*) as count
                FROM crawl_urls WHERE source_id = ?
                GROUP BY status
            "#,
            )?;

            let rows = stmt.query_map(params![source_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?;

            for row in rows {
                let (status, count) = row?;
                status_counts.insert(status, count);
            }
        }

        // Get timing info
        let timing: (Option<String>, Option<String>, Option<String>) = conn.query_row(
            r#"
            SELECT
                MIN(discovered_at) as first_discovered,
                MAX(fetched_at) as last_fetched,
                MIN(CASE WHEN status IN ('discovered', 'fetching')
                    THEN discovered_at END) as oldest_pending
            FROM crawl_urls WHERE source_id = ?
            "#,
            params![source_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;

        // Check for unexplored branches
        let unexplored_count: i64 = conn.query_row(
            r#"
            SELECT COUNT(*) FROM crawl_urls u1
            WHERE u1.source_id = ?
            AND u1.status = 'fetched'
            AND u1.discovery_method IN ('html_link', 'pagination', 'api_result')
            AND NOT EXISTS (
                SELECT 1 FROM crawl_urls u2
                WHERE u2.source_id = u1.source_id
                AND u2.parent_url = u1.url
            )
            AND u1.depth < 10
            "#,
            params![source_id],
            |row| row.get(0),
        )?;

        let urls_discovered: u64 = status_counts.values().sum();
        let urls_fetched = *status_counts.get("fetched").unwrap_or(&0);
        let urls_failed = status_counts.get("failed").unwrap_or(&0)
            + status_counts.get("exhausted").unwrap_or(&0);
        let urls_pending = status_counts.get("discovered").unwrap_or(&0)
            + status_counts.get("fetching").unwrap_or(&0);

        Ok(CrawlState {
            source_id: source_id.to_string(),
            last_crawl_started: timing
                .0
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            last_crawl_completed: if urls_pending == 0 {
                timing
                    .1
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
            } else {
                None
            },
            urls_discovered,
            urls_fetched,
            urls_failed,
            urls_pending,
            has_pending_urls: urls_pending > 0,
            has_unexplored_branches: unexplored_count > 0,
            oldest_pending_url: timing
                .2
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
        })
    }

    /// Count crawl URLs for a source.
    pub fn count_by_source(&self, source_id: &str) -> Result<u64> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM crawl_urls WHERE source_id = ?",
            params![source_id],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get request statistics for a source.
    pub fn get_request_stats(&self, source_id: &str) -> Result<RequestStats> {
        let conn = self.connect()?;

        let stats = conn.query_row(
            r#"
            SELECT
                COUNT(*) as total_requests,
                SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END) as success_200,
                SUM(CASE WHEN response_status = 304 THEN 1 ELSE 0 END) as not_modified_304,
                SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END) as errors,
                SUM(was_conditional) as conditional_requests,
                AVG(duration_ms) as avg_duration_ms,
                SUM(response_size) as total_bytes
            FROM crawl_requests
            WHERE source_id = ?
            "#,
            params![source_id],
            |row| {
                Ok(RequestStats {
                    total_requests: row.get::<_, i64>(0)? as u64,
                    success_200: row.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
                    not_modified_304: row.get::<_, Option<i64>>(2)?.unwrap_or(0) as u64,
                    errors: row.get::<_, Option<i64>>(3)?.unwrap_or(0) as u64,
                    conditional_requests: row.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                    avg_duration_ms: row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                    total_bytes: row.get::<_, Option<i64>>(6)?.unwrap_or(0) as u64,
                })
            },
        )?;

        Ok(stats)
    }

    /// Clear pending crawl state for a source (keeps fetched URLs).
    pub fn clear_source(&self, source_id: &str) -> Result<()> {
        let conn = self.connect()?;
        // Only clear pending/discovered URLs, keep fetched ones to avoid re-downloading
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
    /// Use this for a complete reset.
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

    /// Get recently fetched URLs (successfully completed).
    pub fn get_recent_downloads(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            let mut stmt = conn.prepare(
                r#"
                SELECT * FROM crawl_urls
                WHERE source_id = ? AND status = 'fetched'
                ORDER BY fetched_at DESC
                LIMIT ?
            "#,
            )?;
            let urls = stmt
                .query_map(params![sid, limit], |row| self.row_to_crawl_url(row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(urls)
        } else {
            let mut stmt = conn.prepare(
                r#"
                SELECT * FROM crawl_urls
                WHERE status = 'fetched'
                ORDER BY fetched_at DESC
                LIMIT ?
            "#,
            )?;
            let urls = stmt
                .query_map(params![limit], |row| self.row_to_crawl_url(row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(urls)
        }
    }

    /// Get failed URLs with their error messages.
    pub fn get_failed_urls(&self, source_id: Option<&str>, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            let mut stmt = conn.prepare(
                r#"
                SELECT * FROM crawl_urls
                WHERE source_id = ? AND status IN ('failed', 'exhausted')
                ORDER BY fetched_at DESC NULLS LAST
                LIMIT ?
            "#,
            )?;
            let urls = stmt
                .query_map(params![sid, limit], |row| self.row_to_crawl_url(row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(urls)
        } else {
            let mut stmt = conn.prepare(
                r#"
                SELECT * FROM crawl_urls
                WHERE status IN ('failed', 'exhausted')
                ORDER BY fetched_at DESC NULLS LAST
                LIMIT ?
            "#,
            )?;
            let urls = stmt
                .query_map(params![limit], |row| self.row_to_crawl_url(row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(urls)
        }
    }

    /// Get aggregate stats across all sources.
    pub fn get_all_stats(&self) -> Result<HashMap<String, CrawlState>> {
        let conn = self.connect()?;

        // Get all unique source IDs
        let mut stmt = conn.prepare("SELECT DISTINCT source_id FROM crawl_urls")?;
        let source_ids: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut stats = HashMap::new();
        for source_id in source_ids {
            if let Ok(state) = self.get_crawl_state(&source_id) {
                stats.insert(source_id, state);
            }
        }

        Ok(stats)
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn row_to_crawl_url(&self, row: &rusqlite::Row) -> rusqlite::Result<CrawlUrl> {
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

    fn row_to_crawl_request(&self, row: &rusqlite::Row) -> rusqlite::Result<CrawlRequest> {
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
}
