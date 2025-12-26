//! Crawl state repository for tracking URL discovery and request history.

#![allow(dead_code)]

mod claim;
mod helpers;
mod request;
mod state;
mod url;

use chrono::Utc;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use super::Result;

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

    pub(crate) fn connect(&self) -> Result<Connection> {
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
        let pending_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM crawl_urls WHERE source_id = ? AND status IN ('discovered', 'fetching')",
                params![source_id],
                |row| row.get(0),
            )
            .unwrap_or(0);

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
}
