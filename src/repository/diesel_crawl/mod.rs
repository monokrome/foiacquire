//! Diesel-based crawl repository.
//!
//! Uses diesel-async for async database support. Works with both SQLite and PostgreSQL.
//!
//! Split into submodules:
//! - `mod.rs` (this file): Main struct, From impls, types
//! - `urls.rs`: URL CRUD operations
//! - `queue.rs`: Queue/claiming operations
//! - `requests.rs`: Request logging
//! - `stats.rs`: Statistics and analytics
//! - `config.rs`: Config hash management
//! - `cleanup.rs`: Cleanup operations

mod cleanup;
mod config;
mod queue;
mod requests;
mod stats;
mod urls;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use diesel::prelude::*;

use super::diesel_models::{CrawlRequestRecord, CrawlUrlRecord};
use super::pool::DbPool;
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{CrawlRequest, CrawlUrl, DiscoveryMethod, UrlStatus};

/// Common fields for crawl URL database records.
trait CrawlUrlFields {
    fn url(&self) -> &str;
    fn source_id(&self) -> &str;
    fn status(&self) -> &str;
    fn discovery_method(&self) -> &str;
    fn parent_url(&self) -> Option<&str>;
    fn discovery_context(&self) -> &str;
    fn depth(&self) -> i32;
    fn discovered_at(&self) -> &str;
    fn fetched_at(&self) -> Option<&str>;
    fn retry_count(&self) -> i32;
    fn last_error(&self) -> Option<&str>;
    fn next_retry_at(&self) -> Option<&str>;
    fn etag(&self) -> Option<&str>;
    fn last_modified(&self) -> Option<&str>;
    fn content_hash(&self) -> Option<&str>;
    fn document_id(&self) -> Option<&str>;
}

/// Convert any crawl URL record to a CrawlUrl model.
fn crawl_url_from_record<T: CrawlUrlFields>(record: &T) -> CrawlUrl {
    let discovery_context: HashMap<String, serde_json::Value> =
        serde_json::from_str(record.discovery_context()).unwrap_or_default();

    CrawlUrl {
        url: record.url().to_string(),
        source_id: record.source_id().to_string(),
        status: UrlStatus::from_str(record.status()).unwrap_or(UrlStatus::Discovered),
        discovery_method: DiscoveryMethod::from_str(record.discovery_method())
            .unwrap_or(DiscoveryMethod::Seed),
        parent_url: record.parent_url().map(ToString::to_string),
        discovery_context,
        depth: record.depth() as u32,
        discovered_at: parse_datetime(record.discovered_at()),
        fetched_at: record.fetched_at().map(parse_datetime),
        retry_count: record.retry_count() as u32,
        last_error: record.last_error().map(ToString::to_string),
        next_retry_at: record.next_retry_at().map(parse_datetime),
        etag: record.etag().map(ToString::to_string),
        last_modified: record.last_modified().map(ToString::to_string),
        content_hash: record.content_hash().map(ToString::to_string),
        document_id: record.document_id().map(ToString::to_string),
    }
}

impl CrawlUrlFields for CrawlUrlRecord {
    fn url(&self) -> &str {
        &self.url
    }
    fn source_id(&self) -> &str {
        &self.source_id
    }
    fn status(&self) -> &str {
        &self.status
    }
    fn discovery_method(&self) -> &str {
        &self.discovery_method
    }
    fn parent_url(&self) -> Option<&str> {
        self.parent_url.as_deref()
    }
    fn discovery_context(&self) -> &str {
        &self.discovery_context
    }
    fn depth(&self) -> i32 {
        self.depth
    }
    fn discovered_at(&self) -> &str {
        &self.discovered_at
    }
    fn fetched_at(&self) -> Option<&str> {
        self.fetched_at.as_deref()
    }
    fn retry_count(&self) -> i32 {
        self.retry_count
    }
    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
    fn next_retry_at(&self) -> Option<&str> {
        self.next_retry_at.as_deref()
    }
    fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
    fn last_modified(&self) -> Option<&str> {
        self.last_modified.as_deref()
    }
    fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
    fn document_id(&self) -> Option<&str> {
        self.document_id.as_deref()
    }
}

/// Convert a database record to a domain model.
impl From<CrawlUrlRecord> for CrawlUrl {
    fn from(record: CrawlUrlRecord) -> Self {
        crawl_url_from_record(&record)
    }
}

impl From<CrawlRequestRecord> for CrawlRequest {
    fn from(record: CrawlRequestRecord) -> Self {
        CrawlRequest {
            id: Some(record.id as i64),
            source_id: record.source_id,
            url: record.url,
            method: record.method,
            request_headers: serde_json::from_str(&record.request_headers).unwrap_or_default(),
            request_at: parse_datetime(&record.request_at),
            response_status: record.response_status.map(|s| s as u16),
            response_headers: serde_json::from_str(&record.response_headers).unwrap_or_default(),
            response_at: parse_datetime_opt(record.response_at),
            response_size: record.response_size.map(|s| s as u64),
            duration_ms: record.duration_ms.map(|d| d as u64),
            error: record.error,
            was_conditional: record.was_conditional != 0,
            was_not_modified: record.was_not_modified != 0,
        }
    }
}

/// Diesel-based crawl repository with compile-time query checking.
#[derive(Clone)]
pub struct DieselCrawlRepository {
    pool: DbPool,
}

impl DieselCrawlRepository {
    /// Create a new Diesel crawl repository.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

/// Crawl state statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrawlState {
    pub urls_discovered: u64,
    pub urls_fetched: u64,
    pub urls_pending: u64,
    pub urls_failed: u64,
    pub has_pending_urls: bool,
    pub last_crawl_started: Option<String>,
    pub last_crawl_completed: Option<String>,
}

impl CrawlState {
    /// Check if there's resumable work.
    pub fn needs_resume(&self) -> bool {
        self.has_pending_urls || self.urls_pending > 0
    }

    /// Check if the crawl is complete (fetched > 0 and no pending work).
    pub fn is_complete(&self) -> bool {
        self.urls_fetched > 0 && !self.needs_resume()
    }
}

/// Request statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestStats {
    pub success_200: u64,
    pub not_modified_304: u64,
    pub errors: u64,
    pub avg_duration_ms: u64,
    pub total_bytes: u64,
    pub total_requests: u64,
}

/// Combined crawl statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrawlStats {
    pub crawl_state: CrawlState,
    pub request_stats: RequestStats,
    /// Convenience accessors for urls_pending (delegates to crawl_state)
    pub urls_pending: u64,
    pub urls_discovered: u64,
    pub urls_fetched: u64,
    pub urls_failed: u64,
}

// Helper struct for SQL query results
#[derive(QueryableByName)]
pub(crate) struct StatusCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub status: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub count: i64,
}

#[derive(QueryableByName)]
#[allow(dead_code)]
pub(crate) struct LastInsertRowId {
    #[diesel(sql_type = diesel::sql_types::BigInt, column_name = "last_insert_rowid()")]
    pub id: i64,
}

#[derive(QueryableByName)]
#[allow(dead_code)]
pub(crate) struct LastInsertId {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub id: i32,
}

/// Raw crawl URL record for QueryableByName (used with sql_query).
#[derive(QueryableByName, Debug)]
pub(crate) struct CrawlUrlRecordRaw {
    #[allow(dead_code)]
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub url: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub source_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub status: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub discovery_method: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub parent_url: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub discovery_context: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub depth: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub discovered_at: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub fetched_at: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub retry_count: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub last_error: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub next_retry_at: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub etag: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub last_modified: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub content_hash: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub document_id: Option<String>,
}

impl CrawlUrlFields for CrawlUrlRecordRaw {
    fn url(&self) -> &str {
        &self.url
    }
    fn source_id(&self) -> &str {
        &self.source_id
    }
    fn status(&self) -> &str {
        &self.status
    }
    fn discovery_method(&self) -> &str {
        &self.discovery_method
    }
    fn parent_url(&self) -> Option<&str> {
        self.parent_url.as_deref()
    }
    fn discovery_context(&self) -> &str {
        &self.discovery_context
    }
    fn depth(&self) -> i32 {
        self.depth
    }
    fn discovered_at(&self) -> &str {
        &self.discovered_at
    }
    fn fetched_at(&self) -> Option<&str> {
        self.fetched_at.as_deref()
    }
    fn retry_count(&self) -> i32 {
        self.retry_count
    }
    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
    fn next_retry_at(&self) -> Option<&str> {
        self.next_retry_at.as_deref()
    }
    fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
    fn last_modified(&self) -> Option<&str> {
        self.last_modified.as_deref()
    }
    fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
    fn document_id(&self) -> Option<&str> {
        self.document_id.as_deref()
    }
}

impl From<CrawlUrlRecordRaw> for CrawlUrl {
    fn from(record: CrawlUrlRecordRaw) -> Self {
        crawl_url_from_record(&record)
    }
}

#[cfg(test)]
mod tests {
    use super::super::pool::SqlitePool;
    use super::*;
    use diesel_async::SimpleAsyncConnection;
    use tempfile::tempdir;

    async fn setup_test_db() -> (DbPool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let sqlite_pool = SqlitePool::from_path(&db_path);
        let mut conn = sqlite_pool.get().await.unwrap();

        conn.batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS crawl_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                source_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'discovered',
                discovery_method TEXT NOT NULL DEFAULT 'seed',
                parent_url TEXT,
                discovery_context TEXT NOT NULL DEFAULT '{}',
                depth INTEGER NOT NULL DEFAULT 0,
                discovered_at TEXT NOT NULL,
                fetched_at TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,
                etag TEXT,
                last_modified TEXT,
                content_hash TEXT,
                document_id TEXT,
                UNIQUE(source_id, url)
            );

            CREATE TABLE IF NOT EXISTS crawl_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                url TEXT NOT NULL,
                method TEXT NOT NULL DEFAULT 'GET',
                request_headers TEXT NOT NULL DEFAULT '{}',
                request_at TEXT NOT NULL,
                response_status INTEGER,
                response_headers TEXT NOT NULL DEFAULT '{}',
                response_at TEXT,
                response_size INTEGER,
                duration_ms INTEGER,
                error TEXT,
                was_conditional INTEGER NOT NULL DEFAULT 0,
                was_not_modified INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS crawl_config (
                source_id TEXT PRIMARY KEY,
                config_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            "#,
        )
        .await
        .unwrap();

        (DbPool::Sqlite(sqlite_pool), dir)
    }

    #[tokio::test]
    async fn test_crawl_url_crud() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselCrawlRepository::new(pool);

        // Create a crawl URL
        let crawl_url = CrawlUrl::new(
            "https://example.com/page".to_string(),
            "test-source".to_string(),
            DiscoveryMethod::Seed,
            None,
            0,
        );

        // Add URL
        let added = repo.add_url(&crawl_url).await.unwrap();
        assert!(added);

        // Check exists
        assert!(repo
            .url_exists("test-source", "https://example.com/page")
            .await
            .unwrap());

        // Try to add duplicate
        let duplicate = repo.add_url(&crawl_url).await.unwrap();
        assert!(!duplicate);

        // Get URL
        let fetched = repo
            .get_url("test-source", "https://example.com/page")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.url, "https://example.com/page");
        assert_eq!(fetched.status, UrlStatus::Discovered);

        // Get pending URLs
        let pending = repo.get_pending_urls("test-source", 10).await.unwrap();
        assert_eq!(pending.len(), 1);

        // Count by status
        let counts = repo.count_by_status("test-source").await.unwrap();
        assert_eq!(*counts.get("discovered").unwrap_or(&0), 1);
    }

    #[tokio::test]
    async fn test_claim_pending_url() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselCrawlRepository::new(pool);

        // Add a URL
        let crawl_url = CrawlUrl::new(
            "https://example.com/claim-test".to_string(),
            "test-source".to_string(),
            DiscoveryMethod::Seed,
            None,
            0,
        );
        repo.add_url(&crawl_url).await.unwrap();

        // Claim URL
        let claimed = repo
            .claim_pending_url(Some("test-source"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.url, "https://example.com/claim-test");
        assert_eq!(claimed.status, UrlStatus::Fetching);

        // Verify no more pending
        let pending = repo.claim_pending_url(Some("test-source")).await.unwrap();
        assert!(pending.is_none());
    }

    #[tokio::test]
    async fn test_config_hash() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselCrawlRepository::new(pool);

        // Initially should indicate change
        let changed = repo
            .check_config_changed("test-source", "hash1")
            .await
            .unwrap();
        assert!(changed);

        // Store hash
        repo.store_config_hash("test-source", "hash1")
            .await
            .unwrap();

        // Now should not indicate change
        let changed = repo
            .check_config_changed("test-source", "hash1")
            .await
            .unwrap();
        assert!(!changed);

        // Different hash should indicate change
        let changed = repo
            .check_config_changed("test-source", "hash2")
            .await
            .unwrap();
        assert!(changed);
    }
}
