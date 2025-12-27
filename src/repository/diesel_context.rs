//! Diesel database context for managing connection pools and repository access.
//!
//! Provides a unified entry point for database operations using Diesel ORM.
//! Uses diesel-async's SyncConnectionWrapper for SQLite async support.

use std::path::{Path, PathBuf};

use diesel_async::SimpleAsyncConnection;

use super::diesel_config_history::DieselConfigHistoryRepository;
use super::diesel_crawl::DieselCrawlRepository;
use super::diesel_document::DieselDocumentRepository;
use super::diesel_pool::{AsyncSqlitePool, DieselError};
use super::diesel_source::DieselSourceRepository;

/// Diesel database context that manages the connection pool and provides repository access.
///
/// This is the primary interface for Diesel-based database operations. Create one context
/// per command or service, then use it to access all repositories.
///
/// # Example
/// ```ignore
/// let ctx = DieselDbContext::new(&db_path, &documents_dir)?;
/// let sources = ctx.sources().get_all().await?;
/// let docs = ctx.documents().get_by_source("my-source").await?;
/// ```
#[derive(Clone)]
pub struct DieselDbContext {
    pool: AsyncSqlitePool,
    documents_dir: PathBuf,
}

impl DieselDbContext {
    /// Create a new database context from a file path.
    pub fn new(db_path: &Path, documents_dir: &Path) -> Self {
        let pool = AsyncSqlitePool::from_path(db_path, 10);
        Self {
            pool,
            documents_dir: documents_dir.to_path_buf(),
        }
    }

    /// Create a new database context from a database URL.
    ///
    /// Supports SQLite URLs like `sqlite:path/to/db.sqlite`.
    pub fn from_url(database_url: &str, documents_dir: &Path) -> Self {
        let pool = AsyncSqlitePool::new(database_url, 10);
        Self {
            pool,
            documents_dir: documents_dir.to_path_buf(),
        }
    }

    /// Create a context with an existing pool.
    #[allow(dead_code)]
    pub fn with_pool(pool: AsyncSqlitePool, documents_dir: PathBuf) -> Self {
        Self {
            pool,
            documents_dir,
        }
    }

    /// Get the underlying connection pool.
    pub fn pool(&self) -> &AsyncSqlitePool {
        &self.pool
    }

    /// Get a source repository.
    pub fn sources(&self) -> DieselSourceRepository {
        DieselSourceRepository::new(self.pool.clone())
    }

    /// Get a crawl repository.
    pub fn crawl(&self) -> DieselCrawlRepository {
        DieselCrawlRepository::new(self.pool.clone())
    }

    /// Get a document repository.
    pub fn documents(&self) -> DieselDocumentRepository {
        DieselDocumentRepository::new(self.pool.clone(), self.documents_dir.clone())
    }

    /// Get a config history repository.
    pub fn config_history(&self) -> DieselConfigHistoryRepository {
        DieselConfigHistoryRepository::new(self.pool.clone())
    }

    /// Initialize all database schemas.
    ///
    /// This creates the necessary tables if they don't exist.
    pub async fn init_schema(&self) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        conn.batch_execute(
            r#"
            -- Sources table
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                last_scraped TEXT
            );

            -- Documents table
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                url TEXT NOT NULL,
                title TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (source_id) REFERENCES sources(id)
            );

            -- Document versions table
            CREATE TABLE IF NOT EXISTS document_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                file_path TEXT,
                content_hash TEXT,
                mime_type TEXT,
                file_size INTEGER,
                fetched_at TEXT NOT NULL,
                UNIQUE(document_id, version),
                FOREIGN KEY (document_id) REFERENCES documents(id)
            );

            -- Document pages table
            CREATE TABLE IF NOT EXISTS document_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                page_number INTEGER NOT NULL,
                text_content TEXT,
                ocr_text TEXT,
                has_images INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                UNIQUE(document_id, version, page_number),
                FOREIGN KEY (document_id) REFERENCES documents(id)
            );

            -- Crawl URLs table
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

            -- Crawl requests table
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

            -- Crawl config table
            CREATE TABLE IF NOT EXISTS crawl_config (
                source_id TEXT PRIMARY KEY,
                config_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            -- Config history table
            CREATE TABLE IF NOT EXISTS config_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                format TEXT NOT NULL DEFAULT 'json',
                hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            -- Virtual files table
            CREATE TABLE IF NOT EXISTS virtual_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                mime_type TEXT,
                file_size INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                ocr_text TEXT,
                UNIQUE(document_id, version, path),
                FOREIGN KEY (document_id) REFERENCES documents(id)
            );

            -- Rate limit state table
            CREATE TABLE IF NOT EXISTS rate_limit_state (
                domain TEXT PRIMARY KEY,
                current_delay_ms INTEGER NOT NULL,
                in_backoff INTEGER NOT NULL DEFAULT 0,
                total_requests INTEGER NOT NULL DEFAULT 0,
                rate_limit_hits INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            -- Indexes
            CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id);
            CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url);
            CREATE INDEX IF NOT EXISTS idx_document_versions_doc ON document_versions(document_id);
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_source_status ON crawl_urls(source_id, status);
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_parent ON crawl_urls(parent_url);
            CREATE INDEX IF NOT EXISTS idx_crawl_requests_source ON crawl_requests(source_id, request_at);
            CREATE INDEX IF NOT EXISTS idx_config_history_hash ON config_history(hash);
            "#,
        )
        .await
    }

    /// Get list of all tables in the database.
    #[allow(dead_code)]
    pub async fn list_tables(&self) -> Result<Vec<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        let rows: Vec<TableName> = diesel_async::RunQueryDsl::load(
            diesel::sql_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            ),
            &mut conn,
        )
        .await?;

        Ok(rows.into_iter().map(|r| r.name).collect())
    }
}

#[derive(diesel::QueryableByName)]
#[allow(dead_code)]
struct TableName {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_diesel_context() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let docs_dir = dir.path().join("docs");

        let ctx = DieselDbContext::new(&db_path, &docs_dir);

        // Initialize schema
        ctx.init_schema().await.unwrap();

        // List tables
        let tables = ctx.list_tables().await.unwrap();
        assert!(tables.contains(&"sources".to_string()));
        assert!(tables.contains(&"documents".to_string()));
        assert!(tables.contains(&"crawl_urls".to_string()));

        // Test source repository
        let sources = ctx.sources();
        let all_sources = sources.get_all().await.unwrap();
        assert!(all_sources.is_empty());
    }
}
