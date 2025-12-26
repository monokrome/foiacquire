//! Database context for managing connection pools and repository access.
//!
//! Provides a unified entry point for database operations, eliminating
//! the need for individual pool creation in each command.

use sqlx::sqlite::SqlitePool;
use std::path::{Path, PathBuf};

use super::{
    create_pool, create_pool_from_url, AsyncConfigHistoryRepository, AsyncCrawlRepository,
    AsyncDocumentRepository, AsyncSourceRepository, Result,
};

/// Database context that manages the connection pool and provides repository access.
///
/// This is the primary interface for database operations. Create one context
/// per command or service, then use it to access all repositories.
///
/// # Example
/// ```ignore
/// let ctx = DbContext::new(&settings.database_path(), &settings.documents_dir).await?;
/// let sources = ctx.sources().get_all().await?;
/// let docs = ctx.documents().get_by_source("my-source").await?;
/// ```
#[derive(Clone)]
pub struct DbContext {
    pool: SqlitePool,
    documents_dir: PathBuf,
}

impl DbContext {
    /// Create a new database context from a file path.
    ///
    /// This opens a connection pool to the database and runs any pending migrations.
    pub async fn new(db_path: &Path, documents_dir: &Path) -> Result<Self> {
        let pool = create_pool(db_path).await?;

        // Run schema initialization for all tables
        Self::init_schema(&pool).await?;

        Ok(Self {
            pool,
            documents_dir: documents_dir.to_path_buf(),
        })
    }

    /// Create a new database context from a database URL.
    ///
    /// Supports SQLite URLs like `sqlite:path/to/db.sqlite`.
    /// The URL can be set via the DATABASE_URL environment variable.
    pub async fn from_url(database_url: &str, documents_dir: &Path) -> Result<Self> {
        let pool = create_pool_from_url(database_url).await?;

        // Run schema initialization for all tables
        Self::init_schema(&pool).await?;

        Ok(Self {
            pool,
            documents_dir: documents_dir.to_path_buf(),
        })
    }

    /// Create a context with an existing pool (for sharing across services).
    pub fn with_pool(pool: SqlitePool, documents_dir: PathBuf) -> Self {
        Self {
            pool,
            documents_dir,
        }
    }

    /// Get the underlying connection pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Get a source repository.
    pub fn sources(&self) -> AsyncSourceRepository {
        AsyncSourceRepository::new(self.pool.clone())
    }

    /// Get a crawl repository.
    pub fn crawl(&self) -> AsyncCrawlRepository {
        AsyncCrawlRepository::new(self.pool.clone())
    }

    /// Get a document repository.
    pub fn documents(&self) -> AsyncDocumentRepository {
        AsyncDocumentRepository::new(self.pool.clone(), self.documents_dir.clone())
    }

    /// Get a config history repository.
    pub fn config_history(&self) -> AsyncConfigHistoryRepository {
        AsyncConfigHistoryRepository::new(self.pool.clone())
    }

    /// Initialize all database schemas.
    async fn init_schema(pool: &SqlitePool) -> Result<()> {
        // Sources table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                last_scraped TEXT
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Documents table
        sqlx::query(
            r#"
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
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Document versions table
        sqlx::query(
            r#"
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
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Document pages table
        sqlx::query(
            r#"
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
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Crawl URLs table
        sqlx::query(
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
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Crawl requests table
        sqlx::query(
            r#"
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
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Crawl config table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS crawl_config (
                source_id TEXT PRIMARY KEY,
                config_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Config history table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS config_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                format TEXT NOT NULL DEFAULT 'json',
                hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Virtual files table
        sqlx::query(
            r#"
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
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Rate limit state table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS rate_limit_state (
                domain TEXT PRIMARY KEY,
                current_delay_ms INTEGER NOT NULL,
                in_backoff INTEGER NOT NULL DEFAULT 0,
                total_requests INTEGER NOT NULL DEFAULT 0,
                rate_limit_hits INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Indexes
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id);
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url);
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_document_versions_doc ON document_versions(document_id);
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_source_status ON crawl_urls(source_id, status);
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_crawl_urls_parent ON crawl_urls(parent_url);
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_crawl_requests_source ON crawl_requests(source_id, request_at);
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_config_history_hash ON config_history(hash);
            "#,
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Get list of all tables in the database.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }
}
