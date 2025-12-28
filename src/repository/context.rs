//! Database context for managing connections and repository access.
//!
//! The DbContext is the primary entry point for all database operations.
//! It holds the connection pool and provides access to all repositories.

use std::path::{Path, PathBuf};

use diesel_async::SimpleAsyncConnection;

use super::diesel_config_history::DieselConfigHistoryRepository;
use super::diesel_crawl::DieselCrawlRepository;
use super::diesel_document::DieselDocumentRepository;
use super::pool::{DbError, DbPool};
use super::source::SourceRepository;

/// Database context that manages the connection pool and provides repository access.
///
/// # Example
/// ```ignore
/// let ctx = DbContext::from_url("postgres://localhost/foia", docs_dir)?;
/// let sources = ctx.sources().get_all().await?;
/// ```
#[derive(Clone)]
#[allow(dead_code)]
pub struct DbContext {
    pool: DbPool,
    documents_dir: PathBuf,
}

#[allow(dead_code)]
impl DbContext {
    /// Create a context from a database file path (SQLite only).
    pub fn new(db_path: &Path, documents_dir: &Path) -> Self {
        Self {
            pool: DbPool::sqlite_from_path(db_path),
            documents_dir: documents_dir.to_path_buf(),
        }
    }

    /// Create a context from a database URL.
    ///
    /// Supports:
    /// - SQLite: file paths or `sqlite:` URLs
    /// - PostgreSQL: `postgres://` or `postgresql://` URLs
    pub fn from_url(url: &str, documents_dir: &Path) -> Result<Self, DbError> {
        Ok(Self {
            pool: DbPool::from_url(url)?,
            documents_dir: documents_dir.to_path_buf(),
        })
    }

    /// Get the underlying connection pool.
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Check if using SQLite backend.
    pub fn is_sqlite(&self) -> bool {
        self.pool.is_sqlite()
    }

    /// Check if using PostgreSQL backend.
    #[cfg(feature = "postgres")]
    pub fn is_postgres(&self) -> bool {
        self.pool.is_postgres()
    }

    /// Get a source repository.
    pub fn sources(&self) -> SourceRepository {
        SourceRepository::new(self.pool.clone())
    }

    /// Get a crawl repository.
    pub fn crawl(&self) -> DieselCrawlRepository {
        DieselCrawlRepository::new(self.pool.to_diesel_pool())
    }

    /// Get a document repository.
    pub fn documents(&self) -> DieselDocumentRepository {
        DieselDocumentRepository::new(self.pool.to_diesel_pool(), self.documents_dir.clone())
    }

    /// Get a config history repository.
    pub fn config_history(&self) -> DieselConfigHistoryRepository {
        DieselConfigHistoryRepository::new(self.pool.to_diesel_pool())
    }

    /// Initialize database schema.
    pub async fn init_schema(&self) -> Result<(), DbError> {
        crate::with_conn_split!(self.pool,
            sqlite: conn => {
                init_sqlite_schema(&mut conn).await
            },
            postgres: conn => {
                init_postgres_schema(&mut conn).await
            }
        )
    }
}

/// Initialize SQLite schema.
async fn init_sqlite_schema(conn: &mut super::pool::SqliteConn) -> Result<(), DbError> {
    conn.batch_execute(include_str!("schema_sqlite.sql")).await
}

/// Initialize PostgreSQL schema.
#[cfg(feature = "postgres")]
async fn init_postgres_schema(conn: &mut diesel_async::AsyncPgConnection) -> Result<(), DbError> {
    use diesel_async::RunQueryDsl;

    // PostgreSQL needs statements executed separately
    let statements = include_str!("schema_postgres.sql");
    for stmt in statements.split(';') {
        let stmt = stmt.trim();
        if !stmt.is_empty() && !stmt.starts_with("--") {
            diesel::sql_query(stmt).execute(conn).await?;
        }
    }
    Ok(())
}
