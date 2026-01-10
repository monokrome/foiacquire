//! Database context for managing connections and repository access.
//!
//! The DbContext is the primary entry point for all database operations.
//! It holds the connection pool and provides access to all repositories.

use std::path::{Path, PathBuf};

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
        DieselCrawlRepository::new(self.pool.clone())
    }

    /// Get a document repository.
    pub fn documents(&self) -> DieselDocumentRepository {
        DieselDocumentRepository::new(self.pool.clone())
    }

    /// Get a config history repository.
    pub fn config_history(&self) -> DieselConfigHistoryRepository {
        DieselConfigHistoryRepository::new(self.pool.clone())
    }
}
