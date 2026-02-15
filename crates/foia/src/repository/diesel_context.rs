//! Diesel database context for managing connection pools and repository access.
//!
//! Provides a unified entry point for database operations using Diesel ORM.
//! Supports both SQLite (via SyncConnectionWrapper) and PostgreSQL backends.

use std::path::Path;

use super::diesel_config_history::DieselConfigHistoryRepository;
use super::diesel_crawl::DieselCrawlRepository;
use super::diesel_document::DieselDocumentRepository;
use super::diesel_scraper_config::DieselScraperConfigRepository;
use super::diesel_service_status::DieselServiceStatusRepository;
use super::diesel_source::DieselSourceRepository;
use super::pool::{DbPool, DieselError};
use crate::with_conn_split;

/// Diesel database context that manages the connection pool and provides repository access.
///
/// This is the primary interface for Diesel-based database operations. Create one context
/// per command or service, then use it to access all repositories.
///
/// # Example
/// ```ignore
/// let ctx = DieselDbContext::from_url("postgres://localhost/db")?;
/// let sources = ctx.sources().get_all().await?;
/// let docs = ctx.documents().get_by_source("my-source").await?;
/// ```
#[derive(Clone)]
pub struct DieselDbContext {
    pool: DbPool,
}

#[allow(dead_code)]
impl DieselDbContext {
    /// Create a new database context from a database URL.
    ///
    /// Supports:
    /// - SQLite URLs like `sqlite:path/to/db.sqlite` or just file paths
    /// - PostgreSQL URLs like `postgres://user:pass@host/db`
    pub fn from_url(database_url: &str, no_tls: bool) -> Result<Self, DieselError> {
        let pool = DbPool::from_url(database_url, no_tls)?;
        Ok(Self { pool })
    }

    /// Create a new database context from a SQLite file path.
    ///
    /// This is a convenience method that constructs a sqlite: URL from the path.
    /// For PostgreSQL or explicit URLs, use `from_url()` instead.
    pub fn from_sqlite_path(db_path: &Path) -> Result<Self, DieselError> {
        let url = format!("sqlite:{}", db_path.display());
        Self::from_url(&url, false)
    }

    /// Create a context with an existing pool.
    #[allow(dead_code)]
    pub fn with_pool(pool: DbPool) -> Self {
        Self { pool }
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
    pub fn sources(&self) -> DieselSourceRepository {
        DieselSourceRepository::new(self.pool.clone())
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

    /// Get a scraper config repository.
    pub fn scraper_configs(&self) -> DieselScraperConfigRepository {
        DieselScraperConfigRepository::new(self.pool.clone())
    }

    /// Get a service status repository.
    pub fn service_status(&self) -> DieselServiceStatusRepository {
        DieselServiceStatusRepository::new(self.pool.clone())
    }

    /// Test that the database connection works.
    ///
    /// For PostgreSQL, this validates credentials and network connectivity.
    /// For SQLite, this creates the database file if it doesn't exist.
    ///
    /// Call this early in application startup to fail fast on connection issues.
    pub async fn test_connection(&self) -> Result<(), DieselError> {
        crate::with_conn!(self.pool, _conn, Ok(()))
    }

    /// Get the current schema version from the database.
    ///
    /// Returns None if the storage_meta table doesn't exist or has no format_version entry.
    pub async fn get_schema_version(&self) -> Result<Option<String>, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct MetaValue {
            #[diesel(sql_type = diesel::sql_types::Text)]
            value: String,
        }

        let result = with_conn_split!(self.pool,
            sqlite: conn => {
                use diesel_async::RunQueryDsl;
                let result: Result<MetaValue, _> = diesel::sql_query(
                    "SELECT value FROM storage_meta WHERE key = 'format_version'"
                )
                .get_result(&mut conn)
                .await;
                result
            },
            postgres: conn => {
                use diesel_async::RunQueryDsl;
                let result: Result<MetaValue, _> = diesel::sql_query(
                    "SELECT value FROM storage_meta WHERE key = 'format_version'"
                )
                .get_result(&mut conn)
                .await;
                result
            }
        );

        match result {
            Ok(meta) => Ok(Some(meta.value)),
            Err(diesel::result::Error::NotFound) => Ok(None),
            Err(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                _,
            )) => {
                // Table doesn't exist
                Ok(None)
            }
            Err(e) => {
                // Check if it's a "no such table" error for SQLite or similar
                let err_str = e.to_string();
                if err_str.contains("no such table")
                    || err_str.contains("does not exist")
                    || err_str.contains("relation")
                {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Get list of all tables in the database.
    #[allow(dead_code)]
    pub async fn list_tables(&self) -> Result<Vec<String>, DieselError> {
        with_conn_split!(self.pool,
            sqlite: conn => {
                let rows: Vec<TableName> = diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                    ),
                    &mut conn,
                )
                .await?;
                Ok(rows.into_iter().map(|r| r.name).collect())
            },
            postgres: conn => {
                use diesel_async::RunQueryDsl;
                let rows: Vec<TableName> = diesel::sql_query(
                    "SELECT tablename as name FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
                )
                .load(&mut conn)
                .await?;
                Ok(rows.into_iter().map(|r| r.name).collect())
            }
        )
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
    use crate::repository::migrations;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_diesel_context() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Initialize schema via migrations
        let db_url = format!("sqlite:{}", db_path.display());
        migrations::run_migrations(&db_url, false).await.unwrap();

        let ctx = DieselDbContext::from_sqlite_path(&db_path).unwrap();

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
