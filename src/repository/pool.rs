//! Unified database connection pool supporting SQLite and PostgreSQL.
//!
//! This module provides a backend-agnostic interface for database connections.
//! The actual backend is determined at runtime based on the database URL.

use std::path::Path;

use diesel::sqlite::SqliteConnection;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::AsyncConnection;

#[cfg(feature = "postgres")]
use diesel_async::pooled_connection::deadpool::Pool as DeadPool;
#[cfg(feature = "postgres")]
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
#[cfg(feature = "postgres")]
use diesel_async::AsyncPgConnection;

use super::util::to_diesel_error;

/// Diesel error type alias.
pub type DbError = diesel::result::Error;

/// Async SQLite connection type.
pub type SqliteConn = SyncConnectionWrapper<SqliteConnection>;

/// Async PostgreSQL connection type.
#[cfg(feature = "postgres")]
pub type PgConn = deadpool::managed::Object<AsyncDieselConnectionManager<AsyncPgConnection>>;

/// SQLite connection pool (lightweight - creates connections on demand).
#[derive(Clone)]
pub struct SqlitePool {
    database_url: String,
}

#[allow(dead_code)]
impl SqlitePool {
    /// Create a new SQLite pool.
    pub fn new(database_url: &str) -> Self {
        // Strip sqlite: prefix if present
        let url = database_url.strip_prefix("sqlite:").unwrap_or(database_url);
        Self {
            database_url: url.to_string(),
        }
    }

    /// Create pool from a file path.
    pub fn from_path(path: &Path) -> Self {
        Self::new(&path.display().to_string())
    }

    /// Get a connection.
    pub async fn get(&self) -> Result<SqliteConn, DbError> {
        SqliteConn::establish(&self.database_url)
            .await
            .map_err(to_diesel_error)
    }

    /// Get the database URL.
    pub fn database_url(&self) -> &str {
        &self.database_url
    }
}

/// PostgreSQL connection pool.
#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PgPool {
    pool: DeadPool<AsyncPgConnection>,
}

#[cfg(feature = "postgres")]
#[allow(dead_code)]
impl PgPool {
    /// Create a new PostgreSQL pool.
    pub fn new(database_url: &str, max_size: usize) -> Result<Self, DbError> {
        let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
        let pool = DeadPool::builder(config)
            .max_size(max_size)
            .build()
            .map_err(to_diesel_error)?;
        Ok(Self { pool })
    }

    /// Get a connection.
    pub async fn get(&self) -> Result<PgConn, DbError> {
        self.pool.get().await.map_err(to_diesel_error)
    }

    /// Get the inner deadpool pool for use with diesel_context.
    pub fn inner(&self) -> DeadPool<AsyncPgConnection> {
        self.pool.clone()
    }
}

/// Unified database pool that supports both SQLite and PostgreSQL.
#[derive(Clone)]
pub enum DbPool {
    Sqlite(SqlitePool),
    #[cfg(feature = "postgres")]
    Postgres(PgPool),
}

#[allow(dead_code)]
impl DbPool {
    /// Create a pool from a database URL.
    ///
    /// Detects the backend from the URL:
    /// - `postgres://` or `postgresql://` → PostgreSQL
    /// - Everything else → SQLite
    pub fn from_url(url: &str) -> Result<Self, DbError> {
        #[cfg(feature = "postgres")]
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return Ok(DbPool::Postgres(PgPool::new(url, 10)?));
        }

        Ok(DbPool::Sqlite(SqlitePool::new(url)))
    }

    /// Create a SQLite pool from a file path.
    pub fn sqlite_from_path(path: &Path) -> Self {
        DbPool::Sqlite(SqlitePool::from_path(path))
    }

    /// Check if this is a SQLite backend.
    pub fn is_sqlite(&self) -> bool {
        matches!(self, DbPool::Sqlite(_))
    }

    /// Check if this is a PostgreSQL backend.
    #[cfg(feature = "postgres")]
    pub fn is_postgres(&self) -> bool {
        matches!(self, DbPool::Postgres(_))
    }
}

/// Macro for running database operations on either backend.
///
/// This macro handles the connection dispatch, allowing the same Diesel DSL
/// code to run on both SQLite and PostgreSQL.
///
/// # Example
/// ```ignore
/// with_conn!(self.pool, conn => {
///     sources::table.load::<SourceRecord>(&mut conn).await
/// })
/// ```
#[macro_export]
macro_rules! with_conn {
    ($pool:expr, $conn:ident => $body:expr) => {{
        match &$pool {
            $crate::repository::pool::DbPool::Sqlite(pool) => {
                let mut $conn = pool.get().await?;
                $body
            }
            #[cfg(feature = "postgres")]
            $crate::repository::pool::DbPool::Postgres(pool) => {
                let mut $conn = pool.get().await?;
                $body
            }
        }
    }};
}

/// Macro for running database operations that need different SQL per backend.
///
/// Use this when the SQL syntax differs between SQLite and PostgreSQL.
///
/// # Example
/// ```ignore
/// with_conn_split!(self.pool,
///     sqlite: conn => {
///         diesel::replace_into(table).values(...).execute(&mut conn).await
///     },
///     postgres: conn => {
///         diesel::insert_into(table).values(...).on_conflict(...).execute(&mut conn).await
///     }
/// )
/// ```
#[macro_export]
macro_rules! with_conn_split {
    ($pool:expr, sqlite: $sqlite_conn:ident => $sqlite_body:expr, postgres: $pg_conn:ident => $pg_body:expr) => {{
        match &$pool {
            $crate::repository::pool::DbPool::Sqlite(pool) => {
                let mut $sqlite_conn = pool.get().await?;
                $sqlite_body
            }
            #[cfg(feature = "postgres")]
            $crate::repository::pool::DbPool::Postgres(pool) => {
                let mut $pg_conn = pool.get().await?;
                $pg_body
            }
        }
    }};
}

#[allow(unused_imports)]
pub use with_conn;
#[allow(unused_imports)]
pub use with_conn_split;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_detection() {
        // SQLite paths
        assert!(DbPool::from_url("/path/to/db.sqlite").unwrap().is_sqlite());
        assert!(DbPool::from_url("sqlite:/path/to/db").unwrap().is_sqlite());

        // PostgreSQL URLs (only with feature)
        #[cfg(feature = "postgres")]
        {
            assert!(DbPool::from_url("postgres://localhost/test")
                .unwrap()
                .is_postgres());
            assert!(DbPool::from_url("postgresql://localhost/test")
                .unwrap()
                .is_postgres());
        }
    }
}
