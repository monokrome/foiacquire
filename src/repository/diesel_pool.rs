//! Diesel async connection pool management for SQLite.
//!
//! Uses diesel-async's SyncConnectionWrapper to provide an async interface
//! for SQLite. Since SQLite connections are lightweight, we create new
//! connections per request rather than pooling.

use diesel::sqlite::SqliteConnection;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::AsyncConnection;
use std::path::Path;

/// Diesel error type alias.
pub type DieselError = diesel::result::Error;

/// Async SQLite connection using SyncConnectionWrapper.
pub type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;

/// A simple async connection factory for SQLite.
///
/// Since SQLite connections are lightweight and file-based, we create
/// new connections per request. The SyncConnectionWrapper internally
/// uses spawn_blocking for async operation.
#[derive(Clone)]
pub struct AsyncSqlitePool {
    database_url: String,
}

impl AsyncSqlitePool {
    /// Create a new async SQLite pool.
    pub fn new(database_url: &str, _max_size: usize) -> Self {
        // Strip sqlite: prefix if present for diesel
        let url = database_url.strip_prefix("sqlite:").unwrap_or(database_url);
        Self {
            database_url: url.to_string(),
        }
    }

    /// Create pool from a file path.
    pub fn from_path(db_path: &Path, max_size: usize) -> Self {
        Self::new(&db_path.display().to_string(), max_size)
    }

    /// Get a new connection.
    pub async fn get(&self) -> Result<AsyncSqliteConnection, DieselError> {
        AsyncSqliteConnection::establish(&self.database_url)
            .await
            .map_err(super::util::to_diesel_error)
    }

    /// Get the database URL.
    #[allow(dead_code)]
    pub fn database_url(&self) -> &str {
        &self.database_url
    }
}
