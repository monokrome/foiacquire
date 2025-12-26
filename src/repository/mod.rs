//! Repository layer for database persistence.
//!
//! All database access uses async sqlx. The `DbContext` provides a unified
//! entry point for creating repositories with a shared connection pool.

mod config_history;
mod context;
mod crawl;
mod document;
mod source;

pub use config_history::{AsyncConfigHistoryRepository, ConfigHistoryEntry};
pub use context::DbContext;
pub use crawl::AsyncCrawlRepository;
pub use document::{extract_filename_parts, sanitize_filename, AsyncDocumentRepository, DocumentSummary};
pub use source::AsyncSourceRepository;

use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;

/// Parse a datetime string from the database, defaulting to Unix epoch on error.
pub fn parse_datetime(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(DateTime::UNIX_EPOCH)
}

/// Parse an optional datetime string from the database.
pub fn parse_datetime_opt(s: Option<String>) -> Option<DateTime<Utc>> {
    s.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
    })
}

#[derive(Error, Debug)]
pub enum RepositoryError {
    #[error("SQLx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, RepositoryError>;

/// Create an async SQLx connection pool from a database URL.
///
/// Supports SQLite URLs like:
/// - `sqlite:path/to/db.sqlite`
/// - `sqlite:/absolute/path/to/db.sqlite`
/// - `sqlite::memory:` (in-memory database)
///
/// The URL can also be set via the DATABASE_URL environment variable.
pub async fn create_pool_from_url(database_url: &str) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(30))
        .pragma("cache_size", "-64000") // 64MB cache
        .pragma("mmap_size", "268435456") // 256MB memory-mapped I/O
        .pragma("temp_store", "MEMORY")
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await?;

    Ok(pool)
}

/// Create an async SQLx connection pool from a file path.
///
/// This is a convenience wrapper around `create_pool_from_url` that handles
/// path-to-URL conversion, including UNC paths on Windows.
pub async fn create_pool(db_path: &Path) -> Result<SqlitePool> {
    // Handle UNC paths (\\server\share\...) which need special SQLite URI format
    // SQLite URI for UNC: file:////server/share/path (4 slashes = file:// + //server)
    let path_str = db_path.to_string_lossy();
    let db_url = if path_str.starts_with("\\\\") {
        // Windows UNC path: \\server\share\... -> file:////server/share/...
        let normalized = path_str.replace('\\', "/");
        format!("sqlite:file://{}", normalized)
    } else if path_str.starts_with("//") {
        // Unix-style UNC path: //server/share/... -> file:////server/share/...
        format!("sqlite:file://{}", path_str)
    } else {
        format!("sqlite:{}", db_path.display())
    };

    create_pool_from_url(&db_url).await
}
