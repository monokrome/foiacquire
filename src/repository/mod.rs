//! Repository layer for database persistence.
//!
//! This module is transitioning from rusqlite to sqlx. During transition,
//! both implementations coexist. New code should use the sqlx-based repositories.

#![allow(dead_code)]
#![allow(unused_imports)]

mod config_history;
mod crawl;
mod document;
mod source;

pub use config_history::{ConfigHistoryEntry, ConfigHistoryRepository};
pub use crawl::CrawlRepository;
pub use document::{
    extract_filename_parts, sanitize_filename, DocumentRepository, DocumentSummary,
};
pub use source::{AsyncSourceRepository, SourceRepository};

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
use std::thread;
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

/// Convert a rusqlite Result<T> to Result<Option<T>>, treating QueryReturnedNoRows as None.
pub fn to_option<T>(result: rusqlite::Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[derive(Error, Debug)]
pub enum RepositoryError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("SQLx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, RepositoryError>;

/// Create an async SQLx connection pool with optimized settings.
///
/// This is the sqlx equivalent of `connect()` for rusqlite.
/// The pool handles connection management and concurrency automatically.
pub async fn create_pool(db_path: &Path) -> Result<SqlitePool> {
    let db_url = format!("sqlite:{}", db_path.display());

    let options = SqliteConnectOptions::from_str(&db_url)?
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

/// Create a database connection with optimized settings for concurrency.
pub fn connect(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)?;

    // Enable WAL mode for better concurrency (multiple readers + one writer)
    // WAL mode persists, so this is effectively a one-time setting per database
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 30000;

        -- Performance optimizations
        PRAGMA cache_size = -64000;        -- 64MB cache (negative = KB)
        PRAGMA mmap_size = 268435456;      -- 256MB memory-mapped I/O
        PRAGMA temp_store = MEMORY;        -- Store temp tables in memory
        PRAGMA page_size = 4096;           -- Optimal page size
    "#,
    )?;

    Ok(conn)
}

/// Run all database migrations explicitly.
/// This ensures all tables are created and migrations are applied.
/// Returns a list of tables that exist after migration.
pub fn run_all_migrations(db_path: &Path, documents_dir: &Path) -> Result<Vec<String>> {
    // Create all repositories - this runs their init_schema and migrations
    let _doc_repo = DocumentRepository::new(db_path, documents_dir)?;
    let _source_repo = SourceRepository::new(db_path)?;
    let _crawl_repo = CrawlRepository::new(db_path)?;
    let _config_history_repo = ConfigHistoryRepository::new(db_path)?;

    // Also create the rate_limit_state table (used by scrapers)
    let conn = connect(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS rate_limit_state (
            domain TEXT PRIMARY KEY,
            current_delay_ms INTEGER NOT NULL,
            in_backoff INTEGER NOT NULL DEFAULT 0,
            total_requests INTEGER NOT NULL DEFAULT 0,
            rate_limit_hits INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    "#,
    )?;

    // Get list of all tables
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )?;
    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(tables)
}

/// Execute a database operation with retry logic for lock errors.
/// Retries up to 5 times with exponential backoff (100ms, 200ms, 400ms, 800ms, 1600ms).
pub fn with_retry<T, F>(mut operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let max_retries = 5;
    let mut delay_ms = 100;

    for attempt in 0..max_retries {
        match operation() {
            Ok(result) => return Ok(result),
            Err(RepositoryError::Database(ref e)) => {
                // Check if it's a lock error (SQLITE_BUSY or SQLITE_LOCKED)
                let is_lock_error = e.to_string().contains("database is locked")
                    || e.to_string().contains("SQLITE_BUSY")
                    || e.to_string().contains("SQLITE_LOCKED");

                if is_lock_error && attempt < max_retries - 1 {
                    tracing::debug!(
                        "Database locked, retrying in {}ms (attempt {}/{})",
                        delay_ms,
                        attempt + 1,
                        max_retries
                    );
                    thread::sleep(Duration::from_millis(delay_ms));
                    delay_ms *= 2; // Exponential backoff
                    continue;
                }
                // Not a lock error or out of retries
                return Err(RepositoryError::Database(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(
                        e.sqlite_error_code()
                            .unwrap_or(rusqlite::ffi::ErrorCode::Unknown)
                            as i32,
                    ),
                    Some(e.to_string()),
                )));
            }
            Err(e) => return Err(e),
        }
    }

    // Should not reach here, but just in case
    operation()
}
