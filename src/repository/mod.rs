//! Repository layer for database persistence.
//!
//! All database access uses Diesel ORM with compile-time query checking.
//! For SQLite, uses diesel-async's SyncConnectionWrapper to provide async interface.

// Diesel-based repositories
pub mod diesel_config_history;
pub mod diesel_context;
pub mod diesel_crawl;
pub mod diesel_document;
pub mod diesel_models;
pub mod diesel_pool;
pub mod diesel_source;
pub mod util;

// Database migration
pub mod migration;
#[cfg(feature = "postgres")]
pub mod migration_postgres;
pub mod migration_sqlite;

// Keep the document helpers (types like DocumentNavigation, etc.)
mod document;

// Re-export main types using Diesel implementations
pub use diesel_context::DieselDbContext as DbContext;
pub use diesel_crawl::DieselCrawlRepository;
pub use diesel_document::DieselDocumentRepository;
pub use diesel_pool::{AsyncSqlitePool, DieselError};
pub use diesel_source::DieselSourceRepository;
pub use migration::{DatabaseExporter, DatabaseImporter};
pub use migration_sqlite::SqliteMigrator;

// Re-export helper types from document module
pub use document::{extract_filename_parts, sanitize_filename, DocumentSummary};

use chrono::{DateTime, Utc};

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
