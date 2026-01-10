//! Repository layer for database persistence.
//!
//! All database access uses Diesel ORM with compile-time query checking.
//! Supports both SQLite and PostgreSQL backends.

// New unified modules
pub mod context;
pub mod models;
pub mod pool;

// Repositories
pub mod source;

// Legacy diesel-prefixed modules (to be removed)
pub mod diesel_config_history;
pub mod diesel_crawl;
pub mod diesel_document;

// Keep these until fully migrated
pub mod diesel_context;
pub mod diesel_models;
pub mod diesel_service_status;
pub mod diesel_source;

// Utilities
pub mod util;

// Database migration (legacy - to be removed)
pub mod migration;
#[cfg(feature = "postgres")]
pub mod migration_postgres;
pub mod migration_sqlite;

// Diesel migrations (new)
pub mod migrations;

// Document helpers (types like DocumentNavigation, etc.)
mod document;

// Re-export main types (may be unused in main binary but are public API)
#[allow(unused_imports)]
pub use context::DbContext;
#[allow(unused_imports)]
pub use pool::{DbError, DbPool};
#[allow(unused_imports)]
pub use source::SourceRepository;

// Legacy re-exports for backwards compatibility
#[allow(unused_imports)]
pub use diesel_config_history::DieselConfigHistoryRepository;
pub use diesel_crawl::DieselCrawlRepository;
pub use diesel_document::DieselDocumentRepository;
#[allow(unused_imports)]
pub use diesel_service_status::DieselServiceStatusRepository;
pub use diesel_source::DieselSourceRepository;
pub use migration::{DatabaseExporter, DatabaseImporter};
pub use migration_sqlite::SqliteMigrator;
pub use pool::DieselError;

// Re-export helper types from document module
pub use document::{extract_filename_parts, sanitize_filename};

// Re-export models (public API)
#[allow(unused_imports)]
pub use models::{
    ConfigHistoryRecord, CrawlConfigRecord, CrawlRequestRecord, CrawlUrlRecord, DocumentPageRecord,
    DocumentRecord, DocumentVersionRecord, NewConfigHistory, NewCrawlRequest, NewCrawlUrl,
    NewDocument, NewDocumentPage, NewDocumentVersion, NewRateLimitState, NewSource, NewVirtualFile,
    RateLimitStateRecord, SourceRecord, VirtualFileRecord,
};

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
