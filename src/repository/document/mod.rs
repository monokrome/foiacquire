//! Document repository for SQLite persistence.
//!
//! This module is split into submodules for maintainability:
//! - `schema`: Database schema initialization and migrations
//! - `crud`: Basic create, read, update, delete operations
//! - `query`: Complex queries, browsing, search
//! - `stats`: Counting and statistics
//! - `pages`: Document page and OCR operations
//! - `virtual_files`: Archive/email virtual file handling
//! - `annotations`: Document annotation tracking
//! - `dates`: Date estimation and management
//! - `helpers`: Shared parsing and query building utilities

#![allow(dead_code)]

mod annotations;
mod crud;
mod dates;
mod helpers;
mod pages;
mod query;
mod schema;
mod stats;
mod virtual_files;

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use super::Result;

// Re-export public types
pub use helpers::{
    extract_filename_parts, sanitize_filename, BrowseResult, DocumentNavigation, DocumentSummary,
    VersionSummary,
};

/// Current storage format version. Increment when changing file naming scheme.
pub(crate) const STORAGE_FORMAT_VERSION: i32 = 13;

/// SQLite-backed document repository.
pub struct DocumentRepository {
    pub(crate) db_path: PathBuf,
    pub(crate) documents_dir: PathBuf,
}

impl DocumentRepository {
    /// Create a new document repository.
    pub fn new(db_path: &Path, documents_dir: &Path) -> Result<Self> {
        let repo = Self {
            db_path: db_path.to_path_buf(),
            documents_dir: documents_dir.to_path_buf(),
        };
        repo.init_schema()?;
        repo.migrate_storage()?;
        Ok(repo)
    }

    pub(crate) fn connect(&self) -> Result<Connection> {
        super::connect(&self.db_path)
    }

    /// Get the documents directory path.
    pub fn documents_dir(&self) -> &Path {
        &self.documents_dir
    }

    /// Get the database path.
    pub fn database_path(&self) -> &Path {
        &self.db_path
    }
}
