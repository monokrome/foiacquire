//! Import system for ingesting documents from various sources.
//!
//! This module provides a trait-based abstraction for importing documents
//! from different formats (WARC, Concordance DAT/OPT, URL lists, etc.)
//! with unified progress tracking, duplicate detection, and resume support.

mod runner;
pub mod sources;

pub use runner::{FileStorageMode, ImportConfig, ImportRunner};
pub use sources::{ConcordanceImportSource, MultiPageMode, WarcImportSource};

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A single item yielded from an import source.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ImportItem {
    /// URL or identifier for deduplication.
    pub url: String,
    /// Document title.
    pub title: String,
    /// Raw content bytes.
    pub content: Vec<u8>,
    /// MIME type.
    pub mime_type: String,
    /// Source ID (if known by the importer).
    pub source_id: Option<String>,
    /// Additional metadata from the import source.
    pub metadata: serde_json::Value,
    /// Original filename if known.
    pub original_filename: Option<String>,
    /// Server/creation date if known.
    pub server_date: Option<DateTime<Utc>>,
}

/// Progress state that can be checkpointed and resumed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImportProgress {
    /// Current position (bytes for WARC, rows for DAT, etc.).
    pub position: u64,
    /// Whether processing is complete.
    pub done: bool,
    /// Last error message if any.
    pub error: Option<String>,
}

/// Statistics collected during import.
#[derive(Debug, Clone, Default)]
pub struct ImportStats {
    /// Total items scanned.
    pub scanned: usize,
    /// Successfully imported items.
    pub imported: usize,
    /// Skipped (already exists).
    pub skipped: usize,
    /// Filtered out by regex.
    pub filtered: usize,
    /// No matching source found.
    pub no_source: usize,
    /// Errors during import.
    pub errors: usize,
    /// Referenced files not found (for DAT imports).
    pub missing_files: usize,
    /// URLs of successfully imported documents (for verification queuing).
    pub imported_urls: Vec<String>,
}

impl ImportStats {
    /// Merge stats from another instance.
    pub fn merge(&mut self, other: &ImportStats) {
        self.scanned += other.scanned;
        self.imported += other.imported;
        self.skipped += other.skipped;
        self.filtered += other.filtered;
        self.no_source += other.no_source;
        self.errors += other.errors;
        self.missing_files += other.missing_files;
        self.imported_urls.extend_from_slice(&other.imported_urls);
    }
}

/// Trait for import sources (WARC, DAT/OPT, URL list, stdin, etc.)
#[async_trait::async_trait]
pub trait ImportSource: Send + Sync {
    /// Unique identifier for this import format.
    #[allow(dead_code)]
    fn format_id(&self) -> &'static str;

    /// Human-readable name for display.
    fn display_name(&self) -> &str;

    /// Path to the source file being imported.
    fn source_path(&self) -> &Path;

    /// Whether this source supports resume from checkpoint.
    fn supports_resume(&self) -> bool;

    /// Get total item count if known (for progress bar).
    fn total_count(&self) -> Option<u64> {
        None
    }

    /// Progress file path for a given import file.
    fn progress_path(&self) -> PathBuf {
        let source = self.source_path();
        let ext = source
            .extension()
            .map(|e| format!("{}.progress", e.to_string_lossy()))
            .unwrap_or_else(|| "progress".to_string());
        source.with_extension(ext)
    }

    /// Load previous progress for resumption.
    ///
    /// Supports both JSON format (current) and legacy text format
    /// ("done", "offset:N", "error:msg") from older WARC imports.
    fn load_progress(&self) -> Option<ImportProgress> {
        let path = self.progress_path();
        let content = std::fs::read_to_string(&path).ok()?;
        let content = content.trim();

        if content == "done" {
            return Some(ImportProgress {
                position: 0,
                done: true,
                error: None,
            });
        }
        if let Some(error_msg) = content.strip_prefix("error:") {
            return Some(ImportProgress {
                position: 0,
                done: false,
                error: Some(error_msg.to_string()),
            });
        }
        if let Some(offset_str) = content.strip_prefix("offset:") {
            if let Ok(offset) = offset_str.parse::<u64>() {
                return Some(ImportProgress {
                    position: offset,
                    done: false,
                    error: None,
                });
            }
        }

        serde_json::from_str(content).ok()
    }

    /// Save current progress for checkpointing.
    fn save_progress(&self, progress: &ImportProgress) -> std::io::Result<()> {
        let path = self.progress_path();
        let content = serde_json::to_string(progress).map_err(std::io::Error::other)?;
        std::fs::write(&path, content)
    }

    /// Run the import operation.
    ///
    /// The source is responsible for:
    /// - Iterating over items
    /// - Checking for duplicates using provided existing_urls set
    /// - Saving documents to the repository
    /// - Tracking statistics
    /// - Handling checkpoint saves
    async fn run_import(
        &mut self,
        config: &ImportConfig,
        start_position: u64,
    ) -> anyhow::Result<(ImportProgress, ImportStats)>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_stats_merge_includes_urls() {
        let mut a = ImportStats {
            imported: 2,
            imported_urls: vec![
                "https://example.com/1".into(),
                "https://example.com/2".into(),
            ],
            ..ImportStats::default()
        };

        let b = ImportStats {
            imported: 1,
            imported_urls: vec!["https://example.com/3".into()],
            ..ImportStats::default()
        };

        a.merge(&b);
        assert_eq!(a.imported, 3);
        assert_eq!(a.imported_urls.len(), 3);
        assert_eq!(a.imported_urls[2], "https://example.com/3");
    }
}

/// Guess MIME type from file extension.
pub fn guess_mime_type(path: &Path) -> String {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    foia::utils::guess_mime_from_filename(name).to_string()
}
