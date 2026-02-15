//! Archive sources for discovering historical document versions.
//!
//! This module provides a trait-based abstraction for querying web archives
//! (Wayback Machine, archive.today, Common Crawl, etc.) to find historical
//! versions of documents. The scraper uses these to discover archive URLs,
//! which are then fetched like any other document URL.

mod wayback;

pub use wayback::WaybackSource;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use foia::models::ArchiveService;

/// Errors that can occur when querying archive sources.
#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Rate limited by archive service")]
    RateLimited,

    #[error("Archive service unavailable")]
    Unavailable,

    #[error("No snapshots found")]
    NotFound,
}

/// Information about a snapshot available in an archive.
///
/// This represents metadata from the archive's index, not the actual content.
/// Use this to decide which snapshots to fetch and for deduplication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Which archive service has this snapshot
    pub service: ArchiveService,
    /// Original URL that was archived
    pub original_url: String,
    /// URL to retrieve the archived content
    pub archive_url: String,
    /// When the archive captured this snapshot
    pub captured_at: DateTime<Utc>,
    /// HTTP status code from when it was captured
    pub http_status: Option<u16>,
    /// MIME type from archive metadata
    pub mimetype: Option<String>,
    /// Content length from archive metadata
    pub content_length: Option<i64>,
    /// Content digest from archive (e.g., Wayback SHA-1)
    pub digest: Option<String>,
}

impl SnapshotInfo {
    /// Check if this snapshot likely has the same content as another based on digest.
    pub fn content_matches(&self, other: &SnapshotInfo) -> bool {
        match (&self.digest, &other.digest) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }

    /// Check if this snapshot likely has the same content based on a known hash.
    pub fn matches_digest(&self, digest: &str) -> bool {
        self.digest.as_ref().is_some_and(|d| d == digest)
    }
}

/// Trait for archive sources that can list historical snapshots.
///
/// Implementations query an archive's index/API to find what snapshots exist
/// for a URL. The actual content fetching is handled by the standard scraper
/// infrastructure using the `archive_url` from `SnapshotInfo`.
#[async_trait]
pub trait ArchiveSource: Send + Sync {
    /// Which archive service this source queries.
    fn service(&self) -> ArchiveService;

    /// List all available snapshots for a URL.
    ///
    /// Returns snapshots ordered by capture date (oldest first).
    /// May return empty vec if no snapshots exist.
    async fn list_snapshots(&self, url: &str) -> Result<Vec<SnapshotInfo>, ArchiveError>;

    /// List snapshots captured within a date range.
    async fn list_snapshots_range(
        &self,
        url: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<SnapshotInfo>, ArchiveError>;

    /// Get the most recent snapshot for a URL.
    async fn latest_snapshot(&self, url: &str) -> Result<Option<SnapshotInfo>, ArchiveError> {
        let snapshots = self.list_snapshots(url).await?;
        Ok(snapshots.into_iter().last())
    }

    /// Get the earliest snapshot for a URL.
    async fn earliest_snapshot(&self, url: &str) -> Result<Option<SnapshotInfo>, ArchiveError> {
        let snapshots = self.list_snapshots(url).await?;
        Ok(snapshots.into_iter().next())
    }

    /// Find snapshots that don't match a known digest (potential different versions).
    async fn find_different_versions(
        &self,
        url: &str,
        known_digest: &str,
    ) -> Result<Vec<SnapshotInfo>, ArchiveError> {
        let snapshots = self.list_snapshots(url).await?;
        Ok(snapshots
            .into_iter()
            .filter(|s| !s.matches_digest(known_digest))
            .collect())
    }

    /// Deduplicate snapshots by digest, keeping only the earliest of each unique version.
    fn deduplicate_by_digest(&self, snapshots: Vec<SnapshotInfo>) -> Vec<SnapshotInfo> {
        use std::collections::HashMap;

        let mut by_digest: HashMap<String, SnapshotInfo> = HashMap::new();
        let mut no_digest: Vec<SnapshotInfo> = Vec::new();

        for snapshot in snapshots {
            if let Some(ref digest) = snapshot.digest {
                by_digest
                    .entry(digest.clone())
                    .and_modify(|existing| {
                        if snapshot.captured_at < existing.captured_at {
                            *existing = snapshot.clone();
                        }
                    })
                    .or_insert(snapshot);
            } else {
                no_digest.push(snapshot);
            }
        }

        let mut result: Vec<_> = by_digest.into_values().collect();
        result.extend(no_digest);
        result.sort_by_key(|s| s.captured_at);
        result
    }
}

/// Registry of available archive sources.
pub struct ArchiveRegistry {
    sources: Vec<Box<dyn ArchiveSource>>,
}

impl Default for ArchiveRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveRegistry {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    /// Create a registry with all default archive sources.
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        registry.register(Box::new(WaybackSource::new()));
        registry
    }

    /// Register an archive source.
    pub fn register(&mut self, source: Box<dyn ArchiveSource>) {
        self.sources.push(source);
    }

    /// Get all registered sources.
    pub fn sources(&self) -> &[Box<dyn ArchiveSource>] {
        &self.sources
    }

    /// Get a source by service type.
    pub fn get(&self, service: ArchiveService) -> Option<&dyn ArchiveSource> {
        self.sources
            .iter()
            .find(|s| s.service() == service)
            .map(|s| s.as_ref())
    }

    /// Query all sources for snapshots of a URL.
    pub async fn list_all_snapshots(&self, url: &str) -> Vec<(ArchiveService, Vec<SnapshotInfo>)> {
        let mut results = Vec::new();

        for source in &self.sources {
            match source.list_snapshots(url).await {
                Ok(snapshots) => {
                    if !snapshots.is_empty() {
                        results.push((source.service(), snapshots));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to query {} for {}: {}",
                        source.service().display_name(),
                        url,
                        e
                    );
                }
            }
        }

        results
    }
}
