//! Download service types and events.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::privacy::PrivacyConfig;
use crate::scrapers::ViaMode;

/// Events emitted during download operations.
/// Fields are populated when events are created, even if consumers don't read all of them.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum DownloadEvent {
    /// Download started for a URL
    Started {
        worker_id: usize,
        url: String,
        filename: String,
    },
    /// Progress update (bytes downloaded)
    Progress {
        worker_id: usize,
        bytes: u64,
        total: Option<u64>,
    },
    /// Download completed successfully
    Completed {
        worker_id: usize,
        url: String,
        new_document: bool,
    },
    /// File deduplicated (identical content already exists)
    Deduplicated {
        worker_id: usize,
        url: String,
        existing_path: String,
    },
    /// Document unchanged (304 Not Modified)
    Unchanged { worker_id: usize, url: String },
    /// Download failed
    Failed {
        worker_id: usize,
        url: String,
        error: String,
    },
}

/// Result of a download operation.
/// Part of public API - consumers may use any field even if current CLI doesn't read all.
#[derive(Debug)]
#[allow(dead_code)]
pub struct DownloadResult {
    pub downloaded: usize,
    pub deduplicated: usize,
    pub skipped: usize,
    pub failed: usize,
    pub remaining: u64,
}

/// Configuration for download service.
pub struct DownloadConfig {
    pub documents_dir: PathBuf,
    pub request_timeout: Duration,
    pub request_delay: Duration,
    /// Privacy configuration for HTTP requests.
    pub privacy: PrivacyConfig,
    /// URL rewriting for caching proxies.
    pub via: HashMap<String, String>,
    /// Via mode controlling when via mappings are used.
    pub via_mode: ViaMode,
}
