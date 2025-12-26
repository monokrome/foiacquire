//! Download service types and events.

use std::path::PathBuf;
use std::time::Duration;

/// Events emitted during download operations.
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
#[derive(Debug)]
#[allow(dead_code)]
pub struct DownloadResult {
    pub downloaded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub remaining: u64,
}

/// Configuration for download service.
pub struct DownloadConfig {
    pub documents_dir: PathBuf,
    pub request_timeout: Duration,
    pub request_delay: Duration,
}
