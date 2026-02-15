//! Shared helper functions for CLI commands.

pub use foia_scrape::save_scraped_document_async;

/// Result of a refresh operation on a document.
#[allow(dead_code)]
pub enum RefreshResult {
    /// Content changed, new version added
    ContentChanged,
    /// Metadata updated (filename or server_date)
    MetadataUpdated,
    /// No changes needed
    Unchanged,
}
