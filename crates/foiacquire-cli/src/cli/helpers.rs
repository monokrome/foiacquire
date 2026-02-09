//! Shared helper functions for CLI commands.

pub use foiacquire::storage::{
    content_storage_path, content_storage_path_with_name, mime_to_extension,
    save_scraped_document_async,
};

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
