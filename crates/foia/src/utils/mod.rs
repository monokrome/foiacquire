//! Shared utility functions.
//!
//! This module contains reusable utilities used across the codebase:
//! - `html`: HTML escaping for safe rendering
//! - `format`: Human-readable formatting (sizes, etc.)
//! - `mime`: MIME type categorization and icons

mod format;
mod mime;
pub mod url_finder;

pub use format::format_size;
pub use mime::{
    category_to_mime_patterns, guess_mime_from_filename, guess_mime_from_url,
    has_document_extension, has_file_extension, is_document_mimetype, is_extractable_mimetype,
    mime_icon, mime_to_category, mime_type_category, MimeCategory,
};
pub use url_finder::UrlFinder;

/// Extract document title from URL.
///
/// Takes the last path segment, strips known file extensions, and replaces
/// underscores/hyphens with spaces.
pub fn extract_title_from_url(url: &str) -> String {
    let path = url.split('/').next_back().unwrap_or("untitled");
    let name = if let Some(dot_pos) = path.rfind('.') {
        if mime::guess_mime_from_filename(&path[dot_pos..]) != "application/octet-stream" {
            &path[..dot_pos]
        } else {
            path
        }
    } else {
        path
    };
    name.replace(['_', '-'], " ")
}
