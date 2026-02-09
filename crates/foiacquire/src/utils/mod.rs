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
    category_to_mime_patterns, mime_icon, mime_to_category, mime_type_category, MimeCategory,
};
pub use url_finder::UrlFinder;

/// Extract document title from URL.
///
/// Takes the last path segment, strips common file extensions, and replaces
/// underscores/hyphens with spaces.
pub fn extract_title_from_url(url: &str) -> String {
    let path = url.split('/').next_back().unwrap_or("untitled");
    let name = path
        .trim_end_matches(".pdf")
        .trim_end_matches(".PDF")
        .trim_end_matches(".doc")
        .trim_end_matches(".docx");
    name.replace(['_', '-'], " ")
}
