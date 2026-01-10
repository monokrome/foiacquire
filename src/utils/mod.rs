//! Shared utility functions.
//!
//! This module contains reusable utilities used across the codebase:
//! - `html`: HTML escaping for safe rendering
//! - `format`: Human-readable formatting (sizes, etc.)
//! - `mime`: MIME type categorization and icons

mod format;
mod mime;

pub use format::format_size;
pub use mime::{category_to_mime_patterns, mime_icon, mime_to_category, mime_type_category, MimeCategory};
