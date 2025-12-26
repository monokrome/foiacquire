//! Shared utility functions.
//!
//! This module contains reusable utilities used across the codebase:
//! - `html`: HTML escaping for safe rendering
//! - `format`: Human-readable formatting (sizes, etc.)
//! - `mime`: MIME type categorization and icons

mod format;
mod html;
mod mime;

pub use format::format_size;
pub use html::html_escape;
pub use mime::{mime_icon, mime_type_sql_condition, MimeCategory};
