//! HTTP request handlers for the web server.

#![allow(dead_code)]

mod api;
mod browse;
mod documents;
mod duplicates;
mod helpers;
mod ocr;
mod pages;
mod sources;
mod static_files;
mod tags;
mod timeline;
mod types;

// Re-export handlers for use by the router
pub use api::{
    api_recent_docs, api_search_tags, api_source_status, api_sources, api_status, api_type_stats,
};
pub use browse::browse_documents;
pub use documents::{document_detail, document_versions};
pub use duplicates::list_duplicates;
pub use ocr::{api_reocr_document, api_reocr_status};
pub use pages::api_document_pages;
// Note: sources handlers (index, list_source_documents, list_sources) are
// currently unused since the browse page serves as the main entry point.
pub use static_files::{serve_css, serve_file, serve_js};
pub use tags::{api_tags, list_tag_documents, list_tags};
pub use timeline::{timeline_aggregate, timeline_source};
pub use types::{list_by_type, list_types};
