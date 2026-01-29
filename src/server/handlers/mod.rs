//! HTTP request handlers for the web server.

mod annotations_api;
mod api;
mod browse;
mod documents;
mod documents_api;
mod duplicates;
mod export_api;
mod helpers;
mod ocr;
mod pages;
mod scrape_api;
mod static_files;
mod tags;
mod timeline;
mod types;
mod versions_api;

// Re-export handlers for use by the router
pub use annotations_api::{annotation_stats, get_annotation, list_annotations, update_annotation};
pub use api::{
    api_recent_docs, api_search_tags, api_source_status, api_sources, api_status, api_type_stats,
    health,
};
pub use browse::browse_documents;
pub use documents::{document_detail, document_versions};
pub use documents_api::{get_document, get_document_content, list_documents};
pub use duplicates::list_duplicates;
pub use export_api::{export_annotations, export_documents, export_stats};
pub use ocr::{api_reocr_document, api_reocr_status};
pub use pages::api_document_pages;
pub use scrape_api::{get_scrape_status, list_queue, list_scrapers, retry_failed};
pub use static_files::{serve_css, serve_file, serve_js};
pub use tags::{api_tags, list_tag_documents, list_tags};
pub use timeline::{timeline_aggregate, timeline_source};
pub use types::{list_by_type, list_types};
pub use versions_api::{find_by_hash, get_version, list_versions};
