//! Data models for FOIAcquire.

mod crawl;
mod document;
mod document_page;
mod source;
mod virtual_file;

pub use crawl::{CrawlRequest, CrawlUrl, DiscoveryMethod, UrlStatus};
pub use document::{Document, DocumentDisplay, DocumentStatus, DocumentVersion};
pub use document_page::{DocumentPage, PageOcrStatus};
pub use source::{Source, SourceType};
pub use virtual_file::{VirtualFile, VirtualFileStatus};
