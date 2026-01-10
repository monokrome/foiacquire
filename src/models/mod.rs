//! Data models for FOIAcquire.

mod archive;
mod crawl;
mod document;
mod document_page;
mod service_status;
mod source;
mod virtual_file;

pub use archive::ArchiveService;
pub use crawl::{CrawlRequest, CrawlUrl, DiscoveryMethod, UrlStatus};
pub use document::{Document, DocumentStatus, DocumentVersion};
pub use document_page::{DocumentPage, PageOcrStatus};
pub use service_status::{ScraperStats, ServiceState, ServiceStatus, ServiceType};
pub use source::{Source, SourceType};
pub use virtual_file::{VirtualFile, VirtualFileStatus};
