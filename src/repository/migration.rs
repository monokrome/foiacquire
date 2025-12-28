//! Database migration traits and portable record types.
//!
//! Provides a trait-based abstraction for exporting and importing database
//! contents, enabling migration between different database backends (SQLite, Postgres).
//!
//! The portable record types use owned Strings and are serializable to JSON/JSONL,
//! making them suitable for cross-database migration.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::DieselError;

/// Progress callback for import operations.
/// Called with the current count of imported records.
pub type ProgressCallback = Arc<dyn Fn(usize) + Send + Sync>;

/// Portable source record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableSource {
    pub id: String,
    pub source_type: String,
    pub name: String,
    pub base_url: String,
    pub metadata: String,
    pub created_at: String,
    pub last_scraped: Option<String>,
}

/// Portable document record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableDocument {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub source_url: String,
    pub extracted_text: Option<String>,
    pub status: String,
    pub metadata: String,
    pub created_at: String,
    pub updated_at: String,
    pub synopsis: Option<String>,
    pub tags: Option<String>,
    pub estimated_date: Option<String>,
    pub date_confidence: Option<String>,
    pub date_source: Option<String>,
    pub manual_date: Option<String>,
    pub discovery_method: String,
    pub category_id: Option<String>,
}

/// Portable document version record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableDocumentVersion {
    pub id: i32,
    pub document_id: String,
    pub content_hash: String,
    pub content_hash_blake3: Option<String>,
    pub file_path: String,
    pub file_size: i32,
    pub mime_type: String,
    pub acquired_at: String,
    pub source_url: Option<String>,
    pub original_filename: Option<String>,
    pub server_date: Option<String>,
    pub page_count: Option<i32>,
}

/// Portable document page record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableDocumentPage {
    pub id: i32,
    pub document_id: String,
    pub version_id: i32,
    pub page_number: i32,
    pub pdf_text: Option<String>,
    pub ocr_text: Option<String>,
    pub final_text: Option<String>,
    pub ocr_status: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Portable virtual file record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableVirtualFile {
    pub id: String,
    pub document_id: String,
    pub version_id: i32,
    pub archive_path: String,
    pub filename: String,
    pub mime_type: String,
    pub file_size: i32,
    pub extracted_text: Option<String>,
    pub synopsis: Option<String>,
    pub tags: Option<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Portable crawl URL record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableCrawlUrl {
    pub id: i32,
    pub url: String,
    pub source_id: String,
    pub status: String,
    pub discovery_method: String,
    pub parent_url: Option<String>,
    pub discovery_context: String,
    pub depth: i32,
    pub discovered_at: String,
    pub fetched_at: Option<String>,
    pub retry_count: i32,
    pub last_error: Option<String>,
    pub next_retry_at: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_hash: Option<String>,
    pub document_id: Option<String>,
}

/// Portable crawl request record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableCrawlRequest {
    pub id: i32,
    pub source_id: String,
    pub url: String,
    pub method: String,
    pub request_headers: String,
    pub request_at: String,
    pub response_status: Option<i32>,
    pub response_headers: String,
    pub response_at: Option<String>,
    pub response_size: Option<i32>,
    pub duration_ms: Option<i32>,
    pub error: Option<String>,
    pub was_conditional: i32,
    pub was_not_modified: i32,
}

/// Portable crawl config record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableCrawlConfig {
    pub source_id: String,
    pub config_hash: String,
    pub updated_at: String,
}

/// Portable config history record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableConfigHistory {
    pub uuid: String,
    pub created_at: String,
    pub data: String,
    pub format: String,
    pub hash: String,
}

/// Portable rate limit state record for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableRateLimitState {
    pub domain: String,
    pub current_delay_ms: i32,
    pub in_backoff: i32,
    pub total_requests: i32,
    pub rate_limit_hits: i32,
    pub updated_at: String,
}

/// Trait for exporting database contents to portable format.
///
/// Implementations should stream records in batches to handle large datasets
/// without excessive memory usage.
#[async_trait]
pub trait DatabaseExporter: Send + Sync {
    /// Export all sources.
    async fn export_sources(&self) -> Result<Vec<PortableSource>, DieselError>;

    /// Export all documents.
    async fn export_documents(&self) -> Result<Vec<PortableDocument>, DieselError>;

    /// Export all document versions.
    async fn export_document_versions(&self) -> Result<Vec<PortableDocumentVersion>, DieselError>;

    /// Export all document pages.
    async fn export_document_pages(&self) -> Result<Vec<PortableDocumentPage>, DieselError>;

    /// Export all virtual files.
    async fn export_virtual_files(&self) -> Result<Vec<PortableVirtualFile>, DieselError>;

    /// Export all crawl URLs.
    async fn export_crawl_urls(&self) -> Result<Vec<PortableCrawlUrl>, DieselError>;

    /// Export all crawl requests.
    async fn export_crawl_requests(&self) -> Result<Vec<PortableCrawlRequest>, DieselError>;

    /// Export all crawl configs.
    async fn export_crawl_configs(&self) -> Result<Vec<PortableCrawlConfig>, DieselError>;

    /// Export all config history entries.
    async fn export_config_history(&self) -> Result<Vec<PortableConfigHistory>, DieselError>;

    /// Export all rate limit states.
    async fn export_rate_limit_states(&self) -> Result<Vec<PortableRateLimitState>, DieselError>;
}

/// Trait for importing database contents from portable format.
///
/// Implementations should handle upsert semantics (insert or update on conflict)
/// to support incremental migrations.
#[async_trait]
pub trait DatabaseImporter: Send + Sync {
    /// Clear all data before import (optional, for clean imports).
    async fn clear_all(&self) -> Result<(), DieselError>;

    /// Import sources.
    async fn import_sources(
        &self,
        sources: &[PortableSource],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import documents.
    async fn import_documents(
        &self,
        documents: &[PortableDocument],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import document versions.
    async fn import_document_versions(
        &self,
        versions: &[PortableDocumentVersion],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import document pages.
    async fn import_document_pages(
        &self,
        pages: &[PortableDocumentPage],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import virtual files.
    async fn import_virtual_files(
        &self,
        files: &[PortableVirtualFile],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import crawl URLs.
    async fn import_crawl_urls(
        &self,
        urls: &[PortableCrawlUrl],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import crawl requests.
    async fn import_crawl_requests(
        &self,
        requests: &[PortableCrawlRequest],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import crawl configs.
    async fn import_crawl_configs(
        &self,
        configs: &[PortableCrawlConfig],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import config history entries.
    async fn import_config_history(
        &self,
        history: &[PortableConfigHistory],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;

    /// Import rate limit states.
    async fn import_rate_limit_states(
        &self,
        states: &[PortableRateLimitState],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>;
}

/// Conversion from database record to portable format.
impl From<super::diesel_models::SourceRecord> for PortableSource {
    fn from(r: super::diesel_models::SourceRecord) -> Self {
        PortableSource {
            id: r.id,
            source_type: r.source_type,
            name: r.name,
            base_url: r.base_url,
            metadata: r.metadata,
            created_at: r.created_at,
            last_scraped: r.last_scraped,
        }
    }
}

impl From<super::diesel_models::DocumentRecord> for PortableDocument {
    fn from(r: super::diesel_models::DocumentRecord) -> Self {
        PortableDocument {
            id: r.id,
            source_id: r.source_id,
            title: r.title,
            source_url: r.source_url,
            extracted_text: r.extracted_text,
            status: r.status,
            metadata: r.metadata,
            created_at: r.created_at,
            updated_at: r.updated_at,
            synopsis: r.synopsis,
            tags: r.tags,
            estimated_date: r.estimated_date,
            date_confidence: r.date_confidence,
            date_source: r.date_source,
            manual_date: r.manual_date,
            discovery_method: r.discovery_method,
            category_id: r.category_id,
        }
    }
}

impl From<super::diesel_models::DocumentVersionRecord> for PortableDocumentVersion {
    fn from(r: super::diesel_models::DocumentVersionRecord) -> Self {
        PortableDocumentVersion {
            id: r.id,
            document_id: r.document_id,
            content_hash: r.content_hash,
            content_hash_blake3: r.content_hash_blake3,
            file_path: r.file_path,
            file_size: r.file_size,
            mime_type: r.mime_type,
            acquired_at: r.acquired_at,
            source_url: r.source_url,
            original_filename: r.original_filename,
            server_date: r.server_date,
            page_count: r.page_count,
        }
    }
}

impl From<super::diesel_models::DocumentPageRecord> for PortableDocumentPage {
    fn from(r: super::diesel_models::DocumentPageRecord) -> Self {
        PortableDocumentPage {
            id: r.id,
            document_id: r.document_id,
            version_id: r.version_id,
            page_number: r.page_number,
            pdf_text: r.pdf_text,
            ocr_text: r.ocr_text,
            final_text: r.final_text,
            ocr_status: r.ocr_status,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

impl From<super::diesel_models::VirtualFileRecord> for PortableVirtualFile {
    fn from(r: super::diesel_models::VirtualFileRecord) -> Self {
        PortableVirtualFile {
            id: r.id,
            document_id: r.document_id,
            version_id: r.version_id,
            archive_path: r.archive_path,
            filename: r.filename,
            mime_type: r.mime_type,
            file_size: r.file_size,
            extracted_text: r.extracted_text,
            synopsis: r.synopsis,
            tags: r.tags,
            status: r.status,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

impl From<super::diesel_models::CrawlUrlRecord> for PortableCrawlUrl {
    fn from(r: super::diesel_models::CrawlUrlRecord) -> Self {
        PortableCrawlUrl {
            id: r.id,
            url: r.url,
            source_id: r.source_id,
            status: r.status,
            discovery_method: r.discovery_method,
            parent_url: r.parent_url,
            discovery_context: r.discovery_context,
            depth: r.depth,
            discovered_at: r.discovered_at,
            fetched_at: r.fetched_at,
            retry_count: r.retry_count,
            last_error: r.last_error,
            next_retry_at: r.next_retry_at,
            etag: r.etag,
            last_modified: r.last_modified,
            content_hash: r.content_hash,
            document_id: r.document_id,
        }
    }
}

impl From<super::diesel_models::CrawlRequestRecord> for PortableCrawlRequest {
    fn from(r: super::diesel_models::CrawlRequestRecord) -> Self {
        PortableCrawlRequest {
            id: r.id,
            source_id: r.source_id,
            url: r.url,
            method: r.method,
            request_headers: r.request_headers,
            request_at: r.request_at,
            response_status: r.response_status,
            response_headers: r.response_headers,
            response_at: r.response_at,
            response_size: r.response_size,
            duration_ms: r.duration_ms,
            error: r.error,
            was_conditional: r.was_conditional,
            was_not_modified: r.was_not_modified,
        }
    }
}

impl From<super::diesel_models::CrawlConfigRecord> for PortableCrawlConfig {
    fn from(r: super::diesel_models::CrawlConfigRecord) -> Self {
        PortableCrawlConfig {
            source_id: r.source_id,
            config_hash: r.config_hash,
            updated_at: r.updated_at,
        }
    }
}

impl From<super::diesel_models::ConfigHistoryRecord> for PortableConfigHistory {
    fn from(r: super::diesel_models::ConfigHistoryRecord) -> Self {
        PortableConfigHistory {
            uuid: r.uuid,
            created_at: r.created_at,
            data: r.data,
            format: r.format,
            hash: r.hash,
        }
    }
}

impl From<super::diesel_models::RateLimitStateRecord> for PortableRateLimitState {
    fn from(r: super::diesel_models::RateLimitStateRecord) -> Self {
        PortableRateLimitState {
            domain: r.domain,
            current_delay_ms: r.current_delay_ms,
            in_backoff: r.in_backoff,
            total_requests: r.total_requests,
            rate_limit_hits: r.rate_limit_hits,
            updated_at: r.updated_at,
        }
    }
}
