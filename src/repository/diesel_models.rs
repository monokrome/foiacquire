//! Diesel ORM models for database tables.
//!
//! These models provide compile-time type checking for database operations.
//! For SQLite, operations are wrapped in spawn_blocking since diesel-async
//! only supports Postgres/MySQL.

use diesel::prelude::*;

use crate::schema;

/// Source record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::sources)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct SourceRecord {
    pub id: String,
    pub source_type: String,
    pub name: String,
    pub base_url: String,
    pub metadata: String,
    pub created_at: String,
    pub last_scraped: Option<String>,
}

/// New source for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::sources)]
pub struct NewSource<'a> {
    pub id: &'a str,
    pub source_type: &'a str,
    pub name: &'a str,
    pub base_url: &'a str,
    pub metadata: &'a str,
    pub created_at: &'a str,
    pub last_scraped: Option<&'a str>,
}

/// Crawl URL record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::crawl_urls)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct CrawlUrlRecord {
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

/// New crawl URL for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::crawl_urls)]
pub struct NewCrawlUrl<'a> {
    pub url: &'a str,
    pub source_id: &'a str,
    pub status: &'a str,
    pub discovery_method: &'a str,
    pub parent_url: Option<&'a str>,
    pub discovery_context: &'a str,
    pub depth: i32,
    pub discovered_at: &'a str,
    pub fetched_at: Option<&'a str>,
    pub retry_count: i32,
    pub last_error: Option<&'a str>,
    pub next_retry_at: Option<&'a str>,
    pub etag: Option<&'a str>,
    pub last_modified: Option<&'a str>,
    pub content_hash: Option<&'a str>,
    pub document_id: Option<&'a str>,
}

/// Crawl request record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::crawl_requests)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct CrawlRequestRecord {
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

/// New crawl request for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::crawl_requests)]
pub struct NewCrawlRequest<'a> {
    pub source_id: &'a str,
    pub url: &'a str,
    pub method: &'a str,
    pub request_headers: &'a str,
    pub request_at: &'a str,
    pub response_status: Option<i32>,
    pub response_headers: &'a str,
    pub response_at: Option<&'a str>,
    pub response_size: Option<i32>,
    pub duration_ms: Option<i32>,
    pub error: Option<&'a str>,
    pub was_conditional: i32,
    pub was_not_modified: i32,
}

/// Document record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::documents)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DocumentRecord {
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

/// New document for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::documents)]
pub struct NewDocument<'a> {
    pub id: &'a str,
    pub source_id: &'a str,
    pub title: &'a str,
    pub source_url: &'a str,
    pub extracted_text: Option<&'a str>,
    pub status: &'a str,
    pub metadata: &'a str,
    pub created_at: &'a str,
    pub updated_at: &'a str,
    pub synopsis: Option<&'a str>,
    pub tags: Option<&'a str>,
    pub estimated_date: Option<&'a str>,
    pub date_confidence: Option<&'a str>,
    pub date_source: Option<&'a str>,
    pub manual_date: Option<&'a str>,
    pub discovery_method: &'a str,
    pub category_id: Option<&'a str>,
}

/// Document version record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::document_versions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DocumentVersionRecord {
    pub id: i32,
    pub document_id: String,
    pub content_hash: String,
    pub file_path: String,
    pub file_size: i32,
    pub mime_type: String,
    pub acquired_at: String,
    pub source_url: Option<String>,
    pub original_filename: Option<String>,
    pub server_date: Option<String>,
    pub page_count: Option<i32>,
}

/// New document version for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::document_versions)]
pub struct NewDocumentVersion<'a> {
    pub document_id: &'a str,
    pub content_hash: &'a str,
    pub file_path: &'a str,
    pub file_size: i32,
    pub mime_type: &'a str,
    pub acquired_at: &'a str,
    pub source_url: Option<&'a str>,
    pub original_filename: Option<&'a str>,
    pub server_date: Option<&'a str>,
    pub page_count: Option<i32>,
}

/// Document page record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::document_pages)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DocumentPageRecord {
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

/// New document page for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::document_pages)]
pub struct NewDocumentPage<'a> {
    pub document_id: &'a str,
    pub version_id: i32,
    pub page_number: i32,
    pub pdf_text: Option<&'a str>,
    pub ocr_text: Option<&'a str>,
    pub final_text: Option<&'a str>,
    pub ocr_status: &'a str,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

/// Config history record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::configuration_history)]
#[diesel(primary_key(uuid))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ConfigHistoryRecord {
    pub uuid: String,
    pub created_at: String,
    pub data: String,
    pub format: String,
    pub hash: String,
}

/// New config history entry for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::configuration_history)]
pub struct NewConfigHistory<'a> {
    pub uuid: &'a str,
    pub created_at: &'a str,
    pub data: &'a str,
    pub format: &'a str,
    pub hash: &'a str,
}

/// Crawl config record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::crawl_config)]
#[diesel(primary_key(source_id))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct CrawlConfigRecord {
    pub source_id: String,
    pub config_hash: String,
    pub updated_at: String,
}

/// Virtual file record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::virtual_files)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct VirtualFileRecord {
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

/// New virtual file for insertion.
#[derive(Insertable, Debug)]
#[diesel(table_name = schema::virtual_files)]
pub struct NewVirtualFile<'a> {
    pub id: &'a str,
    pub document_id: &'a str,
    pub version_id: i32,
    pub archive_path: &'a str,
    pub filename: &'a str,
    pub mime_type: &'a str,
    pub file_size: i32,
    pub extracted_text: Option<&'a str>,
    pub synopsis: Option<&'a str>,
    pub tags: Option<&'a str>,
    pub status: &'a str,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

/// Rate limit state record from the database.
#[derive(Queryable, Selectable, Identifiable, Debug, Clone)]
#[diesel(table_name = schema::rate_limit_state)]
#[diesel(primary_key(domain))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RateLimitStateRecord {
    pub domain: String,
    pub current_delay_ms: i32,
    pub in_backoff: i32,
    pub total_requests: i32,
    pub rate_limit_hits: i32,
    pub updated_at: String,
}
