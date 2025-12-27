//! Diesel-based document repository for SQLite.
//!
//! Uses diesel-async's SyncConnectionWrapper for async SQLite support.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};

use super::diesel_models::{DocumentRecord, DocumentVersionRecord, VirtualFileRecord};
use super::diesel_pool::{AsyncSqlitePool, DieselError};
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{Document, DocumentStatus, DocumentVersion, VirtualFile, VirtualFileStatus};
use crate::schema::{document_pages, document_versions, documents, virtual_files};

/// OCR result for a page.
#[derive(Debug, Clone)]
pub struct OcrResult {
    pub backend: String,
    pub text: Option<String>,
    pub confidence: Option<f32>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Summary of a document for list views.
#[derive(Debug, Clone)]
pub struct DieselDocumentSummary {
    pub id: String,
    pub source_id: String,
    pub url: String,
    pub title: Option<String>,
    pub status: DocumentStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version_count: u32,
    pub latest_file_size: Option<u64>,
}

/// Diesel-based document repository with compile-time query checking.
#[derive(Clone)]
pub struct DieselDocumentRepository {
    pool: AsyncSqlitePool,
    documents_dir: PathBuf,
}

impl DieselDocumentRepository {
    /// Create a new Diesel document repository.
    pub fn new(pool: AsyncSqlitePool, documents_dir: PathBuf) -> Self {
        Self { pool, documents_dir }
    }

    /// Get the documents directory path.
    pub fn documents_dir(&self) -> &Path {
        &self.documents_dir
    }

    /// Count all documents.
    pub async fn count(&self) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let count: i64 = documents::table
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(count as u64)
    }

    /// Get document counts per source.
    pub async fn get_all_source_counts(&self) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        let mut conn = self.pool.get().await?;

        let rows: Vec<SourceCount> = diesel::sql_query(
            "SELECT source_id, COUNT(*) as count FROM documents GROUP BY source_id",
        )
        .load(&mut conn)
        .await?;

        let mut counts = std::collections::HashMap::new();
        for SourceCount { source_id, count } in rows {
            counts.insert(source_id, count as u64);
        }
        Ok(counts)
    }

    /// Count documents needing OCR (stub).
    pub async fn count_needing_ocr(&self, _source_id: Option<&str>) -> Result<u64, DieselError> {
        Ok(0)
    }

    /// Count documents needing summarization (stub).
    pub async fn count_needing_summarization(&self, _source_id: Option<&str>) -> Result<u64, DieselError> {
        Ok(0)
    }

    /// Get type statistics (stub).
    pub async fn get_type_stats(&self) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        Ok(std::collections::HashMap::new())
    }

    /// Get recent documents.
    pub async fn get_recent(&self, limit: u32) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;
        let limit = limit as i64;

        let records: Vec<DocumentRecord> = documents::table
            .order(documents::updated_at.desc())
            .limit(limit)
            .load(&mut conn)
            .await?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Get category statistics (stub).
    pub async fn get_category_stats(&self) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        Ok(std::collections::HashMap::new())
    }

    /// Search tags (stub).
    pub async fn search_tags(&self, _query: &str) -> Result<Vec<String>, DieselError> {
        Ok(vec![])
    }

    /// Get all tags (stub).
    pub async fn get_all_tags(&self) -> Result<Vec<String>, DieselError> {
        Ok(vec![])
    }

    /// Browse documents.
    pub async fn browse(
        &self,
        source_id: Option<&str>,
        status: Option<&str>,
        _category: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;
        let limit = limit as i64;
        let offset = offset as i64;

        let mut query = documents::table
            .order(documents::updated_at.desc())
            .limit(limit)
            .offset(offset)
            .into_boxed();

        if let Some(sid) = source_id {
            query = query.filter(documents::source_id.eq(sid));
        }
        if let Some(st) = status {
            query = query.filter(documents::status.eq(st));
        }

        let records: Vec<DocumentRecord> = query.load(&mut conn).await?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Browse count.
    pub async fn browse_count(
        &self,
        source_id: Option<&str>,
        status: Option<&str>,
        _category: Option<&str>,
    ) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let mut query = documents::table.select(count_star()).into_boxed();

        if let Some(sid) = source_id {
            query = query.filter(documents::source_id.eq(sid));
        }
        if let Some(st) = status {
            query = query.filter(documents::status.eq(st));
        }

        let count: i64 = query.first(&mut conn).await?;
        Ok(count as u64)
    }

    /// Get document navigation.
    pub async fn get_document_navigation(
        &self,
        document_id: &str,
        source_id: &str,
    ) -> Result<super::document::DocumentNavigation, DieselError> {
        use super::document::DocumentNavigation;

        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;

        // Get previous document
        let prev: Option<(String, Option<String>)> = documents::table
            .select((documents::id, documents::title))
            .filter(documents::source_id.eq(source_id))
            .filter(documents::id.lt(document_id))
            .order(documents::id.desc())
            .first(&mut conn)
            .await
            .optional()?;

        // Get next document
        let next: Option<(String, Option<String>)> = documents::table
            .select((documents::id, documents::title))
            .filter(documents::source_id.eq(source_id))
            .filter(documents::id.gt(document_id))
            .order(documents::id.asc())
            .first(&mut conn)
            .await
            .optional()?;

        // Get position
        let position: i64 = documents::table
            .filter(documents::source_id.eq(source_id))
            .filter(documents::id.le(document_id))
            .select(count_star())
            .first(&mut conn)
            .await?;

        // Get total
        let total: i64 = documents::table
            .filter(documents::source_id.eq(source_id))
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(DocumentNavigation {
            prev_id: prev.as_ref().map(|(id, _)| id.clone()),
            prev_title: prev.and_then(|(_, title)| title),
            next_id: next.as_ref().map(|(id, _)| id.clone()),
            next_title: next.and_then(|(_, title)| title),
            position: position as u64,
            total: total as u64,
        })
    }

    /// Count pages for a document.
    pub async fn count_pages(&self, document_id: &str, version: i32) -> Result<u32, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let count: i64 = document_pages::table
            .filter(document_pages::document_id.eq(document_id))
            .filter(document_pages::version.eq(version))
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(count as u32)
    }

    // ========================================================================
    // Core CRUD Operations
    // ========================================================================

    /// Get a document by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let record: Option<DocumentRecord> = documents::table
            .find(id)
            .first(&mut conn)
            .await
            .optional()?;

        match record {
            Some(record) => {
                let versions = self.load_versions(&record.id).await?;
                Ok(Some(Self::record_to_document(record, versions)))
            }
            None => Ok(None),
        }
    }

    /// Get all documents for a source.
    pub async fn get_by_source(&self, source_id: &str) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentRecord> = documents::table
            .filter(documents::source_id.eq(source_id))
            .order(documents::created_at.desc())
            .load(&mut conn)
            .await?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Get documents by URL.
    pub async fn get_by_url(&self, url: &str) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentRecord> = documents::table
            .filter(documents::url.eq(url))
            .load(&mut conn)
            .await?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Check if a document exists.
    pub async fn exists(&self, id: &str) -> Result<bool, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let count: i64 = documents::table
            .filter(documents::id.eq(id))
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(count > 0)
    }

    /// Save a document.
    pub async fn save(&self, doc: &Document) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        let metadata = serde_json::to_string(&doc.metadata).unwrap_or_else(|_| "{}".to_string());
        let created_at = doc.created_at.to_rfc3339();
        let updated_at = doc.updated_at.to_rfc3339();
        let status = doc.status.as_str().to_string();

        diesel::replace_into(documents::table)
            .values((
                documents::id.eq(&doc.id),
                documents::source_id.eq(&doc.source_id),
                documents::url.eq(&doc.source_url),
                documents::title.eq(&doc.title),
                documents::status.eq(&status),
                documents::metadata.eq(&metadata),
                documents::created_at.eq(&created_at),
                documents::updated_at.eq(&updated_at),
            ))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    /// Delete a document.
    pub async fn delete(&self, id: &str) -> Result<bool, DieselError> {
        let mut conn = self.pool.get().await?;

        conn.transaction(|conn| {
            Box::pin(async move {
                diesel::delete(document_versions::table.filter(document_versions::document_id.eq(id)))
                    .execute(conn)
                    .await?;

                diesel::delete(document_pages::table.filter(document_pages::document_id.eq(id)))
                    .execute(conn)
                    .await?;

                diesel::delete(virtual_files::table.filter(virtual_files::document_id.eq(id)))
                    .execute(conn)
                    .await?;

                let rows = diesel::delete(documents::table.find(id))
                    .execute(conn)
                    .await?;

                Ok(rows > 0)
            })
        })
        .await
    }

    /// Update document status.
    pub async fn update_status(&self, id: &str, status: DocumentStatus) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        let status_str = status.as_str().to_string();
        let updated_at = Utc::now().to_rfc3339();

        diesel::update(documents::table.find(id))
            .set((
                documents::status.eq(&status_str),
                documents::updated_at.eq(&updated_at),
            ))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    // ========================================================================
    // Version Operations
    // ========================================================================

    /// Load versions for a document.
    async fn load_versions(&self, document_id: &str) -> Result<Vec<DocumentVersion>, DieselError> {
        let mut conn = self.pool.get().await?;

        document_versions::table
            .filter(document_versions::document_id.eq(document_id))
            .order(document_versions::version.desc())
            .load::<DocumentVersionRecord>(&mut conn)
            .await
            .map(|records| records.into_iter().map(Self::version_record_to_model).collect())
    }

    /// Add a new version.
    pub async fn add_version(&self, document_id: &str, version: &DocumentVersion) -> Result<i64, DieselError> {
        let mut conn = self.pool.get().await?;

        let version_num = version.id as i32;
        let file_path = version.file_path.to_string_lossy().to_string();
        let fetched_at = version.acquired_at.to_rfc3339();
        let file_size = version.file_size as i32;

        diesel::insert_into(document_versions::table)
            .values((
                document_versions::document_id.eq(document_id),
                document_versions::version.eq(version_num),
                document_versions::file_path.eq(Some(&file_path)),
                document_versions::content_hash.eq(Some(&version.content_hash)),
                document_versions::mime_type.eq(Some(&version.mime_type)),
                document_versions::file_size.eq(Some(file_size)),
                document_versions::fetched_at.eq(&fetched_at),
            ))
            .execute(&mut conn)
            .await?;

        diesel::sql_query("SELECT last_insert_rowid()")
            .get_result::<LastInsertRowId>(&mut conn)
            .await
            .map(|r| r.id)
    }

    /// Get latest version.
    pub async fn get_latest_version(&self, document_id: &str) -> Result<Option<DocumentVersion>, DieselError> {
        let mut conn = self.pool.get().await?;

        document_versions::table
            .filter(document_versions::document_id.eq(document_id))
            .order(document_versions::version.desc())
            .first::<DocumentVersionRecord>(&mut conn)
            .await
            .optional()
            .map(|opt| opt.map(Self::version_record_to_model))
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Count documents by source.
    pub async fn count_by_source(&self, source_id: &str) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let count: i64 = documents::table
            .filter(documents::source_id.eq(source_id))
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(count as u64)
    }

    /// Count documents by status.
    pub async fn count_by_status(&self, source_id: Option<&str>) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        let mut conn = self.pool.get().await?;

        let query = if let Some(sid) = source_id {
            format!(
                "SELECT status, COUNT(*) as count FROM documents WHERE source_id = '{}' GROUP BY status",
                sid
            )
        } else {
            "SELECT status, COUNT(*) as count FROM documents GROUP BY status".to_string()
        };

        let rows: Vec<StatusCount> = diesel::sql_query(&query).load(&mut conn).await?;

        let mut counts = std::collections::HashMap::new();
        for StatusCount { status, count } in rows {
            counts.insert(status, count as u64);
        }
        Ok(counts)
    }

    /// Get document summaries.
    pub async fn get_summaries(&self, source_id: &str, limit: u32, offset: u32) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        let mut conn = self.pool.get().await?;
        let limit = limit as i64;
        let offset = offset as i64;

        let records: Vec<DocumentRecord> = documents::table
            .filter(documents::source_id.eq(source_id))
            .order(documents::updated_at.desc())
            .limit(limit)
            .offset(offset)
            .load(&mut conn)
            .await?;

        let mut summaries = Vec::with_capacity(records.len());
        for record in records {
            let version_count: i64 = document_versions::table
                .filter(document_versions::document_id.eq(&record.id))
                .count()
                .get_result(&mut conn)
                .await?;

            let latest_size: Option<i32> = document_versions::table
                .filter(document_versions::document_id.eq(&record.id))
                .order(document_versions::version.desc())
                .select(document_versions::file_size)
                .first(&mut conn)
                .await
                .optional()?
                .flatten();

            summaries.push(DieselDocumentSummary {
                id: record.id,
                source_id: record.source_id,
                url: record.url,
                title: record.title,
                status: DocumentStatus::from_str(&record.status).unwrap_or(DocumentStatus::Pending),
                created_at: parse_datetime(&record.created_at),
                updated_at: parse_datetime(&record.updated_at),
                version_count: version_count as u32,
                latest_file_size: latest_size.map(|s| s as u64),
            });
        }

        Ok(summaries)
    }

    /// Get virtual files.
    pub async fn get_virtual_files(&self, document_id: &str, version: i32) -> Result<Vec<VirtualFile>, DieselError> {
        let mut conn = self.pool.get().await?;

        virtual_files::table
            .filter(virtual_files::document_id.eq(document_id))
            .filter(virtual_files::version.eq(version))
            .load::<VirtualFileRecord>(&mut conn)
            .await
            .map(|records| records.into_iter().map(Self::virtual_file_record_to_model).collect())
    }

    // ========================================================================
    // Additional Methods (stubs for compatibility)
    // ========================================================================

    /// Get all documents.
    pub async fn get_all(&self) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentRecord> = documents::table
            .order(documents::created_at.desc())
            .load(&mut conn)
            .await?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Get all document URLs as a HashSet.
    pub async fn get_all_urls_set(&self) -> Result<std::collections::HashSet<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        let urls: Vec<String> = documents::table
            .select(documents::url)
            .load(&mut conn)
            .await?;

        Ok(urls.into_iter().collect())
    }

    /// Get documents by tag (stub).
    pub async fn get_by_tag(&self, _tag: &str, _source_id: Option<&str>) -> Result<Vec<Document>, DieselError> {
        Ok(vec![])
    }

    /// Get documents by type category (stub).
    pub async fn get_by_type_category(&self, _category: &str, _source_id: Option<&str>, _limit: usize) -> Result<Vec<Document>, DieselError> {
        Ok(vec![])
    }

    /// Count documents needing date estimation (stub).
    pub async fn count_documents_needing_date_estimation(&self, _source_id: Option<&str>) -> Result<u64, DieselError> {
        Ok(0)
    }

    /// Get documents needing date estimation (stub).
    pub async fn get_documents_needing_date_estimation(&self, _source_id: Option<&str>, _limit: usize) -> Result<Vec<Document>, DieselError> {
        Ok(vec![])
    }

    /// Update estimated date (stub).
    pub async fn update_estimated_date(&self, _id: &str, _date: DateTime<Utc>, _confidence: &str, _source: &str) -> Result<(), DieselError> {
        Ok(())
    }

    /// Record annotation (stub).
    pub async fn record_annotation(&self, _id: &str, _annotation_type: &str, _version: i32, _data: Option<&str>, _error: Option<&str>) -> Result<(), DieselError> {
        Ok(())
    }

    /// Get URLs by source.
    pub async fn get_urls_by_source(&self, source_id: &str) -> Result<Vec<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        let urls: Vec<String> = documents::table
            .filter(documents::source_id.eq(source_id))
            .select(documents::url)
            .load(&mut conn)
            .await?;

        Ok(urls)
    }

    /// Get current version ID.
    pub async fn get_current_version_id(&self, document_id: &str) -> Result<Option<i64>, DieselError> {
        let mut conn = self.pool.get().await?;

        let version: Option<i32> = document_versions::table
            .filter(document_versions::document_id.eq(document_id))
            .order(document_versions::version.desc())
            .select(document_versions::id)
            .first(&mut conn)
            .await
            .optional()?;

        Ok(version.map(|v| v as i64))
    }

    /// Insert virtual file.
    pub async fn insert_virtual_file(&self, vf: &VirtualFile) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        diesel::insert_into(virtual_files::table)
            .values((
                virtual_files::document_id.eq(&vf.document_id),
                virtual_files::version.eq(vf.version_id as i32),
                virtual_files::path.eq(&vf.archive_path),
                virtual_files::mime_type.eq(Some(&vf.mime_type)),
                virtual_files::file_size.eq(Some(vf.file_size as i32)),
                virtual_files::status.eq(vf.status.as_str()),
                virtual_files::ocr_text.eq(&vf.extracted_text),
            ))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    /// Count unprocessed archives (stub).
    pub async fn count_unprocessed_archives(&self, _source_id: Option<&str>) -> Result<u64, DieselError> {
        Ok(0)
    }

    /// Count unprocessed emails (stub).
    pub async fn count_unprocessed_emails(&self, _source_id: Option<&str>) -> Result<u64, DieselError> {
        Ok(0)
    }

    /// Get unprocessed archives (stub).
    pub async fn get_unprocessed_archives(&self, _source_id: Option<&str>, _limit: usize) -> Result<Vec<Document>, DieselError> {
        Ok(vec![])
    }

    /// Get unprocessed emails (stub).
    pub async fn get_unprocessed_emails(&self, _source_id: Option<&str>, _limit: usize) -> Result<Vec<Document>, DieselError> {
        Ok(vec![])
    }

    /// Count all by status.
    pub async fn count_all_by_status(&self) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        self.count_by_status(None).await
    }

    /// Save page (stub). Returns the page ID.
    pub async fn save_page(&self, page: &crate::models::DocumentPage) -> Result<i64, DieselError> {
        Ok(page.id)
    }

    /// Set version page count (stub).
    pub async fn set_version_page_count(&self, _version_id: i64, _count: u32) -> Result<(), DieselError> {
        Ok(())
    }

    /// Finalize document (stub).
    pub async fn finalize_document(&self, _id: &str) -> Result<(), DieselError> {
        Ok(())
    }

    /// Count pages needing OCR (stub).
    pub async fn count_pages_needing_ocr(&self) -> Result<u64, DieselError> {
        Ok(0)
    }

    /// Get all content hashes for duplicate detection (stub).
    /// Returns (doc_id, source_id, content_hash, title) tuples
    pub async fn get_content_hashes(&self) -> Result<Vec<(String, String, String, String)>, DieselError> {
        Ok(vec![])
    }

    /// Find documents by content hash (stub).
    /// Returns (source_id, document_id, title) tuples
    pub async fn find_sources_by_hash(&self, _content_hash: &str, _exclude_source: Option<&str>) -> Result<Vec<(String, String, String)>, DieselError> {
        Ok(vec![])
    }

    /// Get all document summaries (stub).
    pub async fn get_all_summaries(&self) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        self.get_summaries("", 1000, 0).await
    }

    /// Get summaries for a specific source.
    pub async fn get_summaries_by_source(&self, source_id: &str) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        self.get_summaries(source_id, 1000, 0).await
    }

    /// Get document pages (stub).
    pub async fn get_pages(&self, _document_id: &str, _version: i32) -> Result<Vec<crate::models::DocumentPage>, DieselError> {
        Ok(vec![])
    }

    /// Get OCR results for pages in bulk (stub).
    pub async fn get_pages_ocr_results_bulk(&self, _page_ids: &[i64]) -> Result<std::collections::HashMap<i64, Vec<OcrResult>>, DieselError> {
        Ok(std::collections::HashMap::new())
    }

    /// Get pages without a specific OCR backend (stub).
    pub async fn get_pages_without_backend(&self, _document_id: &str, _backend: &str) -> Result<Vec<crate::models::DocumentPage>, DieselError> {
        Ok(vec![])
    }

    /// Store OCR result for a page (stub).
    pub async fn store_page_ocr_result(
        &self,
        _page_id: i64,
        _backend: &str,
        _text: Option<&str>,
        _confidence: Option<f32>,
        _error: Option<&str>,
    ) -> Result<(), DieselError> {
        Ok(())
    }

    /// Get documents needing summarization (stub).
    pub async fn get_needing_summarization(&self, _limit: usize) -> Result<Vec<Document>, DieselError> {
        Ok(vec![])
    }

    /// Get combined page text for a document (stub).
    pub async fn get_combined_page_text(&self, _document_id: &str, _version: i32) -> Result<Option<String>, DieselError> {
        Ok(None)
    }

    /// Finalize pending documents (stub).
    pub async fn finalize_pending_documents(&self) -> Result<u64, DieselError> {
        Ok(0)
    }

    /// Get documents needing OCR (stub).
    pub async fn get_needing_ocr(&self, _limit: usize) -> Result<Vec<Document>, DieselError> {
        Ok(vec![])
    }

    /// Update version mime type (stub).
    pub async fn update_version_mime_type(&self, _version_id: i64, _mime_type: &str) -> Result<(), DieselError> {
        Ok(())
    }

    /// Get pages needing OCR (stub).
    pub async fn get_pages_needing_ocr(&self, _document_id: &str, _version_id: i32, _limit: usize) -> Result<Vec<crate::models::DocumentPage>, DieselError> {
        Ok(vec![])
    }

    /// Delete pages (stub).
    pub async fn delete_pages(&self, _document_id: &str, _version_id: i32) -> Result<(), DieselError> {
        Ok(())
    }

    /// Check if all pages are complete (stub).
    pub async fn are_all_pages_complete(&self, _document_id: &str, _version_id: i32) -> Result<bool, DieselError> {
        Ok(true)
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    fn record_to_document(record: DocumentRecord, versions: Vec<DocumentVersion>) -> Document {
        Document {
            id: record.id,
            source_id: record.source_id,
            title: record.title.unwrap_or_default(),
            source_url: record.url,
            extracted_text: None,
            synopsis: None,
            tags: vec![],
            status: DocumentStatus::from_str(&record.status).unwrap_or(DocumentStatus::Pending),
            metadata: serde_json::from_str(&record.metadata)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            created_at: parse_datetime(&record.created_at),
            updated_at: parse_datetime(&record.updated_at),
            discovery_method: "unknown".to_string(),
            versions,
        }
    }

    fn version_record_to_model(record: DocumentVersionRecord) -> DocumentVersion {
        DocumentVersion {
            id: record.id as i64,
            content_hash: record.content_hash.unwrap_or_default(),
            file_path: PathBuf::from(record.file_path.unwrap_or_default()),
            file_size: record.file_size.unwrap_or(0) as u64,
            mime_type: record.mime_type.unwrap_or_default(),
            acquired_at: parse_datetime(&record.fetched_at),
            source_url: None,
            original_filename: None,
            server_date: None,
            page_count: None,
        }
    }

    fn virtual_file_record_to_model(record: VirtualFileRecord) -> VirtualFile {
        VirtualFile {
            id: record.id.to_string(),
            document_id: record.document_id,
            version_id: record.version as i64,
            archive_path: record.path,
            filename: String::new(),
            file_size: record.file_size.unwrap_or(0) as u64,
            mime_type: record.mime_type.unwrap_or_default(),
            extracted_text: record.ocr_text,
            synopsis: None,
            tags: vec![],
            status: VirtualFileStatus::from_str(&record.status).unwrap_or(VirtualFileStatus::Pending),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

// Helper structs for SQL queries
#[derive(diesel::QueryableByName)]
struct StatusCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(diesel::QueryableByName)]
struct SourceCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    source_id: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(diesel::QueryableByName)]
struct LastInsertRowId {
    #[diesel(sql_type = diesel::sql_types::BigInt, column_name = "last_insert_rowid()")]
    id: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel_async::SimpleAsyncConnection;
    use tempfile::tempdir;

    async fn setup_test_db() -> (AsyncSqlitePool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_url = db_path.display().to_string();

        let pool = AsyncSqlitePool::new(&db_url, 5);
        let mut conn = pool.get().await.unwrap();

        conn.batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                url TEXT NOT NULL,
                title TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS document_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                file_path TEXT,
                content_hash TEXT,
                mime_type TEXT,
                file_size INTEGER,
                fetched_at TEXT NOT NULL,
                UNIQUE(document_id, version)
            );

            CREATE TABLE IF NOT EXISTS document_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                page_number INTEGER NOT NULL,
                text_content TEXT,
                ocr_text TEXT,
                has_images INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                UNIQUE(document_id, version, page_number)
            );

            CREATE TABLE IF NOT EXISTS virtual_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                path TEXT NOT NULL,
                mime_type TEXT,
                file_size INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                ocr_text TEXT,
                UNIQUE(document_id, version, path)
            );
            "#,
        )
        .await
        .unwrap();

        (pool, dir)
    }

    #[tokio::test]
    async fn test_document_crud() {
        let (pool, dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool, dir.path().to_path_buf());

        let doc = Document {
            id: "doc-1".to_string(),
            source_id: "test-source".to_string(),
            title: "Test Document".to_string(),
            source_url: "https://example.com/doc.pdf".to_string(),
            extracted_text: None,
            synopsis: None,
            tags: vec![],
            status: DocumentStatus::Pending,
            metadata: serde_json::Value::Object(Default::default()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            discovery_method: "seed".to_string(),
            versions: vec![],
        };

        repo.save(&doc).await.unwrap();
        assert!(repo.exists("doc-1").await.unwrap());

        let fetched = repo.get("doc-1").await.unwrap().unwrap();
        assert_eq!(fetched.title, "Test Document");

        repo.update_status("doc-1", DocumentStatus::Downloaded).await.unwrap();
        let updated = repo.get("doc-1").await.unwrap().unwrap();
        assert_eq!(updated.status, DocumentStatus::Downloaded);

        let deleted = repo.delete("doc-1").await.unwrap();
        assert!(deleted);
        assert!(!repo.exists("doc-1").await.unwrap());
    }

    #[tokio::test]
    async fn test_document_versions() {
        let (pool, dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool, dir.path().to_path_buf());

        let doc = Document {
            id: "doc-2".to_string(),
            source_id: "test-source".to_string(),
            title: "Versioned Doc".to_string(),
            source_url: "https://example.com/versioned.pdf".to_string(),
            extracted_text: None,
            synopsis: None,
            tags: vec![],
            status: DocumentStatus::Pending,
            metadata: serde_json::Value::Object(Default::default()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            discovery_method: "seed".to_string(),
            versions: vec![],
        };
        repo.save(&doc).await.unwrap();

        let version = DocumentVersion {
            id: 1,
            content_hash: "abc123".to_string(),
            file_path: PathBuf::from("/tmp/test.pdf"),
            file_size: 1024,
            mime_type: "application/pdf".to_string(),
            acquired_at: Utc::now(),
            source_url: None,
            original_filename: None,
            server_date: None,
            page_count: None,
        };
        repo.add_version("doc-2", &version).await.unwrap();

        let latest = repo.get_latest_version("doc-2").await.unwrap().unwrap();
        assert_eq!(latest.content_hash, "abc123");
        assert_eq!(latest.file_size, 1024);
    }
}
