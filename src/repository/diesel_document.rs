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
        Self {
            pool,
            documents_dir,
        }
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
    pub async fn get_all_source_counts(
        &self,
    ) -> Result<std::collections::HashMap<String, u64>, DieselError> {
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

    /// Count documents needing OCR.
    /// Documents need OCR if status is 'pending' or 'downloaded' and they have a PDF version.
    pub async fn count_needing_ocr(&self, source_id: Option<&str>) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        let mut query = documents::table
            .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
            .into_boxed();

        if let Some(sid) = source_id {
            query = query.filter(documents::source_id.eq(sid));
        }

        let count: i64 = query.count().get_result(&mut conn).await?;

        Ok(count as u64)
    }

    /// Count documents needing summarization.
    /// Documents need summarization if status is 'ocr_complete' (OCR done but not indexed).
    pub async fn count_needing_summarization(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        let mut query = documents::table
            .filter(documents::status.eq("ocr_complete"))
            .into_boxed();

        if let Some(sid) = source_id {
            query = query.filter(documents::source_id.eq(sid));
        }

        let count: i64 = query.count().get_result(&mut conn).await?;

        Ok(count as u64)
    }

    /// Get type statistics - count documents by MIME type.
    pub async fn get_type_stats(
        &self,
    ) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        let mut conn = self.pool.get().await?;

        // Get counts per mime type from document_versions (latest version per document)
        let results: Vec<MimeCount> = diesel_async::RunQueryDsl::load(
            diesel::sql_query(
                r#"SELECT COALESCE(dv.mime_type, 'unknown') as mime_type, COUNT(DISTINCT dv.document_id) as count
                   FROM document_versions dv
                   INNER JOIN (
                       SELECT document_id, MAX(id) as max_id
                       FROM document_versions
                       GROUP BY document_id
                   ) latest ON dv.document_id = latest.document_id AND dv.id = latest.max_id
                   GROUP BY dv.mime_type"#
            ),
            &mut conn,
        ).await?;

        let mut stats = std::collections::HashMap::new();
        for row in results {
            stats.insert(row.mime_type, row.count as u64);
        }
        Ok(stats)
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

    /// Get category statistics - group MIME types into categories.
    pub async fn get_category_stats(
        &self,
    ) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        let type_stats = self.get_type_stats().await?;
        let mut category_stats = std::collections::HashMap::new();

        for (mime, count) in type_stats {
            let category = crate::utils::mime_to_category(&mime).to_string();
            *category_stats.entry(category).or_insert(0) += count;
        }

        Ok(category_stats)
    }

    /// Search tags by prefix in document metadata.
    /// Tags are stored as JSON arrays in the metadata field.
    pub async fn search_tags(&self, query: &str) -> Result<Vec<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        // SQLite JSON extraction to find tags matching the query
        let pattern = format!("%{}%", query.to_lowercase());
        let results: Vec<TagRow> = diesel_async::RunQueryDsl::load(
            diesel::sql_query(
                r#"SELECT DISTINCT value as tag
                   FROM documents, json_each(json_extract(metadata, '$.tags'))
                   WHERE LOWER(value) LIKE ?
                   ORDER BY value
                   LIMIT 100"#,
            )
            .bind::<diesel::sql_types::Text, _>(&pattern),
            &mut conn,
        )
        .await
        .unwrap_or_default();

        Ok(results.into_iter().map(|r| r.tag).collect())
    }

    /// Get all unique tags from document metadata.
    pub async fn get_all_tags(&self) -> Result<Vec<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        let results: Vec<TagRow> = diesel_async::RunQueryDsl::load(
            diesel::sql_query(
                r#"SELECT DISTINCT value as tag
                   FROM documents, json_each(json_extract(metadata, '$.tags'))
                   ORDER BY value"#,
            ),
            &mut conn,
        )
        .await
        .unwrap_or_default();

        Ok(results.into_iter().map(|r| r.tag).collect())
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
        let prev: Option<(String, String)> = documents::table
            .select((documents::id, documents::title))
            .filter(documents::source_id.eq(source_id))
            .filter(documents::id.lt(document_id))
            .order(documents::id.desc())
            .first(&mut conn)
            .await
            .optional()?;

        // Get next document
        let next: Option<(String, String)> = documents::table
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
            prev_title: prev.map(|(_, title)| title),
            next_id: next.as_ref().map(|(id, _)| id.clone()),
            next_title: next.map(|(_, title)| title),
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
            .filter(document_pages::version_id.eq(version))
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
            .filter(documents::source_url.eq(url))
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
                documents::source_url.eq(&doc.source_url),
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
                diesel::delete(
                    document_versions::table.filter(document_versions::document_id.eq(id)),
                )
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
            .order(document_versions::id.desc())
            .load::<DocumentVersionRecord>(&mut conn)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(Self::version_record_to_model)
                    .collect()
            })
    }

    /// Add a new version.
    pub async fn add_version(
        &self,
        document_id: &str,
        version: &DocumentVersion,
    ) -> Result<i64, DieselError> {
        let mut conn = self.pool.get().await?;

        let file_path = version.file_path.to_string_lossy().to_string();
        let acquired_at = version.acquired_at.to_rfc3339();
        let file_size = version.file_size as i32;

        diesel::insert_into(document_versions::table)
            .values((
                document_versions::document_id.eq(document_id),
                document_versions::content_hash.eq(&version.content_hash),
                document_versions::file_path.eq(&file_path),
                document_versions::file_size.eq(file_size),
                document_versions::mime_type.eq(&version.mime_type),
                document_versions::acquired_at.eq(&acquired_at),
                document_versions::source_url.eq(version.source_url.as_deref()),
                document_versions::original_filename.eq(version.original_filename.as_deref()),
                document_versions::server_date
                    .eq(version.server_date.map(|d| d.to_rfc3339()).as_deref()),
                document_versions::page_count.eq(version.page_count.map(|c| c as i32)),
            ))
            .execute(&mut conn)
            .await?;

        diesel::sql_query("SELECT last_insert_rowid()")
            .get_result::<LastInsertRowId>(&mut conn)
            .await
            .map(|r| r.id)
    }

    /// Get latest version.
    pub async fn get_latest_version(
        &self,
        document_id: &str,
    ) -> Result<Option<DocumentVersion>, DieselError> {
        let mut conn = self.pool.get().await?;

        document_versions::table
            .filter(document_versions::document_id.eq(document_id))
            .order(document_versions::id.desc())
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
    pub async fn count_by_status(
        &self,
        source_id: Option<&str>,
    ) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        let mut conn = self.pool.get().await?;

        let query = if let Some(sid) = source_id {
            format!(
                "SELECT status, COUNT(*) as count FROM documents WHERE source_id = '{}' GROUP BY status",
                sid
            )
        } else {
            "SELECT status, COUNT(*) as count FROM documents GROUP BY status".to_string()
        };

        let rows: Vec<StatusCount> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;

        let mut counts = std::collections::HashMap::new();
        for StatusCount { status, count } in rows {
            counts.insert(status, count as u64);
        }
        Ok(counts)
    }

    /// Get document summaries.
    pub async fn get_summaries(
        &self,
        source_id: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<DieselDocumentSummary>, DieselError> {
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
                .order(document_versions::id.desc())
                .select(document_versions::file_size)
                .first(&mut conn)
                .await
                .optional()?;

            summaries.push(DieselDocumentSummary {
                id: record.id,
                source_id: record.source_id,
                url: record.source_url,
                title: Some(record.title),
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
    pub async fn get_virtual_files(
        &self,
        document_id: &str,
        version: i32,
    ) -> Result<Vec<VirtualFile>, DieselError> {
        let mut conn = self.pool.get().await?;

        virtual_files::table
            .filter(virtual_files::document_id.eq(document_id))
            .filter(virtual_files::version_id.eq(version))
            .load::<VirtualFileRecord>(&mut conn)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(Self::virtual_file_record_to_model)
                    .collect()
            })
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
            .select(documents::source_url)
            .load(&mut conn)
            .await?;

        Ok(urls.into_iter().collect())
    }

    /// Get documents by tag.
    /// Tags are stored in metadata JSON.
    pub async fn get_by_tag(
        &self,
        tag: &str,
        source_id: Option<&str>,
    ) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        // Use JSON function to find documents with matching tag
        let query = if let Some(sid) = source_id {
            format!(
                r#"SELECT id FROM documents
                   WHERE source_id = '{}'
                   AND EXISTS (
                       SELECT 1 FROM json_each(json_extract(metadata, '$.tags'))
                       WHERE value = '{}'
                   )
                   ORDER BY updated_at DESC"#,
                sid.replace('\'', "''"),
                tag.replace('\'', "''")
            )
        } else {
            format!(
                r#"SELECT id FROM documents
                   WHERE EXISTS (
                       SELECT 1 FROM json_each(json_extract(metadata, '$.tags'))
                       WHERE value = '{}'
                   )
                   ORDER BY updated_at DESC"#,
                tag.replace('\'', "''")
            )
        };

        let ids: Vec<DocIdRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                .await
                .unwrap_or_default();

        let mut docs = Vec::with_capacity(ids.len());
        for row in ids {
            if let Ok(Some(doc)) = self.get(&row.id).await {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Get documents by MIME type category.
    pub async fn get_by_type_category(
        &self,
        category: &str,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        // Get MIME types for this category
        let mime_patterns = crate::utils::category_to_mime_patterns(category);
        if mime_patterns.is_empty() {
            return Ok(vec![]);
        }

        // Build query with MIME type filters
        let mime_conditions: Vec<String> = mime_patterns
            .iter()
            .map(|p| format!("dv.mime_type LIKE '{}'", p.replace('\'', "''")))
            .collect();

        let source_filter = source_id
            .map(|s| format!("AND d.source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let query = format!(
            r#"SELECT DISTINCT d.id
               FROM documents d
               JOIN document_versions dv ON d.id = dv.document_id
               WHERE ({})
               {}
               ORDER BY d.updated_at DESC
               LIMIT {}"#,
            mime_conditions.join(" OR "),
            source_filter,
            limit
        );

        let ids: Vec<DocIdRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                .await
                .unwrap_or_default();

        let mut docs = Vec::with_capacity(ids.len());
        for row in ids {
            if let Ok(Some(doc)) = self.get(&row.id).await {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Count documents needing date estimation.
    /// These are documents without an estimated_date in metadata.
    pub async fn count_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        let source_filter = source_id
            .map(|s| format!("AND source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let query = format!(
            r#"SELECT COUNT(*) as count FROM documents
               WHERE json_extract(metadata, '$.estimated_date') IS NULL
               {}"#,
            source_filter
        );

        let result: Vec<CountRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                .await
                .unwrap_or_default();

        Ok(result.get(0).map(|r| r.count as u64).unwrap_or(0))
    }

    /// Get documents needing date estimation.
    pub async fn get_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let source_filter = source_id
            .map(|s| format!("AND source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let query = format!(
            r#"SELECT id FROM documents
               WHERE json_extract(metadata, '$.estimated_date') IS NULL
               {}
               LIMIT {}"#,
            source_filter, limit
        );

        let ids: Vec<DocIdRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                .await
                .unwrap_or_default();

        let mut docs = Vec::with_capacity(ids.len());
        for row in ids {
            if let Ok(Some(doc)) = self.get(&row.id).await {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Update estimated date in document metadata.
    pub async fn update_estimated_date(
        &self,
        id: &str,
        date: DateTime<Utc>,
        confidence: &str,
        source: &str,
    ) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        // Get current metadata, add estimated_date, update
        let record: Option<DocumentRecord> = documents::table
            .find(id)
            .first(&mut conn)
            .await
            .optional()?;

        if let Some(record) = record {
            let mut metadata: serde_json::Value =
                serde_json::from_str(&record.metadata).unwrap_or(serde_json::json!({}));

            metadata["estimated_date"] = serde_json::json!({
                "date": date.to_rfc3339(),
                "confidence": confidence,
                "source": source,
            });

            let now = Utc::now().to_rfc3339();
            diesel::update(documents::table.find(id))
                .set((
                    documents::metadata.eq(metadata.to_string()),
                    documents::updated_at.eq(&now),
                ))
                .execute(&mut conn)
                .await?;
        }

        Ok(())
    }

    /// Record an annotation result in document metadata.
    pub async fn record_annotation(
        &self,
        id: &str,
        annotation_type: &str,
        version: i32,
        data: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        let record: Option<DocumentRecord> = documents::table
            .find(id)
            .first(&mut conn)
            .await
            .optional()?;

        if let Some(record) = record {
            let mut metadata: serde_json::Value =
                serde_json::from_str(&record.metadata).unwrap_or(serde_json::json!({}));

            // Add annotation info
            let annotations = metadata
                .as_object_mut()
                .unwrap()
                .entry("annotations")
                .or_insert(serde_json::json!({}));

            annotations[annotation_type] = serde_json::json!({
                "version": version,
                "data": data,
                "error": error,
                "timestamp": Utc::now().to_rfc3339(),
            });

            let now = Utc::now().to_rfc3339();
            diesel::update(documents::table.find(id))
                .set((
                    documents::metadata.eq(metadata.to_string()),
                    documents::updated_at.eq(&now),
                ))
                .execute(&mut conn)
                .await?;
        }

        Ok(())
    }

    /// Get URLs by source.
    pub async fn get_urls_by_source(&self, source_id: &str) -> Result<Vec<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        let urls: Vec<String> = documents::table
            .filter(documents::source_id.eq(source_id))
            .select(documents::source_url)
            .load(&mut conn)
            .await?;

        Ok(urls)
    }

    /// Get current version ID.
    pub async fn get_current_version_id(
        &self,
        document_id: &str,
    ) -> Result<Option<i64>, DieselError> {
        let mut conn = self.pool.get().await?;

        let version: Option<i32> = document_versions::table
            .filter(document_versions::document_id.eq(document_id))
            .order(document_versions::id.desc())
            .select(document_versions::id)
            .first(&mut conn)
            .await
            .optional()?;

        Ok(version.map(|v| v as i64))
    }

    /// Insert virtual file.
    pub async fn insert_virtual_file(&self, vf: &VirtualFile) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        let now = Utc::now().to_rfc3339();

        diesel::insert_into(virtual_files::table)
            .values((
                virtual_files::id.eq(&vf.id),
                virtual_files::document_id.eq(&vf.document_id),
                virtual_files::version_id.eq(vf.version_id as i32),
                virtual_files::archive_path.eq(&vf.archive_path),
                virtual_files::filename.eq(&vf.filename),
                virtual_files::mime_type.eq(&vf.mime_type),
                virtual_files::file_size.eq(vf.file_size as i32),
                virtual_files::extracted_text.eq(&vf.extracted_text),
                virtual_files::synopsis.eq(&vf.synopsis),
                virtual_files::tags.eq(serde_json::to_string(&vf.tags).ok().as_deref()),
                virtual_files::status.eq(vf.status.as_str()),
                virtual_files::created_at.eq(&now),
                virtual_files::updated_at.eq(&now),
            ))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    /// Count unprocessed archives.
    pub async fn count_unprocessed_archives(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        let source_filter = source_id
            .map(|s| format!("AND d.source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let query = format!(
            r#"SELECT COUNT(DISTINCT d.id) as count
               FROM documents d
               JOIN document_versions dv ON d.id = dv.document_id
               WHERE d.status IN ('pending', 'downloaded')
               AND (dv.mime_type = 'application/zip'
                    OR dv.mime_type = 'application/x-zip'
                    OR dv.mime_type = 'application/x-zip-compressed'
                    OR dv.mime_type = 'application/x-tar'
                    OR dv.mime_type = 'application/gzip'
                    OR dv.mime_type = 'application/x-rar-compressed'
                    OR dv.mime_type = 'application/x-7z-compressed')
               {}"#,
            source_filter
        );

        let result: Vec<CountRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;
        Ok(result.get(0).map(|r| r.count as u64).unwrap_or(0))
    }

    /// Count unprocessed emails.
    pub async fn count_unprocessed_emails(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        let source_filter = source_id
            .map(|s| format!("AND d.source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let query = format!(
            r#"SELECT COUNT(DISTINCT d.id) as count
               FROM documents d
               JOIN document_versions dv ON d.id = dv.document_id
               WHERE d.status IN ('pending', 'downloaded')
               AND (dv.mime_type LIKE 'message/%' OR dv.mime_type LIKE '%rfc822%')
               {}"#,
            source_filter
        );

        let result: Vec<CountRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;
        Ok(result.get(0).map(|r| r.count as u64).unwrap_or(0))
    }

    /// Get unprocessed archives.
    pub async fn get_unprocessed_archives(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let source_filter = source_id
            .map(|s| format!("AND d.source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let query = format!(
            r#"SELECT DISTINCT d.id
               FROM documents d
               JOIN document_versions dv ON d.id = dv.document_id
               WHERE d.status IN ('pending', 'downloaded')
               AND (dv.mime_type = 'application/zip'
                    OR dv.mime_type = 'application/x-zip'
                    OR dv.mime_type = 'application/x-zip-compressed'
                    OR dv.mime_type = 'application/x-tar'
                    OR dv.mime_type = 'application/gzip'
                    OR dv.mime_type = 'application/x-rar-compressed'
                    OR dv.mime_type = 'application/x-7z-compressed')
               {}
               ORDER BY d.updated_at ASC
               LIMIT {}"#,
            source_filter, limit
        );

        let ids: Vec<DocIdRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;

        let mut docs = Vec::with_capacity(ids.len());
        for row in ids {
            if let Ok(Some(doc)) = self.get(&row.id).await {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Get unprocessed emails.
    pub async fn get_unprocessed_emails(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let source_filter = source_id
            .map(|s| format!("AND d.source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let query = format!(
            r#"SELECT DISTINCT d.id
               FROM documents d
               JOIN document_versions dv ON d.id = dv.document_id
               WHERE d.status IN ('pending', 'downloaded')
               AND (dv.mime_type LIKE 'message/%' OR dv.mime_type LIKE '%rfc822%')
               {}
               ORDER BY d.updated_at ASC
               LIMIT {}"#,
            source_filter, limit
        );

        let ids: Vec<DocIdRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;

        let mut docs = Vec::with_capacity(ids.len());
        for row in ids {
            if let Ok(Some(doc)) = self.get(&row.id).await {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    /// Count all by status.
    pub async fn count_all_by_status(
        &self,
    ) -> Result<std::collections::HashMap<String, u64>, DieselError> {
        self.count_by_status(None).await
    }

    /// Save a document page. Returns the page ID.
    pub async fn save_page(&self, page: &crate::models::DocumentPage) -> Result<i64, DieselError> {
        let mut conn = self.pool.get().await?;

        let now = Utc::now().to_rfc3339();

        // Use replace_into for upsert
        diesel::replace_into(document_pages::table)
            .values((
                document_pages::document_id.eq(&page.document_id),
                document_pages::version_id.eq(page.version_id as i32),
                document_pages::page_number.eq(page.page_number as i32),
                document_pages::pdf_text.eq(&page.pdf_text),
                document_pages::ocr_text.eq(&page.ocr_text),
                document_pages::final_text.eq(&page.final_text),
                document_pages::ocr_status.eq(page.ocr_status.as_str()),
                document_pages::created_at.eq(&now),
                document_pages::updated_at.eq(&now),
            ))
            .execute(&mut conn)
            .await?;

        // Get the row id
        let result: LastInsertRowId = diesel::sql_query("SELECT last_insert_rowid()")
            .get_result(&mut conn)
            .await?;
        Ok(result.id)
    }

    /// Set version page count.
    /// Note: page_count is not stored in the database schema, so this is a no-op.
    /// The count can be derived from document_pages table.
    pub async fn set_version_page_count(
        &self,
        _version_id: i64,
        _count: u32,
    ) -> Result<(), DieselError> {
        // Page count is derived from document_pages, not stored directly
        Ok(())
    }

    /// Finalize document - mark as indexed.
    pub async fn finalize_document(&self, id: &str) -> Result<(), DieselError> {
        self.update_status(id, DocumentStatus::Indexed).await
    }

    /// Count pages needing OCR across all documents.
    pub async fn count_pages_needing_ocr(&self) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let count: i64 = document_pages::table
            .filter(
                document_pages::ocr_status
                    .eq("pending")
                    .or(document_pages::ocr_status.eq("text_extracted")),
            )
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(count as u64)
    }

    /// Get all content hashes for duplicate detection.
    /// Returns (doc_id, source_id, content_hash, title) tuples
    pub async fn get_content_hashes(
        &self,
    ) -> Result<Vec<(String, String, String, String)>, DieselError> {
        let mut conn = self.pool.get().await?;

        #[derive(diesel::QueryableByName)]
        struct HashRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            document_id: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            source_id: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            content_hash: String,
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            title: Option<String>,
        }

        let results: Vec<HashRow> = diesel::sql_query(
            r#"SELECT dv.document_id, d.source_id, dv.content_hash, d.title
               FROM document_versions dv
               JOIN documents d ON dv.document_id = d.id
               WHERE dv.content_hash IS NOT NULL
               AND dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = dv.document_id)"#
        ).load(&mut conn).await?;

        Ok(results
            .into_iter()
            .map(|r| {
                (
                    r.document_id,
                    r.source_id,
                    r.content_hash,
                    r.title.unwrap_or_default(),
                )
            })
            .collect())
    }

    /// Find documents by content hash.
    /// Returns (source_id, document_id, title) tuples
    pub async fn find_sources_by_hash(
        &self,
        content_hash: &str,
        exclude_source: Option<&str>,
    ) -> Result<Vec<(String, String, String)>, DieselError> {
        let mut conn = self.pool.get().await?;

        #[derive(diesel::QueryableByName)]
        struct SourceRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            source_id: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            document_id: String,
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            title: Option<String>,
        }

        let query = if let Some(exclude) = exclude_source {
            format!(
                r#"SELECT d.source_id, d.id as document_id, d.title
                   FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE dv.content_hash = '{}'
                   AND d.source_id != '{}'"#,
                content_hash.replace('\'', "''"),
                exclude.replace('\'', "''")
            )
        } else {
            format!(
                r#"SELECT d.source_id, d.id as document_id, d.title
                   FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE dv.content_hash = '{}'"#,
                content_hash.replace('\'', "''")
            )
        };

        let results: Vec<SourceRow> =
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;

        Ok(results
            .into_iter()
            .map(|r| (r.source_id, r.document_id, r.title.unwrap_or_default()))
            .collect())
    }

    /// Get all document summaries.
    pub async fn get_all_summaries(&self) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentRecord> = documents::table
            .order(documents::updated_at.desc())
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
                .order(document_versions::id.desc())
                .select(document_versions::file_size)
                .first(&mut conn)
                .await
                .optional()?;

            summaries.push(DieselDocumentSummary {
                id: record.id,
                source_id: record.source_id,
                url: record.source_url,
                title: Some(record.title),
                status: DocumentStatus::from_str(&record.status).unwrap_or(DocumentStatus::Pending),
                created_at: parse_datetime(&record.created_at),
                updated_at: parse_datetime(&record.updated_at),
                version_count: version_count as u32,
                latest_file_size: latest_size.map(|s| s as u64),
            });
        }

        Ok(summaries)
    }

    /// Get summaries for a specific source.
    pub async fn get_summaries_by_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        self.get_summaries(source_id, 1000, 0).await
    }

    /// Get document pages.
    pub async fn get_pages(
        &self,
        document_id: &str,
        version: i32,
    ) -> Result<Vec<crate::models::DocumentPage>, DieselError> {
        use super::diesel_models::DocumentPageRecord;
        use crate::models::PageOcrStatus;

        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentPageRecord> = document_pages::table
            .filter(document_pages::document_id.eq(document_id))
            .filter(document_pages::version_id.eq(version))
            .order(document_pages::page_number.asc())
            .load(&mut conn)
            .await?;

        Ok(records
            .into_iter()
            .map(|r| crate::models::DocumentPage {
                id: r.id as i64,
                document_id: r.document_id,
                version_id: r.version_id as i64,
                page_number: r.page_number as u32,
                pdf_text: r.pdf_text,
                ocr_text: r.ocr_text,
                final_text: None,
                ocr_status: PageOcrStatus::from_str(&r.ocr_status)
                    .unwrap_or(PageOcrStatus::Pending),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .collect())
    }

    /// Get OCR results for pages in bulk (stub).
    pub async fn get_pages_ocr_results_bulk(
        &self,
        _page_ids: &[i64],
    ) -> Result<std::collections::HashMap<i64, Vec<OcrResult>>, DieselError> {
        Ok(std::collections::HashMap::new())
    }

    /// Get pages without a specific OCR backend (stub).
    pub async fn get_pages_without_backend(
        &self,
        _document_id: &str,
        _backend: &str,
    ) -> Result<Vec<crate::models::DocumentPage>, DieselError> {
        Ok(vec![])
    }

    /// Store OCR result for a page.
    /// Updates the ocr_text and status fields on the page.
    pub async fn store_page_ocr_result(
        &self,
        page_id: i64,
        _backend: &str,
        text: Option<&str>,
        _confidence: Option<f32>,
        error: Option<&str>,
    ) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        let status = if error.is_some() {
            "failed"
        } else {
            "ocr_complete"
        };

        diesel::update(document_pages::table.find(page_id as i32))
            .set((
                document_pages::ocr_text.eq(text),
                document_pages::ocr_status.eq(status),
            ))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    /// Get documents needing summarization.
    pub async fn get_needing_summarization(
        &self,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentRecord> = documents::table
            .filter(documents::status.eq("ocr_complete"))
            .order(documents::updated_at.asc())
            .limit(limit as i64)
            .load(&mut conn)
            .await?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Get combined page text for a document.
    pub async fn get_combined_page_text(
        &self,
        document_id: &str,
        version: i32,
    ) -> Result<Option<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        let texts: Vec<Option<String>> = document_pages::table
            .filter(document_pages::document_id.eq(document_id))
            .filter(document_pages::version_id.eq(version))
            .order(document_pages::page_number.asc())
            .select(document_pages::ocr_text)
            .load(&mut conn)
            .await?;

        let combined: String = texts.into_iter().flatten().collect::<Vec<_>>().join("\n\n");

        if combined.is_empty() {
            Ok(None)
        } else {
            Ok(Some(combined))
        }
    }

    /// Finalize pending documents - mark documents with all pages complete as indexed.
    pub async fn finalize_pending_documents(&self) -> Result<u64, DieselError> {
        let mut conn = self.pool.get().await?;

        // Find documents with status 'ocr_complete' that can be finalized
        let doc_ids: Vec<String> = documents::table
            .filter(documents::status.eq("ocr_complete"))
            .select(documents::id)
            .load(&mut conn)
            .await?;

        let mut count = 0u64;
        for doc_id in doc_ids {
            self.update_status(&doc_id, DocumentStatus::Indexed).await?;
            count += 1;
        }

        Ok(count)
    }

    /// Get documents needing OCR.
    pub async fn get_needing_ocr(&self, limit: usize) -> Result<Vec<Document>, DieselError> {
        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentRecord> = documents::table
            .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
            .order(documents::updated_at.asc())
            .limit(limit as i64)
            .load(&mut conn)
            .await?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Update version mime type.
    pub async fn update_version_mime_type(
        &self,
        version_id: i64,
        mime_type: &str,
    ) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        diesel::update(document_versions::table.find(version_id as i32))
            .set(document_versions::mime_type.eq(mime_type))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    /// Get pages needing OCR.
    pub async fn get_pages_needing_ocr(
        &self,
        document_id: &str,
        version_id: i32,
        limit: usize,
    ) -> Result<Vec<crate::models::DocumentPage>, DieselError> {
        use super::diesel_models::DocumentPageRecord;
        use crate::models::PageOcrStatus;

        let mut conn = self.pool.get().await?;

        let records: Vec<DocumentPageRecord> = document_pages::table
            .filter(document_pages::document_id.eq(document_id))
            .filter(document_pages::version_id.eq(version_id))
            .filter(
                document_pages::ocr_status
                    .eq("pending")
                    .or(document_pages::ocr_status.eq("text_extracted")),
            )
            .order(document_pages::page_number.asc())
            .limit(limit as i64)
            .load(&mut conn)
            .await?;

        Ok(records
            .into_iter()
            .map(|r| crate::models::DocumentPage {
                id: r.id as i64,
                document_id: r.document_id,
                version_id: r.version_id as i64,
                page_number: r.page_number as u32,
                pdf_text: r.pdf_text,
                ocr_text: r.ocr_text,
                final_text: None,
                ocr_status: PageOcrStatus::from_str(&r.ocr_status)
                    .unwrap_or(PageOcrStatus::Pending),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .collect())
    }

    /// Delete pages for a document version.
    pub async fn delete_pages(
        &self,
        document_id: &str,
        version_id: i32,
    ) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        diesel::delete(
            document_pages::table
                .filter(document_pages::document_id.eq(document_id))
                .filter(document_pages::version_id.eq(version_id)),
        )
        .execute(&mut conn)
        .await?;

        Ok(())
    }

    /// Check if all pages are complete.
    pub async fn are_all_pages_complete(
        &self,
        document_id: &str,
        version_id: i32,
    ) -> Result<bool, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let pending_count: i64 = document_pages::table
            .filter(document_pages::document_id.eq(document_id))
            .filter(document_pages::version_id.eq(version_id))
            .filter(
                document_pages::ocr_status
                    .eq("pending")
                    .or(document_pages::ocr_status.eq("text_extracted")),
            )
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(pending_count == 0)
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    fn record_to_document(record: DocumentRecord, versions: Vec<DocumentVersion>) -> Document {
        Document {
            id: record.id,
            source_id: record.source_id,
            title: record.title,
            source_url: record.source_url,
            extracted_text: record.extracted_text,
            synopsis: record.synopsis,
            tags: record
                .tags
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            status: DocumentStatus::from_str(&record.status).unwrap_or(DocumentStatus::Pending),
            metadata: serde_json::from_str(&record.metadata)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            created_at: parse_datetime(&record.created_at),
            updated_at: parse_datetime(&record.updated_at),
            discovery_method: record.discovery_method,
            versions,
        }
    }

    fn version_record_to_model(record: DocumentVersionRecord) -> DocumentVersion {
        DocumentVersion {
            id: record.id as i64,
            content_hash: record.content_hash,
            file_path: PathBuf::from(record.file_path),
            file_size: record.file_size as u64,
            mime_type: record.mime_type,
            acquired_at: parse_datetime(&record.acquired_at),
            source_url: record.source_url,
            original_filename: record.original_filename,
            server_date: parse_datetime_opt(record.server_date),
            page_count: record.page_count.map(|c| c as u32),
        }
    }

    fn virtual_file_record_to_model(record: VirtualFileRecord) -> VirtualFile {
        VirtualFile {
            id: record.id,
            document_id: record.document_id,
            version_id: record.version_id as i64,
            archive_path: record.archive_path,
            filename: record.filename,
            file_size: record.file_size as u64,
            mime_type: record.mime_type,
            extracted_text: record.extracted_text,
            synopsis: record.synopsis,
            tags: record
                .tags
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            status: VirtualFileStatus::from_str(&record.status)
                .unwrap_or(VirtualFileStatus::Pending),
            created_at: parse_datetime(&record.created_at),
            updated_at: parse_datetime(&record.updated_at),
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
struct MimeCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    mime_type: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(diesel::QueryableByName)]
struct TagRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    tag: String,
}

#[derive(diesel::QueryableByName)]
struct DocIdRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    id: String,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
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
                title TEXT NOT NULL,
                source_url TEXT NOT NULL,
                extracted_text TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                synopsis TEXT,
                tags TEXT,
                estimated_date TEXT,
                date_confidence TEXT,
                date_source TEXT,
                manual_date TEXT,
                discovery_method TEXT NOT NULL DEFAULT 'import',
                category_id TEXT
            );

            CREATE TABLE IF NOT EXISTS document_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                source_url TEXT,
                original_filename TEXT,
                server_date TEXT,
                page_count INTEGER
            );

            CREATE TABLE IF NOT EXISTS document_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version_id INTEGER NOT NULL,
                page_number INTEGER NOT NULL,
                pdf_text TEXT,
                ocr_text TEXT,
                final_text TEXT,
                ocr_status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(document_id, version_id, page_number)
            );

            CREATE TABLE IF NOT EXISTS virtual_files (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                version_id INTEGER NOT NULL,
                archive_path TEXT NOT NULL,
                filename TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                extracted_text TEXT,
                synopsis TEXT,
                tags TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
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

        repo.update_status("doc-1", DocumentStatus::Downloaded)
            .await
            .unwrap();
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
