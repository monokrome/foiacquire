//! Diesel-based document repository.
//!
//! Uses diesel-async for async database support. Works with both SQLite and PostgreSQL.
//!
//! This module is split into submodules for maintainability:
//! - `mod.rs` (this file): Core CRUD, virtual files, helpers
//! - `versions.rs`: Document version operations
//! - `pages.rs`: Document page and OCR operations
//! - `queries.rs`: Complex queries, browsing, statistics
//! - `analysis.rs`: Analysis result operations

mod analysis;
pub mod entities;
mod pages;
mod queries;
mod versions;

pub use queries::BrowseParams;

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::models::{DocumentRecord, DocumentVersionRecord, VirtualFileRecord};
use super::pool::{DbPool, DieselError};
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{Document, DocumentStatus, DocumentVersion, VirtualFile, VirtualFileStatus};
use crate::schema::{document_versions, documents, virtual_files};
use crate::with_conn;

/// OCR result for a page.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OcrResult {
    pub backend: String,
    pub model: Option<String>,
    pub text: Option<String>,
    pub confidence: Option<f32>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Diesel-based document repository with compile-time query checking.
#[derive(Clone)]
pub struct DieselDocumentRepository {
    pub pool: DbPool,
}

impl DieselDocumentRepository {
    /// Create a new Diesel document repository.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    // ========================================================================
    // Core CRUD Operations
    // ========================================================================

    /// Convert document records to documents with batch-loaded versions.
    ///
    /// Single query for all versions instead of one query per document.
    async fn records_to_documents(
        &self,
        records: Vec<DocumentRecord>,
    ) -> Result<Vec<Document>, DieselError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let doc_ids: Vec<String> = records.iter().map(|r| r.id.clone()).collect();
        let mut versions_map = self.load_versions_batch(&doc_ids).await?;
        records
            .into_iter()
            .map(|record| {
                let versions = versions_map.remove(&record.id).unwrap_or_default();
                Self::record_to_document(record, versions)
            })
            .collect()
    }

    /// Get multiple documents by IDs in a single batch query.
    pub async fn get_batch(&self, ids: &[String]) -> Result<Vec<Document>, DieselError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table
                .filter(documents::id.eq_any(ids))
                .load(&mut conn)
                .await
        })?;

        self.records_to_documents(records).await
    }

    /// Get a document by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Document>, DieselError> {
        let record: Option<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table.find(id).first(&mut conn).await.optional()
        })?;

        match record {
            Some(record) => {
                let versions = self.load_versions(&record.id).await?;
                Ok(Some(Self::record_to_document(record, versions)?))
            }
            None => Ok(None),
        }
    }

    /// Get all documents for a source.
    pub async fn get_by_source(&self, source_id: &str) -> Result<Vec<Document>, DieselError> {
        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table
                .filter(documents::source_id.eq(source_id))
                .order(documents::created_at.desc())
                .load(&mut conn)
                .await
        })?;

        self.records_to_documents(records).await
    }

    /// Get documents by URL.
    pub async fn get_by_url(&self, url: &str) -> Result<Vec<Document>, DieselError> {
        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table
                .filter(documents::source_url.eq(url))
                .load(&mut conn)
                .await
        })?;

        self.records_to_documents(records).await
    }

    /// Check if a document exists.
    #[allow(dead_code)]
    pub async fn exists(&self, id: &str) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = documents::table
                .filter(documents::id.eq(id))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count > 0)
        })
    }

    /// Save a document.
    ///
    /// This also computes and sets the category_id based on the document's
    /// current version's MIME type.
    pub async fn save(&self, doc: &Document) -> Result<(), DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::Documents;
        use sea_query::{OnConflict, Query};

        let metadata = serde_json::to_string(&doc.metadata)
            .map_err(|e| diesel::result::Error::SerializationError(Box::new(e)))?;
        let created_at = doc.created_at.to_rfc3339();
        let updated_at = doc.updated_at.to_rfc3339();
        let status = doc.status.as_str().to_string();

        let category_id: Option<String> = doc.current_version().map(|v| {
            crate::utils::mime_type_category(&v.mime_type)
                .id()
                .to_string()
        });

        let stmt = Query::insert()
            .into_table(Documents::Table)
            .columns([
                Documents::Id,
                Documents::SourceId,
                Documents::SourceUrl,
                Documents::Title,
                Documents::Status,
                Documents::Metadata,
                Documents::CreatedAt,
                Documents::UpdatedAt,
                Documents::CategoryId,
            ])
            .values_panic([
                doc.id.clone().into(),
                doc.source_id.clone().into(),
                doc.source_url.clone().into(),
                doc.title.clone().into(),
                status.clone().into(),
                metadata.clone().into(),
                created_at.clone().into(),
                updated_at.clone().into(),
                category_id.clone().into(),
            ])
            .on_conflict(
                OnConflict::column(Documents::Id)
                    .update_columns([
                        Documents::SourceId,
                        Documents::SourceUrl,
                        Documents::Title,
                        Documents::Status,
                        Documents::Metadata,
                        Documents::UpdatedAt,
                        Documents::CategoryId,
                    ])
                    .to_owned(),
            )
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Text, _>(&doc.id)
                .bind::<diesel::sql_types::Text, _>(&doc.source_id)
                .bind::<diesel::sql_types::Text, _>(&doc.source_url)
                .bind::<diesel::sql_types::Text, _>(&doc.title)
                .bind::<diesel::sql_types::Text, _>(&status)
                .bind::<diesel::sql_types::Text, _>(&metadata)
                .bind::<diesel::sql_types::Text, _>(&created_at)
                .bind::<diesel::sql_types::Text, _>(&updated_at)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&category_id)
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }

    /// Save a document and persist any new versions (id == 0).
    ///
    /// Use this instead of `save()` when creating a new document or adding
    /// versions, so the version rows are actually written to document_versions.
    /// Use `save()` alone when only updating document metadata/status.
    pub async fn save_with_versions(&self, doc: &Document) -> Result<(), DieselError> {
        self.save(doc).await?;

        for version in &doc.versions {
            if version.id == 0 {
                self.add_version(&doc.id, version).await?;
            }
        }

        Ok(())
    }

    /// Delete a document.
    #[allow(dead_code)]
    pub async fn delete(&self, id: &str) -> Result<bool, DieselError> {
        use crate::schema::document_pages;
        use diesel_async::AsyncConnection;

        with_conn!(self.pool, conn, {
            conn.transaction(|conn| {
                Box::pin(async move {
                    diesel::delete(
                        document_versions::table.filter(document_versions::document_id.eq(id)),
                    )
                    .execute(conn)
                    .await?;
                    diesel::delete(
                        document_pages::table.filter(document_pages::document_id.eq(id)),
                    )
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
        })
    }

    /// Update document status.
    pub async fn update_status(&self, id: &str, status: DocumentStatus) -> Result<(), DieselError> {
        let status_str = status.as_str().to_string();
        let updated_at = Utc::now().to_rfc3339();

        with_conn!(self.pool, conn, {
            diesel::update(documents::table.find(id))
                .set((
                    documents::status.eq(&status_str),
                    documents::updated_at.eq(&updated_at),
                ))
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }

    /// Get all documents.
    pub async fn get_all(&self) -> Result<Vec<Document>, DieselError> {
        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table
                .order(documents::created_at.desc())
                .load(&mut conn)
                .await
        })?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions)?);
        }
        Ok(docs)
    }

    /// Get all document URLs as a HashSet.
    ///
    /// Only includes documents that have at least one version row, since
    /// documents without versions are incomplete imports that should be retried.
    pub async fn get_all_urls_set(&self) -> Result<std::collections::HashSet<String>, DieselError> {
        with_conn!(self.pool, conn, {
            let urls: Vec<String> = documents::table
                .filter(diesel::dsl::exists(
                    document_versions::table
                        .filter(document_versions::document_id.eq(documents::id)),
                ))
                .select(documents::source_url)
                .load(&mut conn)
                .await?;
            Ok(urls.into_iter().collect())
        })
    }

    /// Get URLs by source.
    pub async fn get_urls_by_source(&self, source_id: &str) -> Result<Vec<String>, DieselError> {
        with_conn!(self.pool, conn, {
            documents::table
                .filter(documents::source_id.eq(source_id))
                .select(documents::source_url)
                .load(&mut conn)
                .await
        })
    }

    // ========================================================================
    // Virtual File Operations
    // ========================================================================

    /// Insert virtual file.
    pub async fn insert_virtual_file(&self, vf: &VirtualFile) -> Result<(), DieselError> {
        let now = Utc::now().to_rfc3339();

        with_conn!(self.pool, conn, {
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
                    virtual_files::tags.eq(serde_json::to_string(&vf.tags)
                        .map_err(|e| diesel::result::Error::SerializationError(Box::new(e)))?
                        .as_str()),
                    virtual_files::status.eq(vf.status.as_str()),
                    virtual_files::created_at.eq(&now),
                    virtual_files::updated_at.eq(&now),
                ))
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }

    /// Get virtual files.
    pub async fn get_virtual_files(
        &self,
        document_id: &str,
        version: i32,
    ) -> Result<Vec<VirtualFile>, DieselError> {
        with_conn!(self.pool, conn, {
            virtual_files::table
                .filter(virtual_files::document_id.eq(document_id))
                .filter(virtual_files::version_id.eq(version))
                .load::<VirtualFileRecord>(&mut conn)
                .await
                .and_then(|records| {
                    records
                        .into_iter()
                        .map(Self::virtual_file_record_to_model)
                        .collect()
                })
        })
    }

    /// Count unprocessed archives.
    pub async fn count_unprocessed_archives(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        with_conn!(self.pool, conn, {
            let result: Vec<CountRow> = if let Some(sid) = source_id {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
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
                           AND d.source_id = $1"#,
                    )
                    .bind::<diesel::sql_types::Text, _>(sid),
                    &mut conn,
                )
                .await?
            } else {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
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
                                OR dv.mime_type = 'application/x-7z-compressed')"#,
                    ),
                    &mut conn,
                )
                .await?
            };
            #[allow(clippy::get_first)]
            Ok(result.get(0).map(|r| r.count as u64).unwrap_or(0))
        })
    }

    /// Count unprocessed emails.
    pub async fn count_unprocessed_emails(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        with_conn!(self.pool, conn, {
            let result: Vec<CountRow> = if let Some(sid) = source_id {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
                        r#"SELECT COUNT(DISTINCT d.id) as count
                           FROM documents d
                           JOIN document_versions dv ON d.id = dv.document_id
                           WHERE d.status IN ('pending', 'downloaded')
                           AND (dv.mime_type LIKE 'message/%' OR dv.mime_type LIKE '%rfc822%')
                           AND d.source_id = $1"#,
                    )
                    .bind::<diesel::sql_types::Text, _>(sid),
                    &mut conn,
                )
                .await?
            } else {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
                        r#"SELECT COUNT(DISTINCT d.id) as count
                           FROM documents d
                           JOIN document_versions dv ON d.id = dv.document_id
                           WHERE d.status IN ('pending', 'downloaded')
                           AND (dv.mime_type LIKE 'message/%' OR dv.mime_type LIKE '%rfc822%')"#,
                    ),
                    &mut conn,
                )
                .await?
            };
            #[allow(clippy::get_first)]
            Ok(result.get(0).map(|r| r.count as u64).unwrap_or(0))
        })
    }

    /// Get unprocessed archives.
    pub async fn get_unprocessed_archives(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let ids: Vec<DocIdRow> = with_conn!(self.pool, conn, {
            if let Some(sid) = source_id {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(format!(
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
                           AND d.source_id = $1
                           ORDER BY d.updated_at ASC
                           LIMIT {}"#,
                        limit
                    ))
                    .bind::<diesel::sql_types::Text, _>(sid),
                    &mut conn,
                )
                .await
            } else {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(format!(
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
                           ORDER BY d.updated_at ASC
                           LIMIT {}"#,
                        limit
                    )),
                    &mut conn,
                )
                .await
            }
        })?;

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
        let ids: Vec<DocIdRow> = with_conn!(self.pool, conn, {
            if let Some(sid) = source_id {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(format!(
                        r#"SELECT DISTINCT d.id
                           FROM documents d
                           JOIN document_versions dv ON d.id = dv.document_id
                           WHERE d.status IN ('pending', 'downloaded')
                           AND (dv.mime_type LIKE 'message/%' OR dv.mime_type LIKE '%rfc822%')
                           AND d.source_id = $1
                           ORDER BY d.updated_at ASC
                           LIMIT {}"#,
                        limit
                    ))
                    .bind::<diesel::sql_types::Text, _>(sid),
                    &mut conn,
                )
                .await
            } else {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(format!(
                        r#"SELECT DISTINCT d.id
                           FROM documents d
                           JOIN document_versions dv ON d.id = dv.document_id
                           WHERE d.status IN ('pending', 'downloaded')
                           AND (dv.mime_type LIKE 'message/%' OR dv.mime_type LIKE '%rfc822%')
                           ORDER BY d.updated_at ASC
                           LIMIT {}"#,
                        limit
                    )),
                    &mut conn,
                )
                .await
            }
        })?;

        let mut docs = Vec::with_capacity(ids.len());
        for row in ids {
            if let Ok(Some(doc)) = self.get(&row.id).await {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    pub(crate) fn record_to_document(
        record: DocumentRecord,
        versions: Vec<DocumentVersion>,
    ) -> Result<Document, DieselError> {
        let tags = match record.tags {
            Some(ref s) => serde_json::from_str(s).map_err(|e| {
                diesel::result::Error::DeserializationError(
                    format!("Invalid tags JSON for document '{}': {}", record.id, e).into(),
                )
            })?,
            None => Vec::new(),
        };
        let status = DocumentStatus::from_str(&record.status).ok_or_else(|| {
            diesel::result::Error::DeserializationError(
                format!(
                    "Invalid document status '{}' for document '{}'",
                    record.status, record.id
                )
                .into(),
            )
        })?;
        let metadata = serde_json::from_str(&record.metadata).map_err(|e| {
            diesel::result::Error::DeserializationError(
                format!("Invalid metadata JSON for document '{}': {}", record.id, e).into(),
            )
        })?;

        Ok(Document {
            id: record.id,
            source_id: record.source_id,
            title: record.title,
            source_url: record.source_url,
            extracted_text: record.extracted_text,
            synopsis: record.synopsis,
            tags,
            status,
            metadata,
            created_at: parse_datetime(&record.created_at),
            updated_at: parse_datetime(&record.updated_at),
            discovery_method: record.discovery_method,
            versions,
        })
    }

    pub(crate) fn version_record_to_model(record: DocumentVersionRecord) -> DocumentVersion {
        DocumentVersion {
            id: record.id as i64,
            content_hash: record.content_hash,
            content_hash_blake3: record.content_hash_blake3,
            file_path: record.file_path.map(PathBuf::from),
            file_size: record.file_size as u64,
            mime_type: record.mime_type,
            acquired_at: parse_datetime(&record.acquired_at),
            source_url: record.source_url,
            original_filename: record.original_filename,
            server_date: parse_datetime_opt(record.server_date),
            page_count: record.page_count.map(|c| c as u32),
            archive_snapshot_id: record.archive_snapshot_id,
            earliest_archived_at: parse_datetime_opt(record.earliest_archived_at),
            dedup_index: record.dedup_index.map(|i| i as u32),
        }
    }

    fn virtual_file_record_to_model(record: VirtualFileRecord) -> Result<VirtualFile, DieselError> {
        let tags = match record.tags {
            Some(ref s) => serde_json::from_str(s).map_err(|e| {
                diesel::result::Error::DeserializationError(
                    format!("Invalid tags JSON for virtual file '{}': {}", record.id, e).into(),
                )
            })?,
            None => Vec::new(),
        };
        let status = VirtualFileStatus::from_str(&record.status).ok_or_else(|| {
            diesel::result::Error::DeserializationError(
                format!(
                    "Invalid virtual file status '{}' for file '{}'",
                    record.status, record.id
                )
                .into(),
            )
        })?;

        Ok(VirtualFile {
            id: record.id,
            document_id: record.document_id,
            version_id: record.version_id as i64,
            archive_path: record.archive_path,
            filename: record.filename,
            file_size: record.file_size as u64,
            mime_type: record.mime_type,
            extracted_text: record.extracted_text,
            synopsis: record.synopsis,
            tags,
            status,
            created_at: parse_datetime(&record.created_at),
            updated_at: parse_datetime(&record.updated_at),
        })
    }
}

// Helper structs for SQL queries
#[derive(diesel::QueryableByName)]
pub(crate) struct MimeCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub mime_type: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub count: i64,
}

#[derive(diesel::QueryableByName)]
pub(crate) struct TagRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub tag: String,
}

#[derive(diesel::QueryableByName)]
pub struct DocIdRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub id: String,
}

#[derive(diesel::QueryableByName)]
pub(crate) struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub count: i64,
}

/// Lightweight browse result that excludes large text fields.
/// Used for document listing pages to avoid loading extracted_text.
#[derive(diesel::QueryableByName, Debug, Clone)]
pub struct BrowseRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub title: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub source_id: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub synopsis: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub tags: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub original_filename: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub mime_type: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub file_size: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub acquired_at: String,
}

#[derive(diesel::QueryableByName)]
pub(crate) struct ReturningId {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub id: i32,
}

#[cfg(test)]
mod tests {
    use super::super::pool::SqlitePool;
    use super::*;
    use diesel_async::SimpleAsyncConnection;
    use tempfile::tempdir;

    pub(crate) async fn setup_test_db() -> (DbPool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let sqlite_pool = SqlitePool::from_path(&db_path);
        let mut conn = sqlite_pool.get().await.unwrap();

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
                content_hash_blake3 TEXT,
                file_path TEXT,
                file_size INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                source_url TEXT,
                original_filename TEXT,
                server_date TEXT,
                page_count INTEGER,
                archive_snapshot_id INTEGER,
                earliest_archived_at TEXT,
                dedup_index INTEGER
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

        (DbPool::Sqlite(sqlite_pool), dir)
    }

    #[tokio::test]
    async fn test_document_crud() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);

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
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);

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
            content_hash_blake3: Some("def456".to_string()),
            file_path: None,
            file_size: 1024,
            mime_type: "application/pdf".to_string(),
            acquired_at: Utc::now(),
            source_url: None,
            original_filename: None,
            server_date: None,
            page_count: None,
            archive_snapshot_id: None,
            earliest_archived_at: None,
            dedup_index: None,
        };
        repo.add_version("doc-2", &version).await.unwrap();

        let latest = repo.get_latest_version("doc-2").await.unwrap().unwrap();
        assert_eq!(latest.content_hash, "abc123");
        assert_eq!(latest.file_size, 1024);
    }

    #[tokio::test]
    async fn test_count_unprocessed_archives_with_sql_metacharacters() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);

        let result = repo
            .count_unprocessed_archives(Some("'; DROP TABLE documents; --"))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_count_unprocessed_emails_with_sql_metacharacters() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);

        let result = repo
            .count_unprocessed_emails(Some("'; DROP TABLE documents; --"))
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }
}
