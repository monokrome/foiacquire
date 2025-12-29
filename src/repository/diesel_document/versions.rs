//! Document version operations.

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{DieselDocumentRepository, LastInsertRowId};
use crate::models::DocumentVersion;
use crate::repository::diesel_models::DocumentVersionRecord;
use crate::repository::pool::DieselError;
use crate::schema::document_versions;
use crate::{with_conn, with_conn_split};

impl DieselDocumentRepository {
    /// Load versions for a document.
    pub(crate) async fn load_versions(
        &self,
        document_id: &str,
    ) -> Result<Vec<DocumentVersion>, DieselError> {
        with_conn!(self.pool, conn, {
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
        })
    }

    /// Load versions for multiple documents in a single query.
    /// Returns a map of document_id -> versions.
    pub(crate) async fn load_versions_batch(
        &self,
        document_ids: &[String],
    ) -> Result<std::collections::HashMap<String, Vec<DocumentVersion>>, DieselError> {
        if document_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let records: Vec<DocumentVersionRecord> = with_conn!(self.pool, conn, {
            document_versions::table
                .filter(document_versions::document_id.eq_any(document_ids))
                .order((document_versions::document_id, document_versions::id.desc()))
                .load(&mut conn)
                .await
        })?;

        let mut result: std::collections::HashMap<String, Vec<DocumentVersion>> =
            std::collections::HashMap::new();
        for record in records {
            let doc_id = record.document_id.clone();
            let version = Self::version_record_to_model(record);
            result.entry(doc_id).or_default().push(version);
        }
        Ok(result)
    }

    /// Add a new version.
    #[allow(dead_code)]
    pub async fn add_version(
        &self,
        document_id: &str,
        version: &DocumentVersion,
    ) -> Result<i64, DieselError> {
        let file_path = version.file_path.to_string_lossy().to_string();
        let acquired_at = version.acquired_at.to_rfc3339();
        let file_size = version.file_size as i32;

        with_conn_split!(self.pool,
            sqlite: conn => {
                diesel::insert_into(document_versions::table)
                    .values((
                        document_versions::document_id.eq(document_id),
                        document_versions::content_hash.eq(&version.content_hash),
                        document_versions::content_hash_blake3
                            .eq(version.content_hash_blake3.as_deref()),
                        document_versions::file_path.eq(&file_path),
                        document_versions::file_size.eq(file_size),
                        document_versions::mime_type.eq(&version.mime_type),
                        document_versions::acquired_at.eq(&acquired_at),
                        document_versions::source_url.eq(version.source_url.as_deref()),
                        document_versions::original_filename
                            .eq(version.original_filename.as_deref()),
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
            },
            postgres: conn => {
                let result: i32 = diesel::insert_into(document_versions::table)
                    .values((
                        document_versions::document_id.eq(document_id),
                        document_versions::content_hash.eq(&version.content_hash),
                        document_versions::content_hash_blake3
                            .eq(version.content_hash_blake3.as_deref()),
                        document_versions::file_path.eq(&file_path),
                        document_versions::file_size.eq(file_size),
                        document_versions::mime_type.eq(&version.mime_type),
                        document_versions::acquired_at.eq(&acquired_at),
                        document_versions::source_url.eq(version.source_url.as_deref()),
                        document_versions::original_filename
                            .eq(version.original_filename.as_deref()),
                        document_versions::server_date
                            .eq(version.server_date.map(|d| d.to_rfc3339()).as_deref()),
                        document_versions::page_count.eq(version.page_count.map(|c| c as i32)),
                    ))
                    .returning(document_versions::id)
                    .get_result(&mut conn)
                    .await?;
                Ok(result as i64)
            }
        )
    }

    /// Get latest version.
    #[allow(dead_code)]
    pub async fn get_latest_version(
        &self,
        document_id: &str,
    ) -> Result<Option<DocumentVersion>, DieselError> {
        with_conn!(self.pool, conn, {
            document_versions::table
                .filter(document_versions::document_id.eq(document_id))
                .order(document_versions::id.desc())
                .first::<DocumentVersionRecord>(&mut conn)
                .await
                .optional()
                .map(|opt| opt.map(Self::version_record_to_model))
        })
    }

    /// Get current version ID.
    pub async fn get_current_version_id(
        &self,
        document_id: &str,
    ) -> Result<Option<i64>, DieselError> {
        with_conn!(self.pool, conn, {
            let version: Option<i32> = document_versions::table
                .filter(document_versions::document_id.eq(document_id))
                .order(document_versions::id.desc())
                .select(document_versions::id)
                .first(&mut conn)
                .await
                .optional()?;
            Ok(version.map(|v| v as i64))
        })
    }

    /// Update version mime type.
    pub async fn update_version_mime_type(
        &self,
        version_id: i64,
        mime_type: &str,
    ) -> Result<(), DieselError> {
        with_conn!(self.pool, conn, {
            diesel::update(document_versions::table.find(version_id as i32))
                .set(document_versions::mime_type.eq(mime_type))
                .execute(&mut conn)
                .await?;
            Ok(())
        })
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

    /// Find an existing file by dual hash and size for deduplication.
    ///
    /// Returns the file_path if a matching file already exists, allowing
    /// the caller to skip writing a duplicate file to disk.
    ///
    /// Uses SHA-256 + BLAKE3 + file_size for collision-resistant matching.
    pub async fn find_existing_file(
        &self,
        sha256_hash: &str,
        blake3_hash: &str,
        file_size: i64,
    ) -> Result<Option<String>, DieselError> {
        with_conn!(self.pool, conn, {
            document_versions::table
                .filter(document_versions::content_hash.eq(sha256_hash))
                .filter(document_versions::content_hash_blake3.eq(blake3_hash))
                .filter(document_versions::file_size.eq(file_size as i32))
                .select(document_versions::file_path)
                .first::<String>(&mut conn)
                .await
                .optional()
        })
    }

    /// Get all content hashes for duplicate detection.
    /// Returns (doc_id, source_id, content_hash, title) tuples
    pub async fn get_content_hashes(
        &self,
    ) -> Result<Vec<(String, String, String, String)>, DieselError> {
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

        let results: Vec<HashRow> = with_conn!(self.pool, conn, {
            diesel::sql_query(
                r#"SELECT dv.document_id, d.source_id, dv.content_hash, d.title
                   FROM document_versions dv
                   JOIN documents d ON dv.document_id = d.id
                   WHERE dv.content_hash IS NOT NULL
                   AND dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = dv.document_id)"#
            ).load(&mut conn).await
        })?;

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

        let results: Vec<SourceRow> = with_conn!(self.pool, conn, {
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await
        })?;

        Ok(results
            .into_iter()
            .map(|r| (r.source_id, r.document_id, r.title.unwrap_or_default()))
            .collect())
    }
}
