//! Complex queries, browsing, and statistics operations.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{
    CountRow, DieselDocumentRepository, DieselDocumentSummary, DocIdRow, MimeCount, SourceCount,
    StatusCount, TagRow,
};
use crate::models::{Document, DocumentStatus};
use crate::repository::diesel_models::DocumentRecord;
use crate::repository::pool::DieselError;
use crate::repository::{document::DocumentNavigation, parse_datetime};
use crate::schema::{document_versions, documents};
use crate::{with_conn, with_conn_split};

impl DieselDocumentRepository {
    // ========================================================================
    // Counting Operations
    // ========================================================================

    /// Count all documents.
    pub async fn count(&self) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = documents::table
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count as u64)
        })
    }

    /// Get document counts per source.
    pub async fn get_all_source_counts(&self) -> Result<HashMap<String, u64>, DieselError> {
        with_conn!(self.pool, conn, {
            let rows: Vec<SourceCount> = diesel::sql_query(
                "SELECT source_id, COUNT(*) as count FROM documents GROUP BY source_id",
            )
            .load(&mut conn)
            .await?;

            let mut counts = HashMap::new();
            for SourceCount { source_id, count } in rows {
                counts.insert(source_id, count as u64);
            }
            Ok(counts)
        })
    }

    /// Count documents needing OCR.
    /// Documents need OCR if status is 'pending' or 'downloaded' and they have a PDF version.
    pub async fn count_needing_ocr(&self, source_id: Option<&str>) -> Result<u64, DieselError> {
        with_conn!(self.pool, conn, {
            let mut query = documents::table
                .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
                .into_boxed();
            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }
            let count: i64 = query.count().get_result(&mut conn).await?;
            Ok(count as u64)
        })
    }

    /// Count documents needing summarization.
    /// Documents need summarization if status is 'ocr_complete' (OCR done but not indexed).
    pub async fn count_needing_summarization(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        with_conn!(self.pool, conn, {
            let mut query = documents::table
                .filter(documents::status.eq("ocr_complete"))
                .into_boxed();
            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }
            let count: i64 = query.count().get_result(&mut conn).await?;
            Ok(count as u64)
        })
    }

    /// Count documents by source.
    pub async fn count_by_source(&self, source_id: &str) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = documents::table
                .filter(documents::source_id.eq(source_id))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count as u64)
        })
    }

    /// Count documents by status.
    pub async fn count_by_status(
        &self,
        source_id: Option<&str>,
    ) -> Result<HashMap<String, u64>, DieselError> {
        let query = if let Some(sid) = source_id {
            format!(
                "SELECT status, COUNT(*) as count FROM documents WHERE source_id = '{}' GROUP BY status",
                sid.replace('\'', "''")
            )
        } else {
            "SELECT status, COUNT(*) as count FROM documents GROUP BY status".to_string()
        };

        with_conn!(self.pool, conn, {
            let rows: Vec<StatusCount> =
                diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;
            let mut counts = HashMap::new();
            for StatusCount { status, count } in rows {
                counts.insert(status, count as u64);
            }
            Ok(counts)
        })
    }

    /// Count all by status.
    pub async fn count_all_by_status(&self) -> Result<HashMap<String, u64>, DieselError> {
        self.count_by_status(None).await
    }

    /// Get status counts for each source.
    /// Returns a map of source_id -> (status -> count).
    pub async fn get_source_status_counts(
        &self,
    ) -> Result<HashMap<String, HashMap<String, u64>>, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct SourceStatusCount {
            #[diesel(sql_type = diesel::sql_types::Text)]
            source_id: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            status: String,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }

        with_conn!(self.pool, conn, {
            let rows: Vec<SourceStatusCount> = diesel::sql_query(
                "SELECT source_id, status, COUNT(*) as count FROM documents GROUP BY source_id, status",
            )
            .load(&mut conn)
            .await?;

            let mut result: HashMap<String, HashMap<String, u64>> = HashMap::new();
            for row in rows {
                result
                    .entry(row.source_id)
                    .or_default()
                    .insert(row.status, row.count as u64);
            }
            Ok(result)
        })
    }

    /// Count documents needing date estimation.
    /// These are documents without an estimated_date in metadata.
    pub async fn count_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        let source_filter = source_id
            .map(|s| format!("AND source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        with_conn_split!(self.pool,
            sqlite: conn => {
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
                #[allow(clippy::get_first)]
                Ok(result.get(0).map(|r| r.count as u64).unwrap_or(0))
            },
            postgres: conn => {
                let query = format!(
                    r#"SELECT COUNT(*) as count FROM documents
                       WHERE metadata->>'estimated_date' IS NULL
                       {}"#,
                    source_filter
                );
                let result: Vec<CountRow> =
                    diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                        .await
                        .unwrap_or_default();
                #[allow(clippy::get_first)]
                Ok(result.get(0).map(|r| r.count as u64).unwrap_or(0))
            }
        )
    }

    // ========================================================================
    // Statistics Operations
    // ========================================================================

    /// Get type statistics - count documents by MIME type.
    pub async fn get_type_stats(&self) -> Result<HashMap<String, u64>, DieselError> {
        with_conn!(self.pool, conn, {
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
            let mut stats = HashMap::new();
            for row in results {
                stats.insert(row.mime_type, row.count as u64);
            }
            Ok(stats)
        })
    }

    /// Get category statistics - count documents by category_id.
    pub async fn get_category_stats(&self) -> Result<HashMap<String, u64>, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct CategoryCount {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            category_id: Option<String>,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }

        with_conn!(self.pool, conn, {
            let results: Vec<CategoryCount> = diesel_async::RunQueryDsl::load(
                diesel::sql_query(
                    "SELECT category_id, COUNT(*) as count FROM documents GROUP BY category_id",
                ),
                &mut conn,
            )
            .await?;

            let mut stats = HashMap::new();
            for row in results {
                let category = row.category_id.unwrap_or_else(|| "unknown".to_string());
                stats.insert(category, row.count as u64);
            }
            Ok(stats)
        })
    }

    // ========================================================================
    // Browse and Search Operations
    // ========================================================================

    /// Get recent documents.
    pub async fn get_recent(&self, limit: u32) -> Result<Vec<Document>, DieselError> {
        let limit = limit as i64;
        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table
                .order(documents::updated_at.desc())
                .limit(limit)
                .load(&mut conn)
                .await
        })?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
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
        let limit = limit as i64;
        let offset = offset as i64;

        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
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
            query.load(&mut conn).await
        })?;

        // Batch load all versions in a single query
        let doc_ids: Vec<String> = records.iter().map(|r| r.id.clone()).collect();
        let mut versions_map = self.load_versions_batch(&doc_ids).await?;

        let docs = records
            .into_iter()
            .map(|record| {
                let versions = versions_map.remove(&record.id).unwrap_or_default();
                Self::record_to_document(record, versions)
            })
            .collect();
        Ok(docs)
    }

    /// Browse count.
    pub async fn browse_count(
        &self,
        source_id: Option<&str>,
        status: Option<&str>,
        _category: Option<&str>,
    ) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let mut query = documents::table.select(count_star()).into_boxed();
            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }
            if let Some(st) = status {
                query = query.filter(documents::status.eq(st));
            }
            let count: i64 = query.first(&mut conn).await?;
            Ok(count as u64)
        })
    }

    /// Get document navigation.
    pub async fn get_document_navigation(
        &self,
        document_id: &str,
        source_id: &str,
    ) -> Result<DocumentNavigation, DieselError> {
        use diesel::dsl::count_star;

        with_conn!(self.pool, conn, {
            let prev: Option<(String, String)> = documents::table
                .select((documents::id, documents::title))
                .filter(documents::source_id.eq(source_id))
                .filter(documents::id.lt(document_id))
                .order(documents::id.desc())
                .first(&mut conn)
                .await
                .optional()?;
            let next: Option<(String, String)> = documents::table
                .select((documents::id, documents::title))
                .filter(documents::source_id.eq(source_id))
                .filter(documents::id.gt(document_id))
                .order(documents::id.asc())
                .first(&mut conn)
                .await
                .optional()?;
            let position: i64 = documents::table
                .filter(documents::source_id.eq(source_id))
                .filter(documents::id.le(document_id))
                .select(count_star())
                .first(&mut conn)
                .await?;
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
        })
    }

    /// Search tags by prefix in document metadata.
    /// Tags are stored as JSON arrays in the metadata field.
    pub async fn search_tags(&self, query: &str) -> Result<Vec<String>, DieselError> {
        let pattern = format!("%{}%", query.to_lowercase());
        with_conn_split!(self.pool,
            sqlite: conn => {
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
            },
            postgres: conn => {
                // PostgreSQL uses jsonb_array_elements_text for JSON array iteration
                let results: Vec<TagRow> = diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
                        r#"SELECT DISTINCT tag
                           FROM documents, jsonb_array_elements_text(metadata->'tags') as tag
                           WHERE LOWER(tag) LIKE $1
                           ORDER BY tag
                           LIMIT 100"#,
                    )
                    .bind::<diesel::sql_types::Text, _>(&pattern),
                    &mut conn,
                )
                .await
                .unwrap_or_default();
                Ok(results.into_iter().map(|r| r.tag).collect())
            }
        )
    }

    /// Get all unique tags from document metadata.
    pub async fn get_all_tags(&self) -> Result<Vec<String>, DieselError> {
        with_conn_split!(self.pool,
            sqlite: conn => {
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
            },
            postgres: conn => {
                let results: Vec<TagRow> = diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
                        r#"SELECT DISTINCT tag
                           FROM documents, jsonb_array_elements_text(metadata->'tags') as tag
                           ORDER BY tag"#,
                    ),
                    &mut conn,
                )
                .await
                .unwrap_or_default();
                Ok(results.into_iter().map(|r| r.tag).collect())
            }
        )
    }

    /// Get documents by tag.
    /// Tags are stored in metadata JSON.
    pub async fn get_by_tag(
        &self,
        tag: &str,
        source_id: Option<&str>,
    ) -> Result<Vec<Document>, DieselError> {
        let ids: Vec<DocIdRow> = with_conn_split!(self.pool,
            sqlite: conn => {
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
                diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                    .await
                    .unwrap_or_default()
            },
            postgres: conn => {
                let query = if let Some(sid) = source_id {
                    format!(
                        r#"SELECT id FROM documents
                           WHERE source_id = '{}'
                           AND metadata->'tags' ? '{}'
                           ORDER BY updated_at DESC"#,
                        sid.replace('\'', "''"),
                        tag.replace('\'', "''")
                    )
                } else {
                    format!(
                        r#"SELECT id FROM documents
                           WHERE metadata->'tags' ? '{}'
                           ORDER BY updated_at DESC"#,
                        tag.replace('\'', "''")
                    )
                };
                diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                    .await
                    .unwrap_or_default()
            }
        );

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
        let mime_patterns = crate::utils::category_to_mime_patterns(category);
        if mime_patterns.is_empty() {
            return Ok(vec![]);
        }

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

        let ids: Vec<DocIdRow> = with_conn!(self.pool, conn, {
            diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                .await
                .unwrap_or_default()
        });

        let mut docs = Vec::with_capacity(ids.len());
        for row in ids {
            if let Ok(Some(doc)) = self.get(&row.id).await {
                docs.push(doc);
            }
        }
        Ok(docs)
    }

    // ========================================================================
    // Summary Operations
    // ========================================================================

    /// Get document summaries.
    pub async fn get_summaries(
        &self,
        source_id: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        let limit = limit as i64;
        let offset = offset as i64;

        with_conn!(self.pool, conn, {
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
                    status: DocumentStatus::from_str(&record.status)
                        .unwrap_or(DocumentStatus::Pending),
                    created_at: parse_datetime(&record.created_at),
                    updated_at: parse_datetime(&record.updated_at),
                    version_count: version_count as u32,
                    latest_file_size: latest_size.map(|s| s as u64),
                });
            }
            Ok(summaries)
        })
    }

    /// Get all document summaries.
    pub async fn get_all_summaries(&self) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        with_conn!(self.pool, conn, {
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
                    status: DocumentStatus::from_str(&record.status)
                        .unwrap_or(DocumentStatus::Pending),
                    created_at: parse_datetime(&record.created_at),
                    updated_at: parse_datetime(&record.updated_at),
                    version_count: version_count as u32,
                    latest_file_size: latest_size.map(|s| s as u64),
                });
            }
            Ok(summaries)
        })
    }

    /// Get summaries for a specific source.
    pub async fn get_summaries_by_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<DieselDocumentSummary>, DieselError> {
        self.get_summaries(source_id, 1000, 0).await
    }

    // ========================================================================
    // Document State Operations
    // ========================================================================

    /// Get documents needing date estimation.
    pub async fn get_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let source_filter = source_id
            .map(|s| format!("AND source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let ids: Vec<DocIdRow> = with_conn_split!(self.pool,
            sqlite: conn => {
                let query = format!(
                    r#"SELECT id FROM documents
                       WHERE json_extract(metadata, '$.estimated_date') IS NULL
                       {}
                       LIMIT {}"#,
                    source_filter, limit
                );
                diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                    .await
                    .unwrap_or_default()
            },
            postgres: conn => {
                let query = format!(
                    r#"SELECT id FROM documents
                       WHERE metadata->>'estimated_date' IS NULL
                       {}
                       LIMIT {}"#,
                    source_filter, limit
                );
                diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                    .await
                    .unwrap_or_default()
            }
        );

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
        let record: Option<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table.find(id).first(&mut conn).await.optional()
        })?;

        if let Some(record) = record {
            let mut metadata: serde_json::Value =
                serde_json::from_str(&record.metadata).unwrap_or(serde_json::json!({}));

            metadata["estimated_date"] = serde_json::json!({
                "date": date.to_rfc3339(),
                "confidence": confidence,
                "source": source,
            });

            let now = Utc::now().to_rfc3339();
            with_conn!(self.pool, conn, {
                diesel::update(documents::table.find(id))
                    .set((
                        documents::metadata.eq(metadata.to_string()),
                        documents::updated_at.eq(&now),
                    ))
                    .execute(&mut conn)
                    .await?;
                Ok::<(), DieselError>(())
            })?;
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
        let record: Option<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table.find(id).first(&mut conn).await.optional()
        })?;

        if let Some(record) = record {
            let mut metadata: serde_json::Value =
                serde_json::from_str(&record.metadata).unwrap_or(serde_json::json!({}));

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
            with_conn!(self.pool, conn, {
                diesel::update(documents::table.find(id))
                    .set((
                        documents::metadata.eq(metadata.to_string()),
                        documents::updated_at.eq(&now),
                    ))
                    .execute(&mut conn)
                    .await?;
                Ok::<(), DieselError>(())
            })?;
        }

        Ok(())
    }

    /// Get documents needing summarization.
    pub async fn get_needing_summarization(
        &self,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table
                .filter(documents::status.eq("ocr_complete"))
                .order(documents::updated_at.asc())
                .limit(limit as i64)
                .load(&mut conn)
                .await
        })?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Get documents needing OCR.
    pub async fn get_needing_ocr(&self, limit: usize) -> Result<Vec<Document>, DieselError> {
        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            documents::table
                .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
                .order(documents::updated_at.asc())
                .limit(limit as i64)
                .load(&mut conn)
                .await
        })?;

        let mut docs = Vec::with_capacity(records.len());
        for record in records {
            let versions = self.load_versions(&record.id).await?;
            docs.push(Self::record_to_document(record, versions));
        }
        Ok(docs)
    }

    /// Finalize document - mark as indexed.
    pub async fn finalize_document(&self, id: &str) -> Result<(), DieselError> {
        self.update_status(id, DocumentStatus::Indexed).await
    }

    /// Finalize pending documents - mark documents with all pages complete as indexed.
    pub async fn finalize_pending_documents(&self) -> Result<u64, DieselError> {
        let doc_ids: Vec<String> = with_conn!(self.pool, conn, {
            documents::table
                .filter(documents::status.eq("ocr_complete"))
                .select(documents::id)
                .load(&mut conn)
                .await
        })?;

        let mut count = 0u64;
        for doc_id in doc_ids {
            self.update_status(&doc_id, DocumentStatus::Indexed).await?;
            count += 1;
        }

        Ok(count)
    }
}
