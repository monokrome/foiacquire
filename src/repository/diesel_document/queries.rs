//! Complex queries, browsing, and statistics operations.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{CountRow, DieselDocumentRepository, DocIdRow, MimeCount, StatusCount, TagRow};
use crate::models::{Document, DocumentStatus};
use crate::repository::diesel_models::DocumentRecord;
use crate::repository::document::DocumentNavigation;
use crate::repository::pool::DieselError;
use crate::schema::documents;
use crate::{with_conn, with_conn_split};

/// Parameters for browsing/filtering documents.
#[derive(Debug, Default, Clone)]
pub struct BrowseParams<'a> {
    pub source_id: Option<&'a str>,
    pub status: Option<&'a str>,
    pub categories: &'a [String],
    pub tags: &'a [String],
    pub search_query: Option<&'a str>,
    pub sort_field: Option<&'a str>,
    pub sort_order: Option<&'a str>,
    pub limit: u32,
    pub offset: u32,
}

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
                .get_result(&mut conn)
                .await?;
            Ok(count as u64)
        })
    }

    /// Get document counts per source.
    pub async fn get_all_source_counts(&self) -> Result<HashMap<String, u64>, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let rows: Vec<(String, i64)> = documents::table
                .group_by(documents::source_id)
                .select((documents::source_id, count_star()))
                .load(&mut conn)
                .await?;

            Ok(rows
                .into_iter()
                .map(|(id, count)| (id, count as u64))
                .collect())
        })
    }

    /// Count documents needing OCR.
    /// Documents need OCR if status is 'pending' or 'downloaded' and they have a PDF version.
    pub async fn count_needing_ocr(&self, source_id: Option<&str>) -> Result<u64, DieselError> {
        self.count_needing_ocr_filtered(source_id, None).await
    }

    /// Count documents needing OCR with optional mime type filter.
    pub async fn count_needing_ocr_filtered(
        &self,
        source_id: Option<&str>,
        mime_type: Option<&str>,
    ) -> Result<u64, DieselError> {
        use crate::schema::document_versions;

        with_conn!(self.pool, conn, {
            if let Some(mime) = mime_type {
                // Join with versions to filter by mime type
                let mut query = documents::table
                    .inner_join(
                        document_versions::table
                            .on(document_versions::document_id.eq(documents::id)),
                    )
                    .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
                    .filter(document_versions::mime_type.eq(mime))
                    .select(documents::id)
                    .distinct()
                    .into_boxed();

                if let Some(sid) = source_id {
                    query = query.filter(documents::source_id.eq(sid));
                }
                let count: i64 = query.count().get_result(&mut conn).await?;
                Ok(count as u64)
            } else {
                // No mime filter, use simple query
                let mut query = documents::table
                    .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
                    .into_boxed();
                if let Some(sid) = source_id {
                    query = query.filter(documents::source_id.eq(sid));
                }
                let count: i64 = query.count().get_result(&mut conn).await?;
                Ok(count as u64)
            }
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
                .get_result(&mut conn)
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
    // Generic Annotation Queries
    // ========================================================================

    /// Count documents needing a specific annotation type.
    ///
    /// A document needs annotation when `metadata.annotations[type]` is missing
    /// or has a version less than the requested version.
    /// For "llm_summary", also requires status = 'ocr_complete'.
    pub async fn count_documents_needing_annotation(
        &self,
        annotation_type: &str,
        version: i32,
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        // For llm_summary, delegate to existing specialized query
        if annotation_type == "llm_summary" {
            return self.count_needing_summarization(source_id).await;
        }

        // For date_detection, delegate to existing specialized query
        if annotation_type == "date_detection" {
            return self
                .count_documents_needing_date_estimation(source_id)
                .await;
        }

        // Generic: check metadata.annotations[type].version < requested version
        let source_filter = source_id
            .map(|s| format!("AND source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        with_conn_split!(self.pool,
            sqlite: conn => {
                let query = format!(
                    r#"SELECT COUNT(*) as count FROM documents
                       WHERE (
                           json_extract(metadata, '$.annotations.{annotation_type}.version') IS NULL
                           OR json_extract(metadata, '$.annotations.{annotation_type}.version') < {version}
                       )
                       {source_filter}"#,
                    annotation_type = annotation_type.replace('\'', "''"),
                    version = version,
                    source_filter = source_filter,
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
                       WHERE (
                           (metadata->'annotations'->'{annotation_type}'->>'version')::int IS NULL
                           OR (metadata->'annotations'->'{annotation_type}'->>'version')::int < {version}
                       )
                       {source_filter}"#,
                    annotation_type = annotation_type.replace('\'', "''"),
                    version = version,
                    source_filter = source_filter,
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

    /// Get documents needing a specific annotation type.
    pub async fn get_documents_needing_annotation(
        &self,
        annotation_type: &str,
        version: i32,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>, DieselError> {
        // For llm_summary, delegate to existing specialized query
        if annotation_type == "llm_summary" {
            return self.get_needing_summarization(limit).await;
        }

        // For date_detection, delegate to existing specialized query
        if annotation_type == "date_detection" {
            return self
                .get_documents_needing_date_estimation(source_id, limit)
                .await;
        }

        // Generic: check metadata.annotations[type].version < requested version
        let source_filter = source_id
            .map(|s| format!("AND source_id = '{}'", s.replace('\'', "''")))
            .unwrap_or_default();

        let ids: Vec<DocIdRow> = with_conn_split!(self.pool,
            sqlite: conn => {
                let query = format!(
                    r#"SELECT id FROM documents
                       WHERE (
                           json_extract(metadata, '$.annotations.{annotation_type}.version') IS NULL
                           OR json_extract(metadata, '$.annotations.{annotation_type}.version') < {version}
                       )
                       {source_filter}
                       LIMIT {limit}"#,
                    annotation_type = annotation_type.replace('\'', "''"),
                    version = version,
                    source_filter = source_filter,
                    limit = limit,
                );
                diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn)
                    .await
                    .unwrap_or_default()
            },
            postgres: conn => {
                let query = format!(
                    r#"SELECT id FROM documents
                       WHERE (
                           (metadata->'annotations'->'{annotation_type}'->>'version')::int IS NULL
                           OR (metadata->'annotations'->'{annotation_type}'->>'version')::int < {version}
                       )
                       {source_filter}
                       LIMIT {limit}"#,
                    annotation_type = annotation_type.replace('\'', "''"),
                    version = version,
                    source_filter = source_filter,
                    limit = limit,
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
    /// Get category stats. Uses the trigger-maintained file_categories.doc_count
    /// when no source filter is applied; falls back to GROUP BY for per-source stats.
    pub async fn get_category_stats(
        &self,
        source_id: Option<&str>,
    ) -> Result<HashMap<String, u64>, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct CategoryCount {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            category_id: Option<String>,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }

        with_conn!(self.pool, conn, {
            let results: Vec<CategoryCount> = if let Some(sid) = source_id {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
                        "SELECT category_id, COUNT(*) as count FROM documents WHERE source_id = $1 GROUP BY category_id",
                    )
                    .bind::<diesel::sql_types::Text, _>(sid),
                    &mut conn,
                )
                .await?
            } else {
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query(
                        "SELECT id as category_id, doc_count as count FROM file_categories WHERE doc_count > 0",
                    ),
                    &mut conn,
                )
                .await?
            };

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
    pub async fn browse(&self, params: BrowseParams<'_>) -> Result<Vec<Document>, DieselError> {
        let limit = params.limit as i64;
        let offset = params.offset as i64;
        let source_id = params.source_id;
        let status = params.status;
        let categories = params.categories;
        let tags = params.tags;
        let search_query = params.search_query;
        let sort_field = params.sort_field;
        let sort_order = params.sort_order;

        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            // Build query with filters first, then order and paginate
            let mut query = documents::table.into_boxed();

            // Apply filters
            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }
            if let Some(st) = status {
                query = query.filter(documents::status.eq(st));
            }
            if !categories.is_empty() {
                query = query.filter(documents::category_id.eq_any(categories));
            }
            // Tags are stored as comma-separated, filter docs that contain any of the requested tags
            for tag in tags {
                let pattern = format!("%{}%", tag);
                query = query.filter(documents::tags.like(pattern));
            }
            // Text search on title and synopsis
            if let Some(q) = search_query {
                if !q.is_empty() {
                    let pattern = format!("%{}%", q);
                    query = query.filter(
                        documents::title
                            .like(pattern.clone())
                            .or(documents::synopsis.like(pattern)),
                    );
                }
            }

            // Apply sorting
            let is_desc = sort_order
                .map(|o| o.eq_ignore_ascii_case("desc"))
                .unwrap_or(true);
            match sort_field {
                Some("created_at") => {
                    if is_desc {
                        query = query.order(documents::created_at.desc());
                    } else {
                        query = query.order(documents::created_at.asc());
                    }
                }
                Some("title") => {
                    if is_desc {
                        query = query.order(documents::title.desc());
                    } else {
                        query = query.order(documents::title.asc());
                    }
                }
                _ => {
                    // Default: updated_at desc
                    if is_desc {
                        query = query.order(documents::updated_at.desc());
                    } else {
                        query = query.order(documents::updated_at.asc());
                    }
                }
            }

            query.limit(limit).offset(offset).load(&mut conn).await
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
        categories: &[String],
        tags: &[String],
        search_query: Option<&str>,
    ) -> Result<u64, DieselError> {
        let has_filters = status.is_some()
            || !categories.is_empty()
            || !tags.is_empty()
            || search_query.is_some_and(|q| !q.is_empty());

        // Use pre-computed counts when no filters are active
        if !has_filters {
            return if let Some(sid) = source_id {
                self.count_by_source(sid).await
            } else {
                self.count().await
            };
        }

        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let mut query = documents::table.select(count_star()).into_boxed();
            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }
            if let Some(st) = status {
                query = query.filter(documents::status.eq(st));
            }
            if !categories.is_empty() {
                query = query.filter(documents::category_id.eq_any(categories));
            }
            for tag in tags {
                let pattern = format!("%{}%", tag);
                query = query.filter(documents::tags.like(pattern));
            }
            if let Some(q) = search_query {
                if !q.is_empty() {
                    let pattern = format!("%{}%", q);
                    query = query.filter(
                        documents::title
                            .like(pattern.clone())
                            .or(documents::synopsis.like(pattern)),
                    );
                }
            }
            let count: i64 = query.first(&mut conn).await?;
            Ok(count as u64)
        })
    }

    /// Optimized browse that only loads columns needed for listing.
    /// Avoids loading `extracted_text` which can be very large (OCR text).
    /// Two-step query: fetch document page first, then batch-load latest versions.
    pub async fn browse_fast(
        &self,
        source_id: Option<&str>,
        _status: Option<&str>,
        categories: &[String],
        tags: &[String],
        limit: u32,
        offset: u32,
    ) -> Result<Vec<super::BrowseRow>, DieselError> {
        use crate::schema::document_versions;

        with_conn!(self.pool, conn, {
            // Step 1: fetch the page of documents that have at least one version
            // Use EXISTS subquery to filter out versionless documents
            let mut query = documents::table
                .select((
                    documents::id,
                    documents::title,
                    documents::source_id,
                    documents::synopsis,
                    documents::tags,
                ))
                .filter(diesel::dsl::exists(
                    document_versions::table
                        .filter(document_versions::document_id.eq(documents::id))
                        .select(document_versions::id),
                ))
                .order(documents::updated_at.desc())
                .limit(limit as i64)
                .offset(offset as i64)
                .into_boxed();

            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }
            if !categories.is_empty() {
                query = query.filter(documents::category_id.eq_any(categories));
            }
            for tag in tags {
                let pattern = format!("%{}%", tag);
                query = query.filter(documents::tags.like(pattern));
            }

            #[allow(clippy::type_complexity)]
            let doc_rows: Vec<(
                String,
                String,
                String,
                Option<String>,
                Option<String>,
            )> = query.load(&mut conn).await?;

            if doc_rows.is_empty() {
                return Ok(Vec::new());
            }

            let doc_ids: Vec<&str> = doc_rows.iter().map(|r| r.0.as_str()).collect();

            // Step 2: fetch all versions for these documents, ordered by id desc
            let version_rows: Vec<(String, Option<String>, String, i32, String)> =
                document_versions::table
                    .filter(document_versions::document_id.eq_any(&doc_ids))
                    .order(document_versions::id.desc())
                    .select((
                        document_versions::document_id,
                        document_versions::original_filename,
                        document_versions::mime_type,
                        document_versions::file_size,
                        document_versions::acquired_at,
                    ))
                    .load(&mut conn)
                    .await?;

            // Take only the latest version per document (first seen per document_id)
            let mut latest_versions: HashMap<&str, (Option<String>, String, i32, String)> =
                HashMap::new();
            for (doc_id, filename, mime, size, acquired) in &version_rows {
                latest_versions
                    .entry(doc_id.as_str())
                    .or_insert_with(|| (filename.clone(), mime.clone(), *size, acquired.clone()));
            }

            // Combine in document order
            let results: Vec<super::BrowseRow> = doc_rows
                .into_iter()
                .filter_map(|(id, title, source_id, synopsis, tags)| {
                    let (filename, mime, size, acquired) = latest_versions.remove(id.as_str())?;
                    Some(super::BrowseRow {
                        id,
                        title,
                        source_id,
                        synopsis,
                        tags,
                        original_filename: filename,
                        mime_type: mime,
                        file_size: size,
                        acquired_at: acquired,
                    })
                })
                .collect();

            Ok(results)
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
                           FROM documents, json_each(documents.tags)
                           WHERE documents.tags IS NOT NULL AND documents.tags != '[]'
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
                           FROM documents, jsonb_array_elements_text(documents.tags::jsonb) as tag
                           WHERE documents.tags IS NOT NULL AND documents.tags != '[]'
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
    // Timeline Operations
    // ========================================================================

    /// Get timeline buckets (daily counts) for documents by publication date.
    ///
    /// Returns (date_string, timestamp, count) tuples grouped by day.
    /// Uses `manual_date` if set, otherwise `estimated_date`.
    /// Only includes documents that have a publication date.
    /// Optionally filtered by source_id and date range.
    pub async fn get_timeline_buckets(
        &self,
        source_id: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> Result<Vec<(String, i64, u64)>, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct TimelineBucket {
            #[diesel(sql_type = diesel::sql_types::Text)]
            date_bucket: String,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            count: i64,
        }

        // Use publication date: prefer manual_date, fall back to estimated_date
        // Only include documents that have at least one of these dates
        let date_expr = "COALESCE(manual_date, estimated_date)";
        let base_query = format!(
            "SELECT date({}) as date_bucket, COUNT(*) as count FROM documents",
            date_expr
        );

        // Always filter to documents with a publication date
        let mut conditions = vec![format!("{} IS NOT NULL", date_expr)];

        if source_id.is_some() {
            conditions.push("source_id = $1".to_string());
        }
        if start_date.is_some() {
            let idx = if source_id.is_some() { "$2" } else { "$1" };
            conditions.push(format!("date({}) >= {}", date_expr, idx));
        }
        if end_date.is_some() {
            let idx = match (source_id.is_some(), start_date.is_some()) {
                (true, true) => "$3",
                (true, false) | (false, true) => "$2",
                (false, false) => "$1",
            };
            conditions.push(format!("date({}) <= {}", date_expr, idx));
        }

        let where_clause = format!(" WHERE {}", conditions.join(" AND "));

        let query = format!(
            "{}{} GROUP BY date_bucket ORDER BY date_bucket ASC",
            base_query, where_clause
        );

        with_conn!(self.pool, conn, {
            use diesel_async::RunQueryDsl;

            // Build and execute query with appropriate bindings
            let results: Vec<TimelineBucket> = match (source_id, start_date, end_date) {
                (Some(sid), Some(start), Some(end)) => {
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(sid)
                        .bind::<diesel::sql_types::Text, _>(start)
                        .bind::<diesel::sql_types::Text, _>(end)
                        .load(&mut conn)
                        .await?
                }
                (Some(sid), Some(start), None) => {
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(sid)
                        .bind::<diesel::sql_types::Text, _>(start)
                        .load(&mut conn)
                        .await?
                }
                (Some(sid), None, Some(end)) => {
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(sid)
                        .bind::<diesel::sql_types::Text, _>(end)
                        .load(&mut conn)
                        .await?
                }
                (Some(sid), None, None) => {
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(sid)
                        .load(&mut conn)
                        .await?
                }
                (None, Some(start), Some(end)) => {
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(start)
                        .bind::<diesel::sql_types::Text, _>(end)
                        .load(&mut conn)
                        .await?
                }
                (None, Some(start), None) => {
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(start)
                        .load(&mut conn)
                        .await?
                }
                (None, None, Some(end)) => {
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(end)
                        .load(&mut conn)
                        .await?
                }
                (None, None, None) => diesel::sql_query(&query).load(&mut conn).await?,
            };

            // Convert to output format with timestamps
            let buckets: Vec<(String, i64, u64)> = results
                .into_iter()
                .map(|b| {
                    // Parse date string to timestamp (midnight UTC)
                    let timestamp = chrono::NaiveDate::parse_from_str(&b.date_bucket, "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
                        .unwrap_or(0);
                    (b.date_bucket, timestamp, b.count as u64)
                })
                .collect();

            Ok(buckets)
        })
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
    #[allow(dead_code)]
    pub async fn get_needing_ocr(&self, limit: usize) -> Result<Vec<Document>, DieselError> {
        self.get_needing_ocr_filtered(limit, None).await
    }

    /// Get documents needing OCR with optional mime type filter.
    pub async fn get_needing_ocr_filtered(
        &self,
        limit: usize,
        mime_type: Option<&str>,
    ) -> Result<Vec<Document>, DieselError> {
        use crate::schema::document_versions;

        let records: Vec<DocumentRecord> = with_conn!(self.pool, conn, {
            if let Some(mime) = mime_type {
                // Join with versions to filter by mime type
                let doc_ids: Vec<String> = documents::table
                    .inner_join(
                        document_versions::table
                            .on(document_versions::document_id.eq(documents::id)),
                    )
                    .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
                    .filter(document_versions::mime_type.eq(mime))
                    .select(documents::id)
                    .distinct()
                    .limit(limit as i64)
                    .load(&mut conn)
                    .await?;

                documents::table
                    .filter(documents::id.eq_any(doc_ids))
                    .load(&mut conn)
                    .await
            } else {
                documents::table
                    .filter(documents::status.eq_any(vec!["pending", "downloaded"]))
                    .limit(limit as i64)
                    .load(&mut conn)
                    .await
            }
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

    /// Reset annotations for documents, allowing them to be re-annotated.
    /// Sets status back to ocr_complete and clears synopsis/tags.
    pub async fn reset_annotations(&self, source_id: Option<&str>) -> Result<u64, DieselError> {
        let count: u64 = with_conn!(self.pool, conn, {
            let mut query = diesel::update(documents::table)
                .filter(documents::status.eq("indexed"))
                .into_boxed();

            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }

            query
                .set((
                    documents::status.eq("ocr_complete"),
                    documents::synopsis.eq(None::<String>),
                    documents::tags.eq(None::<String>),
                ))
                .execute(&mut conn)
                .await
        })? as u64;

        Ok(count)
    }

    /// Count documents that have been annotated (status = indexed).
    pub async fn count_annotated(&self, source_id: Option<&str>) -> Result<u64, DieselError> {
        with_conn!(self.pool, conn, {
            let mut query = documents::table
                .filter(documents::status.eq("indexed"))
                .into_boxed();

            if let Some(sid) = source_id {
                query = query.filter(documents::source_id.eq(sid));
            }

            query
                .count()
                .get_result::<i64>(&mut conn)
                .await
                .map(|c| c as u64)
        })
    }

    /// Update synopsis and tags for a document.
    pub async fn update_synopsis_and_tags(
        &self,
        id: &str,
        synopsis: Option<&str>,
        tags: &[String],
    ) -> Result<(), DieselError> {
        let now = Utc::now().to_rfc3339();
        let tags_json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".to_string());

        with_conn!(self.pool, conn, {
            diesel::update(documents::table.find(id))
                .set((
                    documents::synopsis.eq(synopsis),
                    documents::tags.eq(&tags_json),
                    documents::status.eq("indexed"),
                    documents::updated_at.eq(&now),
                ))
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }
}
