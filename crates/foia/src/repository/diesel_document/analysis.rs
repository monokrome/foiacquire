//! Document analysis result operations.

// Analysis result types not yet used externally
#![allow(dead_code)]

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{DieselDocumentRepository, ReturningId};
use crate::repository::models::DocumentAnalysisResultRecord;
use crate::repository::pool::DieselError;
use crate::schema::document_analysis_results;
use crate::with_conn;

/// Analysis result status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisResultStatus {
    Pending,
    Complete,
    Failed,
}

impl AnalysisResultStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A stored analysis result.
#[derive(Debug, Clone)]
pub struct AnalysisResultEntry {
    pub id: i64,
    pub page_id: Option<i64>,
    pub document_id: String,
    pub version_id: i64,
    pub analysis_type: String,
    pub backend: String,
    pub result_text: Option<String>,
    pub confidence: Option<f32>,
    pub processing_time_ms: Option<u64>,
    pub error: Option<String>,
    pub status: AnalysisResultStatus,
    pub created_at: String,
    pub metadata: Option<serde_json::Value>,
}

impl From<DocumentAnalysisResultRecord> for AnalysisResultEntry {
    fn from(r: DocumentAnalysisResultRecord) -> Self {
        Self {
            id: r.id as i64,
            page_id: r.page_id.map(|id| id as i64),
            document_id: r.document_id,
            version_id: r.version_id as i64,
            analysis_type: r.analysis_type,
            backend: r.backend,
            result_text: r.result_text,
            confidence: r.confidence,
            processing_time_ms: r.processing_time_ms.map(|ms| ms as u64),
            error: r.error,
            status: AnalysisResultStatus::from_str(&r.status)
                .unwrap_or(AnalysisResultStatus::Pending),
            created_at: r.created_at,
            metadata: r.metadata.and_then(|s| serde_json::from_str(&s).ok()),
        }
    }
}

impl DieselDocumentRepository {
    /// Store an analysis result for a page.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_analysis_result_for_page(
        &self,
        page_id: i64,
        document_id: &str,
        version_id: i32,
        analysis_type: &str,
        backend: &str,
        model: Option<&str>,
        result_text: Option<&str>,
        confidence: Option<f32>,
        processing_time_ms: Option<u64>,
        error: Option<&str>,
        metadata: Option<&serde_json::Value>,
    ) -> Result<i64, DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::DocumentAnalysisResults as Dar;
        use sea_query::{Expr, OnConflict, Query};

        let now = Utc::now().to_rfc3339();
        let status = if error.is_some() {
            AnalysisResultStatus::Failed.as_str()
        } else {
            AnalysisResultStatus::Complete.as_str()
        };
        let metadata_str = metadata.map(|m| serde_json::to_string(m).unwrap_or_default());
        let processing_time = processing_time_ms.map(|ms| ms as i32);
        let page_id_i32 = page_id as i32;

        let stmt = Query::insert()
            .into_table(Dar::Table)
            .columns([
                Dar::PageId,
                Dar::DocumentId,
                Dar::VersionId,
                Dar::AnalysisType,
                Dar::Backend,
                Dar::ResultText,
                Dar::Confidence,
                Dar::ProcessingTimeMs,
                Dar::Error,
                Dar::Status,
                Dar::CreatedAt,
                Dar::Metadata,
                Dar::Model,
            ])
            .values_panic([
                Some(page_id_i32).into(),
                document_id.to_string().into(),
                version_id.into(),
                analysis_type.to_string().into(),
                backend.to_string().into(),
                result_text.map(|s| s.to_string()).into(),
                confidence.into(),
                processing_time.into(),
                error.map(|s| s.to_string()).into(),
                status.to_string().into(),
                now.clone().into(),
                metadata_str.clone().into(),
                model.map(|s| s.to_string()).into(),
            ])
            .on_conflict(
                OnConflict::new()
                    .expr(Expr::col(Dar::PageId))
                    .expr(Expr::col(Dar::AnalysisType))
                    .expr(Expr::col(Dar::Backend))
                    .expr(Expr::cust("COALESCE(\"model\", '')"))
                    .target_and_where(Expr::cust("page_id IS NOT NULL"))
                    .update_columns([
                        Dar::ResultText,
                        Dar::Confidence,
                        Dar::ProcessingTimeMs,
                        Dar::Error,
                        Dar::Status,
                        Dar::CreatedAt,
                        Dar::Metadata,
                        Dar::Model,
                    ])
                    .to_owned(),
            )
            .returning_col(Dar::Id)
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            let result: ReturningId = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(Some(
                    page_id_i32,
                ))
                .bind::<diesel::sql_types::Text, _>(document_id)
                .bind::<diesel::sql_types::Integer, _>(version_id)
                .bind::<diesel::sql_types::Text, _>(analysis_type)
                .bind::<diesel::sql_types::Text, _>(backend)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(result_text)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Float>, _>(confidence)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(processing_time)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(error)
                .bind::<diesel::sql_types::Text, _>(status)
                .bind::<diesel::sql_types::Text, _>(&now)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    metadata_str.as_deref(),
                )
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(model)
                .get_result(&mut conn)
                .await?;
            Ok(result.id as i64)
        })
    }

    /// Store an analysis result for a document (document-level, no page).
    #[allow(clippy::too_many_arguments)]
    pub async fn store_analysis_result_for_document(
        &self,
        document_id: &str,
        version_id: i32,
        analysis_type: &str,
        backend: &str,
        model: Option<&str>,
        result_text: Option<&str>,
        confidence: Option<f32>,
        processing_time_ms: Option<u64>,
        error: Option<&str>,
        metadata: Option<&serde_json::Value>,
    ) -> Result<i64, DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::DocumentAnalysisResults as Dar;
        use sea_query::{Expr, OnConflict, Query};

        // Clean up any pending claim rows for this document/version/analysis_type.
        // Claims use backend='pending' which won't conflict with real backend names,
        // so we must explicitly delete them.
        with_conn!(self.pool, conn, {
            diesel::delete(
                document_analysis_results::table
                    .filter(document_analysis_results::document_id.eq(document_id))
                    .filter(document_analysis_results::version_id.eq(version_id))
                    .filter(document_analysis_results::analysis_type.eq(analysis_type))
                    .filter(document_analysis_results::backend.eq("pending"))
                    .filter(document_analysis_results::status.eq("pending"))
                    .filter(document_analysis_results::page_id.is_null()),
            )
            .execute(&mut conn)
            .await?;
            Ok::<(), DieselError>(())
        })?;

        let now = Utc::now().to_rfc3339();
        let status = if error.is_some() {
            AnalysisResultStatus::Failed.as_str()
        } else {
            AnalysisResultStatus::Complete.as_str()
        };
        let metadata_str = metadata.map(|m| serde_json::to_string(m).unwrap_or_default());
        let processing_time = processing_time_ms.map(|ms| ms as i32);

        let stmt = Query::insert()
            .into_table(Dar::Table)
            .columns([
                Dar::PageId,
                Dar::DocumentId,
                Dar::VersionId,
                Dar::AnalysisType,
                Dar::Backend,
                Dar::ResultText,
                Dar::Confidence,
                Dar::ProcessingTimeMs,
                Dar::Error,
                Dar::Status,
                Dar::CreatedAt,
                Dar::Metadata,
                Dar::Model,
            ])
            .values_panic([
                Option::<i32>::None.into(),
                document_id.to_string().into(),
                version_id.into(),
                analysis_type.to_string().into(),
                backend.to_string().into(),
                result_text.map(|s| s.to_string()).into(),
                confidence.into(),
                processing_time.into(),
                error.map(|s| s.to_string()).into(),
                status.to_string().into(),
                now.clone().into(),
                metadata_str.clone().into(),
                model.map(|s| s.to_string()).into(),
            ])
            .on_conflict(
                OnConflict::new()
                    .expr(Expr::col(Dar::DocumentId))
                    .expr(Expr::col(Dar::VersionId))
                    .expr(Expr::col(Dar::AnalysisType))
                    .expr(Expr::col(Dar::Backend))
                    .expr(Expr::cust("COALESCE(\"model\", '')"))
                    .target_and_where(Expr::cust("page_id IS NULL"))
                    .update_columns([
                        Dar::ResultText,
                        Dar::Confidence,
                        Dar::ProcessingTimeMs,
                        Dar::Error,
                        Dar::Status,
                        Dar::CreatedAt,
                        Dar::Metadata,
                        Dar::Model,
                    ])
                    .to_owned(),
            )
            .returning_col(Dar::Id)
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            let result: ReturningId = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(None::<i32>)
                .bind::<diesel::sql_types::Text, _>(document_id)
                .bind::<diesel::sql_types::Integer, _>(version_id)
                .bind::<diesel::sql_types::Text, _>(analysis_type)
                .bind::<diesel::sql_types::Text, _>(backend)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(result_text)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Float>, _>(confidence)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(processing_time)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(error)
                .bind::<diesel::sql_types::Text, _>(status)
                .bind::<diesel::sql_types::Text, _>(&now)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    metadata_str.as_deref(),
                )
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(model)
                .get_result(&mut conn)
                .await?;
            Ok(result.id as i64)
        })
    }

    /// Get analysis results for a document.
    pub async fn get_analysis_results(
        &self,
        document_id: &str,
        version_id: i32,
    ) -> Result<Vec<AnalysisResultEntry>, DieselError> {
        let records: Vec<DocumentAnalysisResultRecord> = with_conn!(self.pool, conn, {
            document_analysis_results::table
                .filter(document_analysis_results::document_id.eq(document_id))
                .filter(document_analysis_results::version_id.eq(version_id))
                .order(document_analysis_results::created_at.desc())
                .load(&mut conn)
                .await
        })?;

        Ok(records.into_iter().map(AnalysisResultEntry::from).collect())
    }

    /// Get analysis results for a specific page.
    pub async fn get_analysis_results_for_page(
        &self,
        page_id: i64,
    ) -> Result<Vec<AnalysisResultEntry>, DieselError> {
        let records: Vec<DocumentAnalysisResultRecord> = with_conn!(self.pool, conn, {
            document_analysis_results::table
                .filter(document_analysis_results::page_id.eq(Some(page_id as i32)))
                .order(document_analysis_results::created_at.desc())
                .load(&mut conn)
                .await
        })?;

        Ok(records.into_iter().map(AnalysisResultEntry::from).collect())
    }

    /// Get analysis results by type for a document.
    pub async fn get_analysis_results_by_type(
        &self,
        document_id: &str,
        version_id: i32,
        analysis_type: &str,
    ) -> Result<Vec<AnalysisResultEntry>, DieselError> {
        let records: Vec<DocumentAnalysisResultRecord> = with_conn!(self.pool, conn, {
            document_analysis_results::table
                .filter(document_analysis_results::document_id.eq(document_id))
                .filter(document_analysis_results::version_id.eq(version_id))
                .filter(document_analysis_results::analysis_type.eq(analysis_type))
                .order(document_analysis_results::created_at.desc())
                .load(&mut conn)
                .await
        })?;

        Ok(records.into_iter().map(AnalysisResultEntry::from).collect())
    }

    /// Check if analysis exists for a page with given type and backend.
    pub async fn has_analysis_result_for_page(
        &self,
        page_id: i64,
        analysis_type: &str,
        backend: &str,
    ) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = document_analysis_results::table
                .filter(document_analysis_results::page_id.eq(Some(page_id as i32)))
                .filter(document_analysis_results::analysis_type.eq(analysis_type))
                .filter(document_analysis_results::backend.eq(backend))
                .filter(document_analysis_results::status.eq("complete"))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count > 0)
        })
    }

    /// Check if analysis exists for a document with given type and backend.
    pub async fn has_analysis_result_for_document(
        &self,
        document_id: &str,
        version_id: i32,
        analysis_type: &str,
        backend: &str,
    ) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = document_analysis_results::table
                .filter(document_analysis_results::document_id.eq(document_id))
                .filter(document_analysis_results::version_id.eq(version_id))
                .filter(document_analysis_results::analysis_type.eq(analysis_type))
                .filter(document_analysis_results::backend.eq(backend))
                .filter(document_analysis_results::page_id.is_null())
                .filter(document_analysis_results::status.eq("complete"))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count > 0)
        })
    }

    /// Get combined analysis text for a document (all page results concatenated).
    pub async fn get_combined_analysis_text(
        &self,
        document_id: &str,
        version_id: i32,
        analysis_type: &str,
    ) -> Result<Option<String>, DieselError> {
        // Get page-level results ordered by page_id
        let texts: Vec<Option<String>> = with_conn!(self.pool, conn, {
            document_analysis_results::table
                .filter(document_analysis_results::document_id.eq(document_id))
                .filter(document_analysis_results::version_id.eq(version_id))
                .filter(document_analysis_results::analysis_type.eq(analysis_type))
                .filter(document_analysis_results::page_id.is_not_null())
                .filter(document_analysis_results::status.eq("complete"))
                .order(document_analysis_results::page_id.asc())
                .select(document_analysis_results::result_text)
                .load(&mut conn)
                .await
        })?;

        let combined: String = texts.into_iter().flatten().collect::<Vec<_>>().join("\n\n");

        if combined.is_empty() {
            // Check for document-level result
            let doc_text: Option<Option<String>> = with_conn!(self.pool, conn, {
                document_analysis_results::table
                    .filter(document_analysis_results::document_id.eq(document_id))
                    .filter(document_analysis_results::version_id.eq(version_id))
                    .filter(document_analysis_results::analysis_type.eq(analysis_type))
                    .filter(document_analysis_results::page_id.is_null())
                    .filter(document_analysis_results::status.eq("complete"))
                    .select(document_analysis_results::result_text)
                    .first(&mut conn)
                    .await
                    .optional()
            })?;

            Ok(doc_text.flatten())
        } else {
            Ok(Some(combined))
        }
    }

    /// Delete analysis results for a document.
    pub async fn delete_analysis_results(
        &self,
        document_id: &str,
        version_id: i32,
    ) -> Result<usize, DieselError> {
        with_conn!(self.pool, conn, {
            diesel::delete(
                document_analysis_results::table
                    .filter(document_analysis_results::document_id.eq(document_id))
                    .filter(document_analysis_results::version_id.eq(version_id)),
            )
            .execute(&mut conn)
            .await
        })
    }

    /// Claim a document for analysis by inserting a `pending` result.
    ///
    /// Acts as a distributed lock: other workers will skip documents with a
    /// recent pending result (within 90 minutes). The pending row is overwritten
    /// by the completion/failure upsert when processing finishes.
    pub async fn claim_analysis(
        &self,
        document_id: &str,
        version_id: i32,
        analysis_type: &str,
    ) -> Result<(), DieselError> {
        let now = Utc::now().to_rfc3339();

        with_conn!(self.pool, conn, {
            diesel::sql_query(
                r#"INSERT INTO document_analysis_results
                   (document_id, version_id, analysis_type, backend, status, created_at)
                   VALUES ($1, $2, $3, 'pending', 'pending', $4)
                   ON CONFLICT (document_id, version_id, analysis_type, backend, COALESCE(model, ''))
                   WHERE page_id IS NULL
                   DO UPDATE SET status = 'pending', created_at = $4"#,
            )
            .bind::<diesel::sql_types::Text, _>(document_id)
            .bind::<diesel::sql_types::Integer, _>(version_id)
            .bind::<diesel::sql_types::Text, _>(analysis_type)
            .bind::<diesel::sql_types::Text, _>(&now)
            .execute(&mut conn)
            .await?;
            Ok(())
        })
    }

    /// Backfill `document_analysis_results` completion rows for documents
    /// that are already fully processed (status 'indexed' or 'ocr_complete')
    /// but don't yet have a completion row for the given analysis type.
    ///
    /// Uses a single INSERT ... SELECT for efficiency. Returns the number of
    /// rows inserted.
    pub async fn backfill_analysis_completions(
        &self,
        analysis_type: &str,
    ) -> Result<u64, DieselError> {
        let now = Utc::now().to_rfc3339();

        with_conn!(self.pool, conn, {
            let count = diesel::sql_query(
                r#"INSERT INTO document_analysis_results
                   (page_id, document_id, version_id, analysis_type, backend, status, created_at)
                   SELECT NULL, d.id, dv.id, $1, 'backfill', 'complete', $2
                   FROM documents d
                   JOIN document_versions dv ON dv.document_id = d.id
                   WHERE d.status IN ('indexed', 'ocr_complete')
                   AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
                   AND NOT EXISTS (
                       SELECT 1 FROM document_analysis_results dar
                       WHERE dar.document_id = d.id
                       AND dar.version_id = dv.id
                       AND dar.analysis_type = $3
                       AND dar.status = 'complete'
                   )"#,
            )
            .bind::<diesel::sql_types::Text, _>(analysis_type)
            .bind::<diesel::sql_types::Text, _>(&now)
            .bind::<diesel::sql_types::Text, _>(analysis_type)
            .execute(&mut conn)
            .await?;
            Ok(count as u64)
        })
    }

    /// Count pending analysis for a specific type.
    pub async fn count_pending_analysis(&self, analysis_type: &str) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = document_analysis_results::table
                .filter(document_analysis_results::analysis_type.eq(analysis_type))
                .filter(document_analysis_results::status.eq("pending"))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count as u64)
        })
    }
}
