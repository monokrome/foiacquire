//! Document analysis result operations.

// Analysis result types not yet used externally
#![allow(dead_code)]

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::DieselDocumentRepository;
use crate::repository::diesel_models::{DocumentAnalysisResultRecord, NewDocumentAnalysisResult};
use crate::repository::pool::DieselError;
use crate::schema::document_analysis_results;
use crate::{with_conn, with_conn_split};

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
        result_text: Option<&str>,
        confidence: Option<f32>,
        processing_time_ms: Option<u64>,
        error: Option<&str>,
        metadata: Option<&serde_json::Value>,
    ) -> Result<i64, DieselError> {
        let now = Utc::now().to_rfc3339();
        let status = if error.is_some() {
            AnalysisResultStatus::Failed.as_str()
        } else {
            AnalysisResultStatus::Complete.as_str()
        };
        let metadata_str = metadata.map(|m| serde_json::to_string(m).unwrap_or_default());
        let processing_time = processing_time_ms.map(|ms| ms as i32);

        with_conn_split!(self.pool,
            sqlite: conn => {
                diesel::replace_into(document_analysis_results::table)
                    .values(NewDocumentAnalysisResult {
                        page_id: Some(page_id as i32),
                        document_id,
                        version_id,
                        analysis_type,
                        backend,
                        result_text,
                        confidence,
                        processing_time_ms: processing_time,
                        error,
                        status,
                        created_at: &now,
                        metadata: metadata_str.as_deref(),
                    })
                    .execute(&mut conn)
                    .await?;
                let result: super::LastInsertRowId = diesel::sql_query("SELECT last_insert_rowid()")
                    .get_result(&mut conn)
                    .await?;
                Ok(result.id)
            },
            postgres: conn => {
                use diesel::upsert::{excluded, on_constraint};
                // Use partial unique index name for ON CONFLICT with page-level results
                let result: i32 = diesel::insert_into(document_analysis_results::table)
                    .values(NewDocumentAnalysisResult {
                        page_id: Some(page_id as i32),
                        document_id,
                        version_id,
                        analysis_type,
                        backend,
                        result_text,
                        confidence,
                        processing_time_ms: processing_time,
                        error,
                        status,
                        created_at: &now,
                        metadata: metadata_str.as_deref(),
                    })
                    .on_conflict(on_constraint("idx_analysis_results_page_unique"))
                    .do_update()
                    .set((
                        document_analysis_results::result_text.eq(excluded(document_analysis_results::result_text)),
                        document_analysis_results::confidence.eq(excluded(document_analysis_results::confidence)),
                        document_analysis_results::processing_time_ms.eq(excluded(document_analysis_results::processing_time_ms)),
                        document_analysis_results::error.eq(excluded(document_analysis_results::error)),
                        document_analysis_results::status.eq(excluded(document_analysis_results::status)),
                        document_analysis_results::created_at.eq(excluded(document_analysis_results::created_at)),
                        document_analysis_results::metadata.eq(excluded(document_analysis_results::metadata)),
                    ))
                    .returning(document_analysis_results::id)
                    .get_result(&mut conn)
                    .await?;
                Ok(result as i64)
            }
        )
    }

    /// Store an analysis result for a document (document-level, no page).
    #[allow(clippy::too_many_arguments)]
    pub async fn store_analysis_result_for_document(
        &self,
        document_id: &str,
        version_id: i32,
        analysis_type: &str,
        backend: &str,
        result_text: Option<&str>,
        confidence: Option<f32>,
        processing_time_ms: Option<u64>,
        error: Option<&str>,
        metadata: Option<&serde_json::Value>,
    ) -> Result<i64, DieselError> {
        let now = Utc::now().to_rfc3339();
        let status = if error.is_some() {
            AnalysisResultStatus::Failed.as_str()
        } else {
            AnalysisResultStatus::Complete.as_str()
        };
        let metadata_str = metadata.map(|m| serde_json::to_string(m).unwrap_or_default());
        let processing_time = processing_time_ms.map(|ms| ms as i32);

        with_conn_split!(self.pool,
            sqlite: conn => {
                diesel::replace_into(document_analysis_results::table)
                    .values(NewDocumentAnalysisResult {
                        page_id: None,
                        document_id,
                        version_id,
                        analysis_type,
                        backend,
                        result_text,
                        confidence,
                        processing_time_ms: processing_time,
                        error,
                        status,
                        created_at: &now,
                        metadata: metadata_str.as_deref(),
                    })
                    .execute(&mut conn)
                    .await?;
                let result: super::LastInsertRowId = diesel::sql_query("SELECT last_insert_rowid()")
                    .get_result(&mut conn)
                    .await?;
                Ok(result.id)
            },
            postgres: conn => {
                use diesel::upsert::{excluded, on_constraint};
                // Use partial unique index name for ON CONFLICT with document-level results
                let result: i32 = diesel::insert_into(document_analysis_results::table)
                    .values(NewDocumentAnalysisResult {
                        page_id: None,
                        document_id,
                        version_id,
                        analysis_type,
                        backend,
                        result_text,
                        confidence,
                        processing_time_ms: processing_time,
                        error,
                        status,
                        created_at: &now,
                        metadata: metadata_str.as_deref(),
                    })
                    .on_conflict(on_constraint("idx_analysis_results_doc_unique"))
                    .do_update()
                    .set((
                        document_analysis_results::result_text.eq(excluded(document_analysis_results::result_text)),
                        document_analysis_results::confidence.eq(excluded(document_analysis_results::confidence)),
                        document_analysis_results::processing_time_ms.eq(excluded(document_analysis_results::processing_time_ms)),
                        document_analysis_results::error.eq(excluded(document_analysis_results::error)),
                        document_analysis_results::status.eq(excluded(document_analysis_results::status)),
                        document_analysis_results::created_at.eq(excluded(document_analysis_results::created_at)),
                        document_analysis_results::metadata.eq(excluded(document_analysis_results::metadata)),
                    ))
                    .returning(document_analysis_results::id)
                    .get_result(&mut conn)
                    .await?;
                Ok(result as i64)
            }
        )
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
