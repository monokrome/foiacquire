//! Document page and OCR operations.

use std::collections::HashMap;

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{DieselDocumentRepository, LastInsertRowId, OcrResult};
use crate::models::{DocumentPage, PageOcrStatus};
use crate::repository::diesel_models::{DocumentPageRecord, NewPageOcrResult, PageOcrResultRecord};
use crate::repository::pool::DieselError;
use crate::schema::{document_pages, page_ocr_results};
use crate::{with_conn, with_conn_split};

impl DieselDocumentRepository {
    /// Count pages for a document.
    pub async fn count_pages(&self, document_id: &str, version: i32) -> Result<u32, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = document_pages::table
                .filter(document_pages::document_id.eq(document_id))
                .filter(document_pages::version_id.eq(version))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count as u32)
        })
    }

    /// Save a document page. Returns the page ID.
    pub async fn save_page(&self, page: &DocumentPage) -> Result<i64, DieselError> {
        let now = Utc::now().to_rfc3339();

        with_conn_split!(self.pool,
            sqlite: conn => {
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
                let result: LastInsertRowId = diesel::sql_query("SELECT last_insert_rowid()")
                    .get_result(&mut conn)
                    .await?;
                Ok(result.id)
            },
            postgres: conn => {
                use diesel::upsert::excluded;
                let result: i32 = diesel::insert_into(document_pages::table)
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
                    .on_conflict((
                        document_pages::document_id,
                        document_pages::version_id,
                        document_pages::page_number,
                    ))
                    .do_update()
                    .set((
                        document_pages::pdf_text.eq(excluded(document_pages::pdf_text)),
                        document_pages::ocr_text.eq(excluded(document_pages::ocr_text)),
                        document_pages::final_text.eq(excluded(document_pages::final_text)),
                        document_pages::ocr_status.eq(excluded(document_pages::ocr_status)),
                        document_pages::updated_at.eq(excluded(document_pages::updated_at)),
                    ))
                    .returning(document_pages::id)
                    .get_result(&mut conn)
                    .await?;
                Ok(result as i64)
            }
        )
    }

    /// Get document pages.
    pub async fn get_pages(
        &self,
        document_id: &str,
        version: i32,
    ) -> Result<Vec<DocumentPage>, DieselError> {
        let records: Vec<DocumentPageRecord> = with_conn!(self.pool, conn, {
            document_pages::table
                .filter(document_pages::document_id.eq(document_id))
                .filter(document_pages::version_id.eq(version))
                .order(document_pages::page_number.asc())
                .load(&mut conn)
                .await
        })?;

        Ok(records
            .into_iter()
            .map(|r| DocumentPage {
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

    /// Get pages needing OCR.
    pub async fn get_pages_needing_ocr(
        &self,
        document_id: &str,
        version_id: i32,
        limit: usize,
    ) -> Result<Vec<DocumentPage>, DieselError> {
        let records: Vec<DocumentPageRecord> = with_conn!(self.pool, conn, {
            document_pages::table
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
                .await
        })?;

        Ok(records
            .into_iter()
            .map(|r| DocumentPage {
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

    /// Store OCR result for a page from a specific backend.
    /// Stores in page_ocr_results table and updates page's ocr_text/status.
    pub async fn store_page_ocr_result(
        &self,
        page_id: i64,
        backend: &str,
        text: Option<&str>,
        confidence: Option<f32>,
        processing_time_ms: Option<i32>,
    ) -> Result<(), DieselError> {
        let now = Utc::now().to_rfc3339();
        let char_count = text.map(|t| t.chars().count() as i32);
        let word_count = text.map(|t| t.split_whitespace().count() as i32);

        let new_result = NewPageOcrResult {
            page_id: page_id as i32,
            backend,
            text,
            confidence,
            quality_score: None, // Computed later if needed
            char_count,
            word_count,
            processing_time_ms,
            error_message: None,
            created_at: &now,
        };

        with_conn!(self.pool, conn, {
            // Upsert: insert or update if backend already exists for this page
            diesel::insert_into(page_ocr_results::table)
                .values(&new_result)
                .on_conflict((page_ocr_results::page_id, page_ocr_results::backend))
                .do_update()
                .set((
                    page_ocr_results::text.eq(text),
                    page_ocr_results::confidence.eq(confidence),
                    page_ocr_results::char_count.eq(char_count),
                    page_ocr_results::word_count.eq(word_count),
                    page_ocr_results::processing_time_ms.eq(processing_time_ms),
                    page_ocr_results::created_at.eq(&now),
                ))
                .execute(&mut conn)
                .await?;

            // Also update the page's ocr_text for backwards compatibility
            diesel::update(document_pages::table.find(page_id as i32))
                .set((
                    document_pages::ocr_text.eq(text),
                    document_pages::ocr_status.eq("ocr_complete"),
                ))
                .execute(&mut conn)
                .await?;

            Ok(())
        })
    }

    /// Store OCR error for a page from a specific backend.
    pub async fn store_page_ocr_error(
        &self,
        page_id: i64,
        backend: &str,
        error_message: &str,
    ) -> Result<(), DieselError> {
        let now = Utc::now().to_rfc3339();

        let new_result = NewPageOcrResult {
            page_id: page_id as i32,
            backend,
            text: None,
            confidence: None,
            quality_score: None,
            char_count: None,
            word_count: None,
            processing_time_ms: None,
            error_message: Some(error_message),
            created_at: &now,
        };

        with_conn!(self.pool, conn, {
            diesel::insert_into(page_ocr_results::table)
                .values(&new_result)
                .on_conflict((page_ocr_results::page_id, page_ocr_results::backend))
                .do_update()
                .set((
                    page_ocr_results::text.eq::<Option<&str>>(None),
                    page_ocr_results::error_message.eq(Some(error_message)),
                    page_ocr_results::created_at.eq(&now),
                ))
                .execute(&mut conn)
                .await?;

            Ok(())
        })
    }

    /// Get all OCR results for a page from different backends.
    pub async fn get_page_ocr_results(
        &self,
        page_id: i64,
    ) -> Result<Vec<PageOcrResultRecord>, DieselError> {
        with_conn!(self.pool, conn, {
            page_ocr_results::table
                .filter(page_ocr_results::page_id.eq(page_id as i32))
                .order(page_ocr_results::created_at.desc())
                .load(&mut conn)
                .await
        })
    }

    /// Delete pages for a document version.
    pub async fn delete_pages(
        &self,
        document_id: &str,
        version_id: i32,
    ) -> Result<(), DieselError> {
        with_conn!(self.pool, conn, {
            diesel::delete(
                document_pages::table
                    .filter(document_pages::document_id.eq(document_id))
                    .filter(document_pages::version_id.eq(version_id)),
            )
            .execute(&mut conn)
            .await?;
            Ok(())
        })
    }

    /// Check if all pages are complete.
    pub async fn are_all_pages_complete(
        &self,
        document_id: &str,
        version_id: i32,
    ) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
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
        })
    }

    /// Count pages needing OCR across all documents.
    pub async fn count_pages_needing_ocr(&self) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
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
        })
    }

    /// Get pages needing OCR across all documents.
    pub async fn get_all_pages_needing_ocr(
        &self,
        limit: usize,
    ) -> Result<Vec<DocumentPage>, DieselError> {
        let records: Vec<DocumentPageRecord> = with_conn!(self.pool, conn, {
            document_pages::table
                .filter(
                    document_pages::ocr_status
                        .eq("pending")
                        .or(document_pages::ocr_status.eq("text_extracted")),
                )
                .limit(limit as i64)
                .load(&mut conn)
                .await
        })?;

        Ok(records
            .into_iter()
            .map(|r| DocumentPage {
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

    /// Get combined page text for a document.
    pub async fn get_combined_page_text(
        &self,
        document_id: &str,
        version: i32,
    ) -> Result<Option<String>, DieselError> {
        let texts: Vec<Option<String>> = with_conn!(self.pool, conn, {
            document_pages::table
                .filter(document_pages::document_id.eq(document_id))
                .filter(document_pages::version_id.eq(version))
                .order(document_pages::page_number.asc())
                .select(document_pages::ocr_text)
                .load(&mut conn)
                .await
        })?;

        let combined: String = texts.into_iter().flatten().collect::<Vec<_>>().join("\n\n");

        if combined.is_empty() {
            Ok(None)
        } else {
            Ok(Some(combined))
        }
    }

    /// Get OCR results for pages in bulk (stub).
    pub async fn get_pages_ocr_results_bulk(
        &self,
        _page_ids: &[i64],
    ) -> Result<HashMap<i64, Vec<OcrResult>>, DieselError> {
        Ok(HashMap::new())
    }

    /// Get pages without a specific OCR backend (stub).
    pub async fn get_pages_without_backend(
        &self,
        _document_id: &str,
        _backend: &str,
    ) -> Result<Vec<DocumentPage>, DieselError> {
        Ok(vec![])
    }
}
