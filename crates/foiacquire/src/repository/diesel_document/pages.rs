//! Document page and OCR operations.

use std::collections::HashMap;

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{DieselDocumentRepository, OcrResult, ReturningId};
use crate::models::{DocumentPage, PageOcrStatus};
use crate::repository::models::{DocumentPageRecord, PageOcrResultRecord};
use crate::repository::parse_datetime;
use crate::repository::pool::DieselError;
use crate::schema::{document_pages, page_ocr_results};
use crate::with_conn;

impl From<DocumentPageRecord> for DocumentPage {
    fn from(r: DocumentPageRecord) -> Self {
        Self {
            id: r.id as i64,
            document_id: r.document_id,
            version_id: r.version_id as i64,
            page_number: r.page_number as u32,
            pdf_text: r.pdf_text,
            ocr_text: r.ocr_text,
            final_text: r.final_text,
            ocr_status: PageOcrStatus::from_str(&r.ocr_status).unwrap_or(PageOcrStatus::Pending),
            created_at: parse_datetime(&r.created_at),
            updated_at: parse_datetime(&r.updated_at),
        }
    }
}

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
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::DocumentPages;
        use sea_query::{OnConflict, Query};

        let now = Utc::now().to_rfc3339();
        let version_id = page.version_id as i32;
        let page_number = page.page_number as i32;
        let ocr_status = page.ocr_status.as_str().to_string();

        let stmt = Query::insert()
            .into_table(DocumentPages::Table)
            .columns([
                DocumentPages::DocumentId,
                DocumentPages::VersionId,
                DocumentPages::PageNumber,
                DocumentPages::PdfText,
                DocumentPages::OcrText,
                DocumentPages::FinalText,
                DocumentPages::OcrStatus,
                DocumentPages::CreatedAt,
                DocumentPages::UpdatedAt,
            ])
            .values_panic([
                page.document_id.clone().into(),
                version_id.into(),
                page_number.into(),
                page.pdf_text.clone().into(),
                page.ocr_text.clone().into(),
                page.final_text.clone().into(),
                ocr_status.clone().into(),
                now.clone().into(),
                now.clone().into(),
            ])
            .on_conflict(
                OnConflict::columns([
                    DocumentPages::DocumentId,
                    DocumentPages::VersionId,
                    DocumentPages::PageNumber,
                ])
                .update_columns([
                    DocumentPages::PdfText,
                    DocumentPages::OcrText,
                    DocumentPages::FinalText,
                    DocumentPages::OcrStatus,
                    DocumentPages::UpdatedAt,
                ])
                .to_owned(),
            )
            .returning_col(DocumentPages::Id)
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            let result: ReturningId = diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Text, _>(&page.document_id)
                .bind::<diesel::sql_types::Integer, _>(version_id)
                .bind::<diesel::sql_types::Integer, _>(page_number)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&page.pdf_text)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&page.ocr_text)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&page.final_text)
                .bind::<diesel::sql_types::Text, _>(&ocr_status)
                .bind::<diesel::sql_types::Text, _>(&now)
                .bind::<diesel::sql_types::Text, _>(&now)
                .get_result(&mut conn)
                .await?;
            Ok(result.id as i64)
        })
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

        Ok(records.into_iter().map(DocumentPage::from).collect())
    }

    /// Get pages needing OCR.
    #[allow(dead_code)]
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

        Ok(records.into_iter().map(DocumentPage::from).collect())
    }

    /// Store OCR result for a page from a specific backend.
    /// Stores in page_ocr_results table and updates page's ocr_text/status.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_page_ocr_result(
        &self,
        page_id: i64,
        backend: &str,
        model: Option<&str>,
        text: Option<&str>,
        confidence: Option<f32>,
        processing_time_ms: Option<i32>,
        image_hash: Option<&str>,
    ) -> Result<(), DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::PageOcrResults;
        use sea_query::{Expr, OnConflict, Query};

        let now = Utc::now().to_rfc3339();
        let char_count = text.map(|t| t.chars().count() as i32);
        let word_count = text.map(|t| t.split_whitespace().count() as i32);
        let page_id_i32 = page_id as i32;

        let stmt = Query::insert()
            .into_table(PageOcrResults::Table)
            .columns([
                PageOcrResults::PageId,
                PageOcrResults::Backend,
                PageOcrResults::Text,
                PageOcrResults::Confidence,
                PageOcrResults::QualityScore,
                PageOcrResults::CharCount,
                PageOcrResults::WordCount,
                PageOcrResults::ProcessingTimeMs,
                PageOcrResults::ErrorMessage,
                PageOcrResults::CreatedAt,
                PageOcrResults::Model,
                PageOcrResults::ImageHash,
            ])
            .values_panic([
                page_id_i32.into(),
                backend.to_string().into(),
                text.map(|s| s.to_string()).into(),
                confidence.into(),
                Option::<i32>::None.into(),
                char_count.into(),
                word_count.into(),
                processing_time_ms.into(),
                Option::<String>::None.into(),
                now.clone().into(),
                model.map(|s| s.to_string()).into(),
                image_hash.map(|s| s.to_string()).into(),
            ])
            .on_conflict(
                OnConflict::new()
                    .expr(Expr::col(PageOcrResults::PageId))
                    .expr(Expr::col(PageOcrResults::Backend))
                    .expr(Expr::cust("COALESCE(\"model\", '')"))
                    .update_columns([
                        PageOcrResults::Text,
                        PageOcrResults::Confidence,
                        PageOcrResults::CharCount,
                        PageOcrResults::WordCount,
                        PageOcrResults::ProcessingTimeMs,
                        PageOcrResults::CreatedAt,
                        PageOcrResults::ImageHash,
                    ])
                    .to_owned(),
            )
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Integer, _>(page_id_i32)
                .bind::<diesel::sql_types::Text, _>(backend)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(text)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Float>, _>(confidence)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(None::<i32>)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(char_count)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(word_count)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                    processing_time_ms,
                )
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(None::<&str>)
                .bind::<diesel::sql_types::Text, _>(&now)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(model)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(image_hash)
                .execute(&mut conn)
                .await?;

            diesel::update(document_pages::table.find(page_id_i32))
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
    #[allow(dead_code)]
    pub async fn store_page_ocr_error(
        &self,
        page_id: i64,
        backend: &str,
        model: Option<&str>,
        error_message: &str,
    ) -> Result<(), DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::PageOcrResults;
        use sea_query::{Expr, OnConflict, Query};

        let now = Utc::now().to_rfc3339();
        let page_id_i32 = page_id as i32;

        let stmt = Query::insert()
            .into_table(PageOcrResults::Table)
            .columns([
                PageOcrResults::PageId,
                PageOcrResults::Backend,
                PageOcrResults::Text,
                PageOcrResults::Confidence,
                PageOcrResults::QualityScore,
                PageOcrResults::CharCount,
                PageOcrResults::WordCount,
                PageOcrResults::ProcessingTimeMs,
                PageOcrResults::ErrorMessage,
                PageOcrResults::CreatedAt,
                PageOcrResults::Model,
                PageOcrResults::ImageHash,
            ])
            .values_panic([
                page_id_i32.into(),
                backend.to_string().into(),
                Option::<String>::None.into(),
                Option::<f32>::None.into(),
                Option::<i32>::None.into(),
                Option::<i32>::None.into(),
                Option::<i32>::None.into(),
                Option::<i32>::None.into(),
                error_message.to_string().into(),
                now.clone().into(),
                model.map(|s| s.to_string()).into(),
                Option::<String>::None.into(),
            ])
            .on_conflict(
                OnConflict::new()
                    .expr(Expr::col(PageOcrResults::PageId))
                    .expr(Expr::col(PageOcrResults::Backend))
                    .expr(Expr::cust("COALESCE(\"model\", '')"))
                    .update_columns([
                        PageOcrResults::Text,
                        PageOcrResults::ErrorMessage,
                        PageOcrResults::CreatedAt,
                    ])
                    .to_owned(),
            )
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Integer, _>(page_id_i32)
                .bind::<diesel::sql_types::Text, _>(backend)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(None::<&str>)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Float>, _>(None::<f32>)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(None::<i32>)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(None::<i32>)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(None::<i32>)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(None::<i32>)
                .bind::<diesel::sql_types::Text, _>(error_message)
                .bind::<diesel::sql_types::Text, _>(&now)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(model)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(None::<&str>)
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }

    /// Get all OCR results for a page from different backends.
    #[allow(dead_code)]
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

    /// Find an existing OCR result by image hash and backend.
    /// Used for deduplication - if we've already OCR'd this exact image, reuse the result.
    pub async fn find_ocr_result_by_image_hash(
        &self,
        image_hash: &str,
        backend: &str,
    ) -> Result<Option<PageOcrResultRecord>, DieselError> {
        with_conn!(self.pool, conn, {
            page_ocr_results::table
                .filter(page_ocr_results::image_hash.eq(image_hash))
                .filter(page_ocr_results::backend.eq(backend))
                .filter(page_ocr_results::text.is_not_null())
                .first(&mut conn)
                .await
                .optional()
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

        Ok(records.into_iter().map(DocumentPage::from).collect())
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
