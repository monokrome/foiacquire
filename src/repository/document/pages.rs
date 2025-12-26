//! Document page and OCR operations.

use chrono::{DateTime, Utc};
use rusqlite::{params, Row};
use std::collections::HashMap;

use super::DocumentRepository;
use crate::models::{DocumentPage, DocumentStatus, PageOcrStatus};
use crate::repository::Result;

impl DocumentRepository {
    /// Save a document page.
    pub fn save_page(&self, page: &DocumentPage) -> Result<i64> {
        let document_id = page.document_id.clone();
        let version_id = page.version_id;
        let page_number = page.page_number;
        let pdf_text = page.pdf_text.clone();
        let ocr_text = page.ocr_text.clone();
        let final_text = page.final_text.clone();
        let ocr_status = page.ocr_status.as_str().to_string();

        crate::repository::with_retry(|| {
            let conn = self.connect()?;
            let now = Utc::now().to_rfc3339();

            conn.execute(
                r#"INSERT INTO document_pages
                   (document_id, version_id, page_number, pdf_text, ocr_text, final_text, ocr_status, created_at, updated_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
                   ON CONFLICT(document_id, version_id, page_number) DO UPDATE SET
                       pdf_text = COALESCE(?4, pdf_text),
                       ocr_text = COALESCE(?5, ocr_text),
                       final_text = COALESCE(?6, final_text),
                       ocr_status = ?7,
                       updated_at = ?8"#,
                params![
                    document_id,
                    version_id,
                    page_number,
                    pdf_text,
                    ocr_text,
                    final_text,
                    ocr_status,
                    now,
                ],
            )?;

            Ok(conn.last_insert_rowid())
        })
    }

    /// Get all pages for a document version.
    pub fn get_pages(&self, document_id: &str, version_id: i64) -> Result<Vec<DocumentPage>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT * FROM document_pages WHERE document_id = ? AND version_id = ? ORDER BY page_number",
        )?;

        let pages = stmt
            .query_map(params![document_id, version_id], |row| {
                self.row_to_document_page(row)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(pages)
    }

    /// Get a specific page.
    pub fn get_page(
        &self,
        document_id: &str,
        version_id: i64,
        page_number: u32,
    ) -> Result<Option<DocumentPage>> {
        use super::helpers::OptionalExt;
        let conn = self.connect()?;

        let page = conn
            .query_row(
                "SELECT * FROM document_pages WHERE document_id = ? AND version_id = ? AND page_number = ?",
                params![document_id, version_id, page_number],
                |row| self.row_to_document_page(row),
            )
            .optional()?;

        Ok(page)
    }

    /// Get pages needing OCR (status = 'text_extracted').
    /// Pages with little/no PDF text are prioritized.
    pub fn get_pages_needing_ocr(&self, limit: usize) -> Result<Vec<DocumentPage>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(&format!(
            "SELECT * FROM document_pages
             WHERE ocr_status = 'text_extracted'
             ORDER BY
                 CASE
                     WHEN pdf_text IS NULL OR pdf_text = '' THEN 0
                     WHEN LENGTH(pdf_text) < 100 THEN 1
                     ELSE 2
                 END,
                 created_at ASC
             LIMIT {}",
            limit.max(1)
        ))?;

        let pages = stmt
            .query_map([], |row| self.row_to_document_page(row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(pages)
    }

    /// Count pages needing OCR.
    pub fn count_pages_needing_ocr(&self) -> Result<u64> {
        let conn = self.connect()?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM document_pages WHERE ocr_status = 'text_extracted'",
            [],
            |row| row.get(0),
        )?;

        Ok(count as u64)
    }

    /// Count pages for a document version.
    pub fn count_pages(&self, document_id: &str, version_id: i64) -> Result<u32> {
        let conn = self.connect()?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
            |row| row.get(0),
        )?;

        Ok(count as u32)
    }

    /// Update the cached page count for a document version.
    pub fn set_version_page_count(&self, version_id: i64, page_count: u32) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "UPDATE document_versions SET page_count = ? WHERE id = ?",
            params![page_count as i64, version_id],
        )?;

        Ok(())
    }

    /// Get the cached page count for a version, or count from pages table if not cached.
    pub fn get_version_page_count(
        &self,
        document_id: &str,
        version_id: i64,
    ) -> Result<Option<u32>> {
        let conn = self.connect()?;

        let cached: Option<i64> = conn
            .query_row(
                "SELECT page_count FROM document_versions WHERE id = ?",
                params![version_id],
                |row| row.get(0),
            )
            .ok()
            .flatten();

        if let Some(count) = cached {
            return Ok(Some(count as u32));
        }

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
            |row| row.get(0),
        )?;

        if count > 0 {
            Ok(Some(count as u32))
        } else {
            Ok(None)
        }
    }

    /// Delete all pages for a document version.
    pub fn delete_pages(&self, document_id: &str, version_id: i64) -> Result<u64> {
        let conn = self.connect()?;

        let deleted = conn.execute(
            "DELETE FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
        )?;

        Ok(deleted as u64)
    }

    /// Check if all pages for a document version have completed OCR.
    pub fn are_all_pages_ocr_complete(&self, document_id: &str, version_id: i64) -> Result<bool> {
        let conn = self.connect()?;

        let (total, complete): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), SUM(CASE WHEN ocr_status = 'ocr_complete' THEN 1 ELSE 0 END)
             FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
            |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
        )?;

        Ok(total > 0 && total == complete)
    }

    /// Check if all pages for a document version are done processing.
    pub fn are_all_pages_complete(&self, document_id: &str, version_id: i64) -> Result<bool> {
        let conn = self.connect()?;

        let (total, done): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), SUM(CASE WHEN ocr_status IN ('ocr_complete', 'failed', 'skipped') THEN 1 ELSE 0 END)
             FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
            |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
        )?;

        Ok(total > 0 && total == done)
    }

    /// Finalize a document by combining page text and setting status to OcrComplete.
    pub fn finalize_document(&self, document_id: &str) -> Result<bool> {
        let doc = match self.get(document_id)? {
            Some(d) => d,
            None => return Ok(false),
        };

        let version = match doc.current_version() {
            Some(v) => v,
            None => return Ok(false),
        };

        let combined_text = match self.get_combined_page_text(document_id, version.id)? {
            Some(t) if !t.is_empty() => t,
            _ => return Ok(false),
        };

        let mut updated_doc = doc.clone();
        updated_doc.extracted_text = Some(combined_text.clone());
        updated_doc.status = DocumentStatus::OcrComplete;
        updated_doc.updated_at = chrono::Utc::now();
        self.save(&updated_doc)?;

        let text_path = version.file_path.with_extension(format!(
            "{}.txt",
            version
                .file_path
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
        ));
        let _ = std::fs::write(&text_path, &combined_text);

        Ok(true)
    }

    /// Find and finalize all documents that have all pages OCR complete.
    pub fn finalize_pending_documents(&self, source_id: Option<&str>) -> Result<usize> {
        let conn = self.connect()?;

        let sql = match source_id {
            Some(_) => {
                "SELECT DISTINCT d.id FROM documents d
                 JOIN document_versions dv ON dv.document_id = d.id
                 JOIN document_pages dp ON dp.document_id = d.id AND dp.version_id = dv.id
                 WHERE d.status != 'ocr_complete'
                   AND d.source_id = ?
                 GROUP BY d.id, dp.version_id
                 HAVING COUNT(*) = SUM(CASE WHEN dp.ocr_status = 'ocr_complete' THEN 1 ELSE 0 END)
                   AND COUNT(*) > 0"
            }
            None => {
                "SELECT DISTINCT d.id FROM documents d
                 JOIN document_versions dv ON dv.document_id = d.id
                 JOIN document_pages dp ON dp.document_id = d.id AND dp.version_id = dv.id
                 WHERE d.status != 'ocr_complete'
                 GROUP BY d.id, dp.version_id
                 HAVING COUNT(*) = SUM(CASE WHEN dp.ocr_status = 'ocr_complete' THEN 1 ELSE 0 END)
                   AND COUNT(*) > 0"
            }
        };

        let doc_ids: Vec<String> = match source_id {
            Some(sid) => {
                let mut stmt = conn.prepare(sql)?;
                let ids: Vec<String> = stmt
                    .query_map(params![sid], |row| row.get(0))?
                    .filter_map(|r| r.ok())
                    .collect();
                ids
            }
            None => {
                let mut stmt = conn.prepare(sql)?;
                let ids: Vec<String> = stmt
                    .query_map([], |row| row.get(0))?
                    .filter_map(|r| r.ok())
                    .collect();
                ids
            }
        };

        drop(conn);

        let mut finalized = 0;
        for doc_id in doc_ids {
            if self.finalize_document(&doc_id)? {
                finalized += 1;
            }
        }

        Ok(finalized)
    }

    /// Get combined final text for all pages of a document.
    pub fn get_combined_page_text(
        &self,
        document_id: &str,
        version_id: i64,
    ) -> Result<Option<String>> {
        let pages = self.get_pages(document_id, version_id)?;

        if pages.is_empty() {
            return Ok(None);
        }

        let combined: String = pages
            .into_iter()
            .filter_map(|p| p.final_text)
            .collect::<Vec<_>>()
            .join("\n\n");

        if combined.is_empty() {
            Ok(None)
        } else {
            Ok(Some(combined))
        }
    }

    // ========== OCR Results ==========

    /// Store an alternative OCR result for a page.
    pub fn store_page_ocr_result(
        &self,
        page_id: i64,
        backend: &str,
        ocr_text: Option<&str>,
        confidence: Option<f64>,
        processing_time_ms: Option<u64>,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO page_ocr_results (page_id, backend, ocr_text, confidence, processing_time_ms, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(page_id, backend) DO UPDATE SET
                ocr_text = excluded.ocr_text,
                confidence = excluded.confidence,
                processing_time_ms = excluded.processing_time_ms,
                created_at = excluded.created_at
            "#,
            params![
                page_id,
                backend,
                ocr_text,
                confidence,
                processing_time_ms.map(|t| t as i64),
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    /// Get all OCR results for a page.
    #[allow(clippy::type_complexity)]
    pub fn get_page_ocr_results(
        &self,
        page_id: i64,
    ) -> Result<Vec<(String, Option<String>, Option<f64>, Option<i64>)>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT backend, ocr_text, confidence, processing_time_ms
            FROM page_ocr_results
            WHERE page_id = ?
            ORDER BY created_at DESC
            "#,
        )?;

        let results = stmt
            .query_map(params![page_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get OCR results for multiple pages in a single query.
    #[allow(clippy::type_complexity)]
    pub fn get_pages_ocr_results_bulk(
        &self,
        page_ids: &[i64],
    ) -> Result<HashMap<i64, Vec<(String, Option<String>, Option<f64>, Option<i64>)>>> {
        if page_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.connect()?;

        let placeholders: String = page_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            r#"
            SELECT page_id, backend, ocr_text, confidence, processing_time_ms
            FROM page_ocr_results
            WHERE page_id IN ({})
            ORDER BY page_id, created_at DESC
            "#,
            placeholders
        );

        let mut stmt = conn.prepare(&query)?;

        let params: Vec<&dyn rusqlite::ToSql> = page_ids
            .iter()
            .map(|id| id as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<f64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })?;

        let mut results: HashMap<i64, Vec<(String, Option<String>, Option<f64>, Option<i64>)>> =
            HashMap::new();

        for row in rows {
            let (page_id, backend, ocr_text, confidence, processing_time_ms) = row?;
            results.entry(page_id).or_default().push((
                backend,
                ocr_text,
                confidence,
                processing_time_ms,
            ));
        }

        Ok(results)
    }

    /// Check if a page has OCR result from a specific backend.
    pub fn has_page_ocr_result(&self, page_id: i64, backend: &str) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM page_ocr_results WHERE page_id = ? AND backend = ?",
            params![page_id, backend],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get page IDs for a document that don't have OCR from a specific backend.
    pub fn get_pages_without_backend(
        &self,
        document_id: &str,
        backend: &str,
    ) -> Result<Vec<(i64, i32)>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT dp.id, dp.page_number
            FROM document_pages dp
            WHERE dp.document_id = ?
              AND NOT EXISTS (
                  SELECT 1 FROM page_ocr_results por
                  WHERE por.page_id = dp.id AND por.backend = ?
              )
            ORDER BY dp.page_number
            "#,
        )?;

        let results = stmt
            .query_map(params![document_id, backend], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i32>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(results)
    }

    // ========== Internal helpers ==========

    pub(crate) fn row_to_document_page(&self, row: &Row) -> rusqlite::Result<DocumentPage> {
        Ok(DocumentPage {
            id: row.get("id")?,
            document_id: row.get("document_id")?,
            version_id: row.get("version_id")?,
            page_number: row.get::<_, u32>("page_number")?,
            pdf_text: row.get("pdf_text")?,
            ocr_text: row.get("ocr_text")?,
            final_text: row.get("final_text")?,
            ocr_status: PageOcrStatus::from_str(&row.get::<_, String>("ocr_status")?)
                .unwrap_or(PageOcrStatus::Pending),
            created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("created_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("updated_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
        })
    }
}
