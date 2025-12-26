//! Document counting and statistics operations.

use rusqlite::params;
use std::collections::HashMap;

use super::DocumentRepository;
use crate::repository::Result;

impl DocumentRepository {
    /// MIME types supported by the OCR extractor.
    pub(crate) const OCR_SUPPORTED_MIME_TYPES: &'static [&'static str] = &[
        "application/pdf",
        "image/png",
        "image/jpeg",
        "image/tiff",
        "image/gif",
        "image/bmp",
        "text/plain",
        "text/html",
    ];

    /// Count total documents in O(1) time.
    /// Uses the trigger-maintained document_counts table.
    pub fn count(&self) -> Result<u64> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COALESCE(SUM(count), 0) FROM document_counts",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Count documents for a specific source in O(1) time.
    pub fn count_by_source(&self, source_id: &str) -> Result<u64> {
        let conn = self.connect()?;
        let count: i64 = conn
            .query_row(
                "SELECT COALESCE(count, 0) FROM document_counts WHERE source_id = ?",
                params![source_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(count as u64)
    }

    /// Get document counts for all sources in O(1) time.
    pub fn get_all_source_counts(&self) -> Result<HashMap<String, u64>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT source_id, count FROM document_counts")?;

        let mut counts = HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;

        for row in rows {
            let (source_id, count) = row?;
            counts.insert(source_id, count);
        }

        Ok(counts)
    }

    /// Get document counts grouped by status.
    pub fn count_all_by_status(&self) -> Result<HashMap<String, u64>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT status, COUNT(*) FROM documents GROUP BY status")?;

        let mut counts = HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;

        for row in rows {
            let (status, count) = row?;
            counts.insert(status, count);
        }

        Ok(counts)
    }

    /// Count documents needing OCR.
    pub fn count_needing_ocr(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        let placeholders = Self::OCR_SUPPORTED_MIME_TYPES
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        let count: i64 = match source_id {
            Some(sid) => {
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = Self::OCR_SUPPORTED_MIME_TYPES
                    .iter()
                    .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::ToSql>)
                    .collect();
                params.push(Box::new(sid.to_string()));
                let params_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                conn.query_row(
                    &format!(
                        "SELECT COUNT(DISTINCT d.id) FROM documents d
                         JOIN document_versions dv ON dv.document_id = d.id
                         WHERE d.status = 'downloaded'
                           AND dv.mime_type IN ({})
                           AND d.source_id = ?",
                        placeholders
                    ),
                    params_refs.as_slice(),
                    |row| row.get(0),
                )?
            }
            None => {
                let params: Vec<Box<dyn rusqlite::ToSql>> = Self::OCR_SUPPORTED_MIME_TYPES
                    .iter()
                    .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::ToSql>)
                    .collect();
                let params_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                conn.query_row(
                    &format!(
                        "SELECT COUNT(DISTINCT d.id) FROM documents d
                         JOIN document_versions dv ON dv.document_id = d.id
                         WHERE d.status = 'downloaded'
                           AND dv.mime_type IN ({})",
                        placeholders
                    ),
                    params_refs.as_slice(),
                    |row| row.get(0),
                )?
            }
        };

        Ok(count as u64)
    }

    /// Count documents needing summarization.
    pub fn count_needing_summarization(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        let count: i64 = match source_id {
            Some(sid) => conn.query_row(
                "SELECT COUNT(DISTINCT d.id) FROM documents d
                 JOIN document_pages dp ON dp.document_id = d.id
                 WHERE d.synopsis IS NULL
                   AND d.source_id = ?
                   AND dp.final_text IS NOT NULL AND LENGTH(dp.final_text) > 0",
                params![sid],
                |row| row.get(0),
            )?,
            None => conn.query_row(
                "SELECT COUNT(DISTINCT d.id) FROM documents d
                 JOIN document_pages dp ON dp.document_id = d.id
                 WHERE d.synopsis IS NULL
                   AND dp.final_text IS NOT NULL AND LENGTH(dp.final_text) > 0",
                [],
                |row| row.get(0),
            )?,
        };

        Ok(count as u64)
    }

    /// Get all unique tags across all documents.
    pub fn get_all_tags(&self) -> Result<Vec<(String, usize)>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            r#"
            SELECT LOWER(json_each.value) as tag, COUNT(*) as cnt
            FROM documents, json_each(tags)
            WHERE tags IS NOT NULL AND tags != '[]'
            GROUP BY LOWER(json_each.value)
            ORDER BY cnt DESC
            "#,
        )?;

        let tags = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(tags)
    }

    /// Get category statistics from file_categories table.
    /// O(1) lookup using pre-computed counts maintained by triggers.
    pub fn get_category_stats(&self, source_id: Option<&str>) -> Result<Vec<(String, u64)>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            let mut stmt = conn.prepare(
                r#"
                SELECT category_id, COUNT(*) as count
                FROM documents
                WHERE source_id = ? AND category_id IS NOT NULL
                GROUP BY category_id
                ORDER BY count DESC
            "#,
            )?;
            let stats = stmt
                .query_map(params![sid], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(stats)
        } else {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, doc_count
                FROM file_categories
                WHERE doc_count > 0
                ORDER BY doc_count DESC
            "#,
            )?;
            let stats = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(stats)
        }
    }

    /// Get document type statistics (raw MIME types).
    pub fn get_type_stats(&self, source_id: Option<&str>) -> Result<Vec<(String, u64)>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            let mut stmt = conn.prepare(
                r#"
                SELECT dv.mime_type, COUNT(DISTINCT dv.document_id) as count
                FROM document_versions dv
                JOIN documents d ON dv.document_id = d.id
                WHERE d.source_id = ?
                GROUP BY dv.mime_type
                ORDER BY count DESC
            "#,
            )?;
            let stats = stmt
                .query_map(params![sid], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(stats)
        } else {
            let mut stmt = conn.prepare(
                r#"
                SELECT mime_type, COUNT(DISTINCT document_id) as count
                FROM document_versions
                GROUP BY mime_type
                ORDER BY count DESC
            "#,
            )?;
            let stats = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(stats)
        }
    }
}
