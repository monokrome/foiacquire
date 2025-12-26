//! Document annotation tracking.

use chrono::Utc;
use rusqlite::params;

use super::DocumentRepository;
use crate::repository::Result;

impl DocumentRepository {
    /// Record that an annotation was completed for a document.
    pub fn record_annotation(
        &self,
        document_id: &str,
        annotation_type: &str,
        version: i32,
        result: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO document_annotations (document_id, annotation_type, completed_at, version, result, error)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(document_id, annotation_type) DO UPDATE SET
                completed_at = excluded.completed_at,
                version = excluded.version,
                result = excluded.result,
                error = excluded.error
            "#,
            params![
                document_id,
                annotation_type,
                Utc::now().to_rfc3339(),
                version,
                result,
                error
            ],
        )?;
        Ok(())
    }

    /// Check if a specific annotation type has been completed for a document.
    pub fn has_annotation(&self, document_id: &str, annotation_type: &str) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM document_annotations WHERE document_id = ? AND annotation_type = ?",
            params![document_id, annotation_type],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get documents missing a specific annotation type.
    pub fn get_documents_missing_annotation(
        &self,
        annotation_type: &str,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let conn = self.connect()?;

        let base_query = r#"
            SELECT d.id FROM documents d
            WHERE NOT EXISTS (
                SELECT 1 FROM document_annotations da
                WHERE da.document_id = d.id AND da.annotation_type = ?
            )
        "#;

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match source_id {
            Some(sid) => (
                format!("{} AND d.source_id = ? LIMIT {}", base_query, limit),
                vec![
                    Box::new(annotation_type.to_string()) as Box<dyn rusqlite::ToSql>,
                    Box::new(sid.to_string()),
                ],
            ),
            None => (
                format!("{} LIMIT {}", base_query, limit),
                vec![Box::new(annotation_type.to_string()) as Box<dyn rusqlite::ToSql>],
            ),
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let ids = stmt
            .query_map(params_refs.as_slice(), |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(ids)
    }
}
