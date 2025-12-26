//! Date estimation and management for documents.

use chrono::{DateTime, Utc};
use rusqlite::params;

use super::DocumentRepository;
use crate::repository::Result;

impl DocumentRepository {
    /// Update estimated date for a document.
    pub fn update_estimated_date(
        &self,
        document_id: &str,
        estimated_date: DateTime<Utc>,
        confidence: &str,
        source: &str,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE documents SET estimated_date = ?, date_confidence = ?, date_source = ?, updated_at = ? WHERE id = ?",
            params![
                estimated_date.to_rfc3339(),
                confidence,
                source,
                Utc::now().to_rfc3339(),
                document_id
            ],
        )?;
        Ok(())
    }

    /// Set manual date override for a document.
    pub fn set_manual_date(&self, document_id: &str, manual_date: DateTime<Utc>) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE documents SET manual_date = ?, updated_at = ? WHERE id = ?",
            params![
                manual_date.to_rfc3339(),
                Utc::now().to_rfc3339(),
                document_id
            ],
        )?;
        Ok(())
    }

    /// Get documents that need date estimation.
    /// Returns documents where estimated_date is NULL, manual_date is NULL,
    /// and no "date_detection" annotation exists.
    #[allow(clippy::type_complexity)]
    pub fn get_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<
        Vec<(
            String,
            Option<String>,
            Option<DateTime<Utc>>,
            DateTime<Utc>,
            Option<String>,
        )>,
    > {
        let conn = self.connect()?;

        let base_query = r#"
            SELECT d.id, dv.original_filename, dv.server_date, dv.acquired_at, d.source_url
            FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE d.estimated_date IS NULL
              AND d.manual_date IS NULL
              AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
              AND NOT EXISTS (
                  SELECT 1 FROM document_annotations da
                  WHERE da.document_id = d.id AND da.annotation_type = 'date_detection'
              )
        "#;

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match source_id {
            Some(sid) => (
                format!("{} AND d.source_id = ? LIMIT {}", base_query, limit),
                vec![Box::new(sid.to_string()) as Box<dyn rusqlite::ToSql>],
            ),
            None => (format!("{} LIMIT {}", base_query, limit), vec![]),
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let server_date: Option<DateTime<Utc>> = row
                    .get::<_, Option<String>>("server_date")?
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc));

                let acquired_at =
                    DateTime::parse_from_rfc3339(&row.get::<_, String>("acquired_at")?)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now());

                Ok((
                    row.get::<_, String>("id")?,
                    row.get::<_, Option<String>>("original_filename")?,
                    server_date,
                    acquired_at,
                    row.get::<_, Option<String>>("source_url")?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    /// Count documents needing date estimation.
    pub fn count_documents_needing_date_estimation(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        let base_query = r#"
            SELECT COUNT(*) FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE d.estimated_date IS NULL
              AND d.manual_date IS NULL
              AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
              AND NOT EXISTS (
                  SELECT 1 FROM document_annotations da
                  WHERE da.document_id = d.id AND da.annotation_type = 'date_detection'
              )
        "#;

        let count: i64 = match source_id {
            Some(sid) => conn.query_row(
                &format!("{} AND d.source_id = ?", base_query),
                params![sid],
                |row| row.get(0),
            )?,
            None => conn.query_row(base_query, [], |row| row.get(0))?,
        };

        Ok(count as u64)
    }
}
