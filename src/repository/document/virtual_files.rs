//! Virtual file operations for archives and email attachments.

use chrono::{DateTime, Utc};
use rusqlite::{params, Row};

use super::helpers::OptionalExt;
use super::DocumentRepository;
use crate::models::{Document, VirtualFile, VirtualFileStatus};
use crate::repository::Result;

impl DocumentRepository {
    /// Insert a new virtual file.
    pub fn insert_virtual_file(&self, vf: &VirtualFile) -> Result<()> {
        let conn = self.connect()?;
        let tags_json = serde_json::to_string(&vf.tags).unwrap_or_else(|_| "[]".to_string());

        conn.execute(
            "INSERT INTO virtual_files (id, document_id, version_id, archive_path, filename, mime_type, file_size, extracted_text, synopsis, tags, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                vf.id,
                vf.document_id,
                vf.version_id,
                vf.archive_path,
                vf.filename,
                vf.mime_type,
                vf.file_size as i64,
                vf.extracted_text,
                vf.synopsis,
                tags_json,
                vf.status.as_str(),
                vf.created_at.to_rfc3339(),
                vf.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    /// Get all virtual files for a document.
    pub fn get_virtual_files(&self, document_id: &str) -> Result<Vec<VirtualFile>> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare("SELECT * FROM virtual_files WHERE document_id = ? ORDER BY archive_path")?;

        let files = stmt
            .query_map(params![document_id], |row| self.row_to_virtual_file(row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(files)
    }

    /// Get virtual files by document version.
    pub fn get_virtual_files_by_version(&self, version_id: i64) -> Result<Vec<VirtualFile>> {
        let conn = self.connect()?;
        let mut stmt =
            conn.prepare("SELECT * FROM virtual_files WHERE version_id = ? ORDER BY archive_path")?;

        let files = stmt
            .query_map(params![version_id], |row| self.row_to_virtual_file(row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(files)
    }

    /// Get virtual files needing OCR processing.
    pub fn get_virtual_files_needing_ocr(&self, limit: usize) -> Result<Vec<VirtualFile>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(&format!(
            "SELECT * FROM virtual_files WHERE status = 'pending' LIMIT {}",
            limit.max(1)
        ))?;

        let files = stmt
            .query_map([], |row| self.row_to_virtual_file(row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(files)
    }

    /// Count virtual files needing OCR.
    pub fn count_virtual_files_needing_ocr(&self) -> Result<u64> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM virtual_files WHERE status = 'pending'",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Update virtual file extracted text and status.
    pub fn update_virtual_file_text(
        &self,
        id: &str,
        text: &str,
        status: VirtualFileStatus,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE virtual_files SET extracted_text = ?, status = ?, updated_at = ? WHERE id = ?",
            params![text, status.as_str(), Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }

    /// Update virtual file synopsis and tags.
    pub fn update_virtual_file_summary(
        &self,
        id: &str,
        synopsis: &str,
        tags: &[String],
    ) -> Result<()> {
        let conn = self.connect()?;
        let tags_json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".to_string());
        conn.execute(
            "UPDATE virtual_files SET synopsis = ?, tags = ?, updated_at = ? WHERE id = ?",
            params![synopsis, tags_json, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }

    /// Check if virtual files exist for a document version.
    pub fn virtual_files_exist(&self, version_id: i64) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM virtual_files WHERE version_id = ?",
            params![version_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get the version ID for a document's current version.
    pub fn get_current_version_id(&self, document_id: &str) -> Result<Option<i64>> {
        let conn = self.connect()?;
        let id = conn
            .query_row(
                "SELECT id FROM document_versions WHERE document_id = ? ORDER BY acquired_at DESC LIMIT 1",
                params![document_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(id)
    }

    /// Get archive documents that haven't been processed for virtual files yet.
    pub fn get_unprocessed_archives(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let base_query = r#"
            SELECT d.* FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE (dv.mime_type = 'application/zip' OR dv.mime_type = 'application/x-zip-compressed')
            AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
            AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
        "#;

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match source_id {
            Some(sid) => (
                format!(
                    "{} AND d.source_id = ? ORDER BY d.updated_at DESC LIMIT {}",
                    base_query,
                    limit.max(1)
                ),
                vec![Box::new(sid.to_string()) as Box<dyn rusqlite::ToSql>],
            ),
            None => (
                format!(
                    "{} ORDER BY d.updated_at DESC LIMIT {}",
                    base_query,
                    limit.max(1)
                ),
                vec![],
            ),
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let docs = stmt
            .query_map(params_refs.as_slice(), |row| {
                self.row_to_document(&conn, row)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(docs)
    }

    /// Count archive documents that haven't been processed.
    pub fn count_unprocessed_archives(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        let base_query = r#"
            SELECT COUNT(*) FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE (dv.mime_type = 'application/zip' OR dv.mime_type = 'application/x-zip-compressed')
            AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
            AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
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

    /// Get emails that haven't been processed for attachments.
    pub fn get_unprocessed_emails(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let base_query = r#"
            SELECT d.* FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE dv.mime_type = 'message/rfc822'
            AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
            AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
        "#;

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match source_id {
            Some(sid) => (
                format!(
                    "{} AND d.source_id = ? ORDER BY d.updated_at DESC LIMIT {}",
                    base_query,
                    limit.max(1)
                ),
                vec![Box::new(sid.to_string()) as Box<dyn rusqlite::ToSql>],
            ),
            None => (
                format!(
                    "{} ORDER BY d.updated_at DESC LIMIT {}",
                    base_query,
                    limit.max(1)
                ),
                vec![],
            ),
        };

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn.prepare(&sql)?;
        let docs = stmt
            .query_map(params_refs.as_slice(), |row| {
                self.row_to_document(&conn, row)
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(docs)
    }

    /// Count emails that haven't been processed for attachments.
    pub fn count_unprocessed_emails(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        let base_query = r#"
            SELECT COUNT(*) FROM documents d
            JOIN document_versions dv ON d.id = dv.document_id
            WHERE dv.mime_type = 'message/rfc822'
            AND dv.id = (SELECT MAX(dv2.id) FROM document_versions dv2 WHERE dv2.document_id = d.id)
            AND NOT EXISTS (SELECT 1 FROM virtual_files vf WHERE vf.version_id = dv.id)
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

    // ========== Internal helpers ==========

    pub(crate) fn row_to_virtual_file(&self, row: &Row) -> rusqlite::Result<VirtualFile> {
        let tags_str: Option<String> = row.get("tags")?;
        let tags: Vec<String> = tags_str
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        Ok(VirtualFile {
            id: row.get("id")?,
            document_id: row.get("document_id")?,
            version_id: row.get("version_id")?,
            archive_path: row.get("archive_path")?,
            filename: row.get("filename")?,
            mime_type: row.get("mime_type")?,
            file_size: row.get::<_, i64>("file_size")? as u64,
            extracted_text: row.get("extracted_text")?,
            synopsis: row.get("synopsis")?,
            tags,
            status: VirtualFileStatus::from_str(&row.get::<_, String>("status")?)
                .unwrap_or(VirtualFileStatus::Pending),
            created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("created_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("updated_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
        })
    }
}
