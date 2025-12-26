//! Basic CRUD operations for documents.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Row};
use std::collections::HashMap;
use std::path::PathBuf;

use super::helpers::{
    row_to_document_partial, row_to_document_with_versions, row_to_version, DocumentPartial,
    DocumentSummary, OptionalExt, VersionSummary,
};
use super::DocumentRepository;
use crate::models::{Document, DocumentStatus, DocumentVersion};
use crate::repository::Result;

impl DocumentRepository {
    /// Get a document by ID.
    pub fn get(&self, id: &str) -> Result<Option<Document>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT * FROM documents WHERE id = ?")?;

        let doc = stmt
            .query_row(params![id], |row| self.row_to_document(&conn, row))
            .optional()?;

        Ok(doc)
    }

    /// Get a document by source URL.
    pub fn get_by_url(&self, url: &str) -> Result<Option<Document>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT * FROM documents WHERE source_url = ?")?;

        let doc = stmt
            .query_row(params![url], |row| self.row_to_document(&conn, row))
            .optional()?;

        Ok(doc)
    }

    /// Get just the source URLs for a source (lightweight, for URL analysis).
    pub fn get_urls_by_source(&self, source_id: &str) -> Result<Vec<String>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT source_url FROM documents WHERE source_id = ?")?;
        let urls = stmt
            .query_map(params![source_id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(urls)
    }

    /// Get all source URLs as a HashSet for fast duplicate detection during import.
    pub fn get_all_urls_set(&self) -> Result<std::collections::HashSet<String>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT source_url FROM documents")?;
        let urls = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(urls)
    }

    /// Get all content hashes as a HashSet for fast content deduplication during import.
    pub fn get_all_content_hashes(&self) -> Result<std::collections::HashSet<String>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT DISTINCT content_hash FROM document_versions")?;
        let hashes = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(hashes)
    }

    /// Get all documents from a source.
    pub fn get_by_source(&self, source_id: &str) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare("SELECT * FROM documents WHERE source_id = ?")?;
        let rows: Vec<_> = stmt
            .query_map(params![source_id], |row| {
                let id: String = row.get("id")?;
                Ok((id, row_to_document_partial(row)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let versions_map = self.load_versions_bulk(&conn, &doc_ids)?;

        let docs = rows
            .into_iter()
            .map(|(id, partial)| {
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get documents by status.
    pub fn get_by_status(&self, status: DocumentStatus) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare("SELECT * FROM documents WHERE status = ?")?;
        let rows: Vec<_> = stmt
            .query_map(params![status.as_str()], |row| {
                let id: String = row.get("id")?;
                Ok((id, row_to_document_partial(row)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let versions_map = self.load_versions_bulk(&conn, &doc_ids)?;

        let docs = rows
            .into_iter()
            .map(|(id, partial)| {
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get all documents.
    pub fn get_all(&self) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare("SELECT * FROM documents")?;
        let rows: Vec<_> = stmt
            .query_map([], |row| {
                let id: String = row.get("id")?;
                Ok((id, row_to_document_partial(row)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let doc_ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let versions_map = self.load_versions_bulk(&conn, &doc_ids)?;

        let docs = rows
            .into_iter()
            .map(|(id, partial)| {
                let versions = versions_map.get(&id).cloned().unwrap_or_default();
                partial.with_versions(versions)
            })
            .collect();

        Ok(docs)
    }

    /// Get all document summaries (lightweight, excludes extracted_text).
    pub fn get_all_summaries(&self) -> Result<Vec<DocumentSummary>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT id, source_id, title, source_url, synopsis, tags, status, created_at, updated_at FROM documents"
        )?;

        let summaries = stmt
            .query_map([], |row| self.row_to_summary(&conn, row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(summaries)
    }

    /// Get document summaries by source (lightweight).
    pub fn get_summaries_by_source(&self, source_id: &str) -> Result<Vec<DocumentSummary>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT id, source_id, title, source_url, synopsis, tags, status, created_at, updated_at FROM documents WHERE source_id = ?"
        )?;

        let summaries = stmt
            .query_map(params![source_id], |row| self.row_to_summary(&conn, row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(summaries)
    }

    /// Get just content hashes for all documents (for duplicate detection).
    /// Returns (document_id, source_id, content_hash, title).
    pub fn get_content_hashes(&self) -> Result<Vec<(String, String, String, String)>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"SELECT d.id, d.source_id, dv.content_hash, d.title
               FROM documents d
               JOIN document_versions dv ON d.id = dv.document_id
               WHERE dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = d.id)"#,
        )?;

        let hashes = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(hashes)
    }

    /// Find sources that have a document with the given content hash.
    /// Returns list of (source_id, document_id, title) for matching documents.
    pub fn find_sources_by_hash(
        &self,
        content_hash: &str,
        exclude_source: Option<&str>,
    ) -> Result<Vec<(String, String, String)>> {
        let conn = self.connect()?;

        let (sql, params_vec): (&str, Vec<Box<dyn rusqlite::ToSql>>) = match exclude_source {
            Some(exclude) => (
                r#"SELECT DISTINCT d.source_id, d.id, d.title
                   FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE dv.content_hash = ? AND d.source_id != ?"#,
                vec![
                    Box::new(content_hash.to_string()),
                    Box::new(exclude.to_string()),
                ],
            ),
            None => (
                r#"SELECT DISTINCT d.source_id, d.id, d.title
                   FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE dv.content_hash = ?"#,
                vec![Box::new(content_hash.to_string())],
            ),
        };

        let mut stmt = conn.prepare(sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let results = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Save a document.
    pub fn save(&self, doc: &Document) -> Result<()> {
        let doc = doc.clone();

        crate::repository::with_retry(|| {
            let conn = self.connect()?;

            let tags_json = serde_json::to_string(&doc.tags)?;

            conn.execute(
                r#"
                INSERT INTO documents (id, source_id, title, source_url, extracted_text, synopsis, tags, status, metadata, created_at, updated_at, discovery_method)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    source_url = excluded.source_url,
                    extracted_text = excluded.extracted_text,
                    synopsis = excluded.synopsis,
                    tags = excluded.tags,
                    status = excluded.status,
                    metadata = excluded.metadata,
                    updated_at = excluded.updated_at
                "#,
                params![
                    doc.id,
                    doc.source_id,
                    doc.title,
                    doc.source_url,
                    doc.extracted_text,
                    doc.synopsis,
                    tags_json,
                    doc.status.as_str(),
                    serde_json::to_string(&doc.metadata)?,
                    doc.created_at.to_rfc3339(),
                    doc.updated_at.to_rfc3339(),
                    doc.discovery_method,
                ],
            )?;

            let existing_hashes: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT content_hash FROM document_versions WHERE document_id = ?")?;
                let rows = stmt.query_map(params![doc.id], |row| row.get(0))?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };

            for version in &doc.versions {
                if !existing_hashes.contains(&version.content_hash) {
                    conn.execute(
                        r#"
                        INSERT INTO document_versions
                            (document_id, content_hash, file_path, file_size, mime_type, acquired_at, source_url, original_filename, server_date, page_count)
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                        "#,
                        params![
                            doc.id,
                            version.content_hash,
                            version.file_path.to_string_lossy(),
                            version.file_size as i64,
                            version.mime_type,
                            version.acquired_at.to_rfc3339(),
                            version.source_url,
                            version.original_filename,
                            version.server_date.map(|d| d.to_rfc3339()),
                            version.page_count.map(|c| c as i64),
                        ],
                    )?;
                }
            }

            Ok(())
        })
    }

    /// Delete a document.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM document_versions WHERE document_id = ?",
            params![id],
        )?;
        let rows = conn.execute("DELETE FROM documents WHERE id = ?", params![id])?;
        Ok(rows > 0)
    }

    /// Check if a document exists.
    pub fn exists(&self, id: &str) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM documents WHERE id = ?",
            params![id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Check if content hash exists.
    pub fn content_exists(&self, content_hash: &str) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM document_versions WHERE content_hash = ?",
            params![content_hash],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    // ========== Internal helper methods ==========

    pub(crate) fn load_versions(
        &self,
        conn: &Connection,
        document_id: &str,
    ) -> rusqlite::Result<Vec<DocumentVersion>> {
        let mut stmt = conn.prepare(
            "SELECT * FROM document_versions WHERE document_id = ? ORDER BY acquired_at DESC",
        )?;

        let versions = stmt
            .query_map(params![document_id], row_to_version)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(versions)
    }

    /// Load versions for multiple documents in batched queries.
    pub(crate) fn load_versions_bulk(
        &self,
        conn: &Connection,
        document_ids: &[String],
    ) -> rusqlite::Result<HashMap<String, Vec<DocumentVersion>>> {
        if document_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut versions_map: HashMap<String, Vec<DocumentVersion>> = HashMap::new();

        const BATCH_SIZE: usize = 500;

        for chunk in document_ids.chunks(BATCH_SIZE) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "SELECT * FROM document_versions WHERE document_id IN ({}) ORDER BY document_id, acquired_at DESC",
                placeholders
            );

            let mut stmt = conn.prepare(&sql)?;
            let params: Vec<&dyn rusqlite::ToSql> =
                chunk.iter().map(|id| id as &dyn rusqlite::ToSql).collect();

            let versions = stmt.query_map(params.as_slice(), |row| {
                let doc_id: String = row.get("document_id")?;
                let version = row_to_version(row)?;
                Ok((doc_id, version))
            })?;

            for result in versions {
                let (doc_id, version) = result?;
                versions_map.entry(doc_id).or_default().push(version);
            }
        }

        Ok(versions_map)
    }

    pub(crate) fn row_to_document(
        &self,
        conn: &Connection,
        row: &Row,
    ) -> rusqlite::Result<Document> {
        let id: String = row.get("id")?;
        let versions = self.load_versions(conn, &id)?;
        row_to_document_with_versions(row, versions)
    }

    /// Convert a row to a lightweight DocumentSummary.
    pub(crate) fn row_to_summary(
        &self,
        conn: &Connection,
        row: &Row,
    ) -> rusqlite::Result<DocumentSummary> {
        let id: String = row.get("id")?;

        let tags: Vec<String> = row
            .get::<_, Option<String>>("tags")?
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let current_version = self.load_current_version(conn, &id)?;

        Ok(DocumentSummary {
            id,
            source_id: row.get("source_id")?,
            title: row.get("title")?,
            source_url: row.get("source_url")?,
            synopsis: row.get("synopsis")?,
            tags,
            status: DocumentStatus::from_str(&row.get::<_, String>("status")?)
                .unwrap_or(DocumentStatus::Pending),
            created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("created_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("updated_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            current_version,
        })
    }

    /// Load only the most recent version for a document.
    pub(crate) fn load_current_version(
        &self,
        conn: &Connection,
        document_id: &str,
    ) -> rusqlite::Result<Option<VersionSummary>> {
        let mut stmt = conn.prepare(
            "SELECT content_hash, file_path, file_size, mime_type, acquired_at, original_filename, server_date
             FROM document_versions WHERE document_id = ? ORDER BY acquired_at DESC LIMIT 1"
        )?;

        stmt.query_row(params![document_id], |row| {
            Ok(VersionSummary {
                content_hash: row.get("content_hash")?,
                file_path: PathBuf::from(row.get::<_, String>("file_path")?),
                file_size: row.get::<_, i64>("file_size")? as u64,
                mime_type: row.get("mime_type")?,
                acquired_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("acquired_at")?)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
                original_filename: row.get("original_filename")?,
                server_date: row
                    .get::<_, Option<String>>("server_date")?
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc)),
            })
        })
        .optional()
    }

    /// Update the MIME type of a specific document version.
    pub fn update_version_mime_type(
        &self,
        document_id: &str,
        version_id: i64,
        new_mime_type: &str,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE document_versions SET mime_type = ?1 WHERE document_id = ?2 AND id = ?3",
            params![new_mime_type, document_id, version_id],
        )?;
        Ok(())
    }
}
