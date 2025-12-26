//! Complex query operations - browse, search, and filtered retrieval.

use rusqlite::{params, Connection};
use std::path::PathBuf;

use super::helpers::{
    mime_type_condition, BrowseResult, DocumentNavigation, DocumentSummary, OptionalExt,
};
use super::DocumentRepository;
use crate::models::{Document, DocumentStatus, DocumentVersion};
use crate::repository::{parse_datetime, Result};

impl DocumentRepository {
    /// Get documents needing OCR processing.
    pub fn get_needing_ocr(&self, source_id: Option<&str>, limit: usize) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let placeholders = Self::OCR_SUPPORTED_MIME_TYPES
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match source_id {
            Some(sid) => {
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = Self::OCR_SUPPORTED_MIME_TYPES
                    .iter()
                    .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::ToSql>)
                    .collect();
                params.push(Box::new(sid.to_string()));
                (
                    format!(
                        "SELECT d.* FROM documents d
                         JOIN document_versions dv ON dv.document_id = d.id
                         WHERE d.status = 'downloaded'
                           AND dv.mime_type IN ({})
                           AND d.source_id = ?
                         GROUP BY d.id
                         LIMIT {}",
                        placeholders,
                        limit.max(1)
                    ),
                    params,
                )
            }
            None => {
                let params: Vec<Box<dyn rusqlite::ToSql>> = Self::OCR_SUPPORTED_MIME_TYPES
                    .iter()
                    .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::ToSql>)
                    .collect();
                (
                    format!(
                        "SELECT d.* FROM documents d
                         JOIN document_versions dv ON dv.document_id = d.id
                         WHERE d.status = 'downloaded'
                           AND dv.mime_type IN ({})
                         GROUP BY d.id
                         LIMIT {}",
                        placeholders,
                        limit.max(1)
                    ),
                    params,
                )
            }
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

    /// Get documents needing LLM summarization.
    pub fn get_needing_summarization(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match source_id {
            Some(sid) => (
                format!(
                    "SELECT DISTINCT d.* FROM documents d
                     JOIN document_pages dp ON dp.document_id = d.id
                     WHERE d.synopsis IS NULL
                       AND d.source_id = ?
                       AND dp.final_text IS NOT NULL AND LENGTH(dp.final_text) > 0
                     LIMIT {}",
                    limit.max(1)
                ),
                vec![Box::new(sid.to_string()) as Box<dyn rusqlite::ToSql>],
            ),
            None => (
                format!(
                    "SELECT DISTINCT d.* FROM documents d
                     JOIN document_pages dp ON dp.document_id = d.id
                     WHERE d.synopsis IS NULL
                       AND dp.final_text IS NOT NULL AND LENGTH(dp.final_text) > 0
                     LIMIT {}",
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

    /// Get documents filtered by tag.
    pub fn get_by_tag(&self, tag: &str, source_id: Option<&str>) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let tag_pattern = format!("%\"{}%", tag.to_lowercase());

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match source_id {
            Some(sid) => (
                "SELECT * FROM documents WHERE LOWER(tags) LIKE ? AND source_id = ? ORDER BY updated_at DESC".to_string(),
                vec![
                    Box::new(tag_pattern) as Box<dyn rusqlite::ToSql>,
                    Box::new(sid.to_string()) as Box<dyn rusqlite::ToSql>,
                ],
            ),
            None => (
                "SELECT * FROM documents WHERE LOWER(tags) LIKE ? ORDER BY updated_at DESC".to_string(),
                vec![Box::new(tag_pattern) as Box<dyn rusqlite::ToSql>],
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

    /// Get recently added/updated documents.
    pub fn get_recent(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DocumentSummary>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            let mut stmt = conn.prepare(&format!(
                "SELECT id, source_id, title, source_url, synopsis, tags, status, created_at, updated_at
                 FROM documents WHERE source_id = ? ORDER BY updated_at DESC LIMIT {}",
                limit.max(1)
            ))?;
            let summaries = stmt
                .query_map(params![sid], |row| self.row_to_summary(&conn, row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(summaries)
        } else {
            let mut stmt = conn.prepare(&format!(
                "SELECT id, source_id, title, source_url, synopsis, tags, status, created_at, updated_at
                 FROM documents ORDER BY updated_at DESC LIMIT {}",
                limit.max(1)
            ))?;
            let summaries = stmt
                .query_map([], |row| self.row_to_summary(&conn, row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(summaries)
        }
    }

    /// Get documents filtered by MIME type.
    pub fn get_by_mime_type(
        &self,
        mime_type: &str,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            let mut stmt = conn.prepare(&format!(
                r#"SELECT d.* FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE d.source_id = ?
                   AND dv.mime_type = ?
                   AND dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = d.id)
                   ORDER BY d.updated_at DESC
                   LIMIT {}"#,
                limit.max(1)
            ))?;
            let docs = stmt
                .query_map(params![sid, mime_type], |row| {
                    self.row_to_document(&conn, row)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(docs)
        } else {
            let mut stmt = conn.prepare(&format!(
                r#"SELECT d.* FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE dv.mime_type = ?
                   AND dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = d.id)
                   ORDER BY d.updated_at DESC
                   LIMIT {}"#,
                limit.max(1)
            ))?;
            let docs = stmt
                .query_map(params![mime_type], |row| self.row_to_document(&conn, row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(docs)
        }
    }

    /// Get documents filtered by MIME type category (pdf, images, documents, etc).
    pub fn get_by_type_category(
        &self,
        category: &str,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        let mime_condition = match mime_type_condition(category) {
            Some(c) => c,
            None => return Ok(vec![]),
        };

        let sql = if source_id.is_some() {
            format!(
                r#"SELECT d.* FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE d.source_id = ?
                   AND {}
                   AND dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = d.id)
                   ORDER BY d.updated_at DESC
                   LIMIT {}"#,
                mime_condition,
                limit.max(1)
            )
        } else {
            format!(
                r#"SELECT d.* FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE {}
                   AND dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = d.id)
                   ORDER BY d.updated_at DESC
                   LIMIT {}"#,
                mime_condition,
                limit.max(1)
            )
        };

        let mut stmt = conn.prepare(&sql)?;
        if let Some(sid) = source_id {
            let docs = stmt
                .query_map(params![sid], |row| self.row_to_document(&conn, row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(docs)
        } else {
            let docs = stmt
                .query_map([], |row| self.row_to_document(&conn, row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(docs)
        }
    }

    /// Get documents with combined filters using offset-based pagination.
    #[allow(clippy::too_many_arguments)]
    pub fn browse(
        &self,
        types: &[String],
        tags: &[String],
        source_id: Option<&str>,
        query: Option<&str>,
        page: usize,
        limit: usize,
        cached_total: Option<u64>,
    ) -> Result<BrowseResult> {
        let conn = self.connect()?;
        let limit = limit.clamp(1, 200);
        let page = page.max(1);
        let offset = (page - 1) * limit;

        let doc_conditions = self.build_browse_conditions(types, source_id, tags, query);
        let type_condition = self.build_type_conditions(types);

        let sql = if let Some(type_cond) = type_condition {
            format!(
                r#"SELECT
                    d.id, d.source_id, d.source_url, d.title, d.synopsis, d.tags,
                    d.extracted_text, d.created_at, d.updated_at, d.status,
                    dv.mime_type, dv.file_size, dv.file_path,
                    dv.acquired_at as version_acquired_at,
                    dv.original_filename, dv.server_date, dv.content_hash,
                    d.discovery_method
                FROM documents d INDEXED BY idx_documents_source_updated
                JOIN document_versions dv ON d.id = dv.document_id
                WHERE {} AND {}
                ORDER BY d.updated_at DESC
                LIMIT ? OFFSET ?"#,
                doc_conditions.join(" AND "),
                type_cond
            )
        } else {
            format!(
                r#"WITH filtered_docs AS (
                    SELECT id FROM documents d
                    WHERE {}
                    ORDER BY updated_at DESC
                    LIMIT ? OFFSET ?
                )
                SELECT
                    d.id, d.source_id, d.source_url, d.title, d.synopsis, d.tags,
                    d.extracted_text, d.created_at, d.updated_at, d.status,
                    dv.mime_type, dv.file_size, dv.file_path,
                    dv.acquired_at as version_acquired_at,
                    dv.original_filename, dv.server_date, dv.content_hash,
                    d.discovery_method
                FROM filtered_docs fd
                JOIN documents d ON fd.id = d.id
                JOIN document_versions dv ON d.id = dv.document_id"#,
                doc_conditions.join(" AND ")
            )
        };

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        self.add_browse_params(&mut params_vec, source_id, tags, query);
        params_vec.push(Box::new((limit + 1) as i64));
        params_vec.push(Box::new(offset as i64));

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;

        let mut documents = Vec::with_capacity(limit + 1);
        let mut rows = stmt.query(params_refs.as_slice())?;
        while let Some(row) = rows.next()? {
            let tags_json: Option<String> = row.get(5)?;
            let tags: Vec<String> = tags_json
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let file_path: String = row.get(12)?;
            let status_str: String = row.get(9)?;

            documents.push(Document {
                id: row.get(0)?,
                source_id: row.get(1)?,
                source_url: row.get(2)?,
                title: row.get(3)?,
                synopsis: row.get(4)?,
                tags,
                extracted_text: row.get(6)?,
                created_at: parse_datetime(&row.get::<_, String>(7)?),
                updated_at: parse_datetime(&row.get::<_, String>(8)?),
                status: DocumentStatus::from_str(&status_str).unwrap_or(DocumentStatus::Pending),
                metadata: serde_json::Value::Null,
                versions: vec![DocumentVersion {
                    id: 0,
                    content_hash: row.get(16)?,
                    file_path: PathBuf::from(file_path),
                    file_size: row.get::<_, i64>(11)? as u64,
                    mime_type: row.get(10)?,
                    acquired_at: parse_datetime(&row.get::<_, String>(13)?),
                    source_url: None,
                    original_filename: row.get(14)?,
                    server_date: row
                        .get::<_, Option<String>>(15)?
                        .map(|s| parse_datetime(&s)),
                    page_count: None,
                }],
                discovery_method: row.get(17)?,
            });
        }

        let has_next = documents.len() > limit;
        if has_next {
            documents.pop();
        }

        let total = match cached_total {
            Some(count) => count,
            None => self.browse_count(types, tags, source_id, query)?,
        };

        let start_position = offset as u64 + 1;
        let prev_cursor = if page > 1 {
            Some((page - 1).to_string())
        } else {
            None
        };
        let next_cursor = if has_next {
            Some((page + 1).to_string())
        } else {
            None
        };

        Ok(BrowseResult {
            documents,
            prev_cursor,
            next_cursor,
            start_position,
            total,
        })
    }

    /// Count documents matching the browse filters.
    pub fn browse_count(
        &self,
        types: &[String],
        tags: &[String],
        source_id: Option<&str>,
        query: Option<&str>,
    ) -> Result<u64> {
        let conn = self.connect()?;

        let type_condition = self.build_type_conditions(types);
        let doc_conditions = self.build_browse_conditions(types, source_id, tags, query);

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        self.add_browse_params(&mut params_vec, source_id, tags, query);

        let sql = if let Some(type_cond) = type_condition {
            format!(
                r#"SELECT COUNT(DISTINCT d.id) FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE {} AND {}"#,
                doc_conditions.join(" AND "),
                type_cond
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM documents d WHERE {}",
                doc_conditions.join(" AND ")
            )
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let count: i64 = stmt.query_row(params_refs.as_slice(), |row| row.get(0))?;

        Ok(count as u64)
    }

    /// Get navigation context for a document within a filtered result set.
    pub fn get_document_navigation(
        &self,
        doc_id: &str,
        types: &[String],
        tags: &[String],
        source_id: Option<&str>,
        query: Option<&str>,
    ) -> Result<Option<DocumentNavigation>> {
        let conn = self.connect()?;

        let mut conditions: Vec<String> = vec![
            "dv.id = (SELECT MAX(id) FROM document_versions WHERE document_id = d.id)".to_string(),
        ];

        if !types.is_empty() {
            let type_conditions: Vec<String> = types
                .iter()
                .filter_map(|t| mime_type_condition(t))
                .collect();

            if !type_conditions.is_empty() {
                conditions.push(format!("({})", type_conditions.join(" OR ")));
            }
        }

        if source_id.is_some() {
            conditions.push("d.source_id = ?".to_string());
        }

        for _ in tags.iter() {
            conditions.push("LOWER(d.tags) LIKE ?".to_string());
        }

        if query.is_some() {
            conditions.push("(d.title LIKE ? OR d.synopsis LIKE ?)".to_string());
        }

        let sql = format!(
            r#"WITH ranked AS (
                SELECT
                    d.id,
                    d.title,
                    ROW_NUMBER() OVER (ORDER BY d.updated_at DESC, d.id ASC) as row_num,
                    LAG(d.id) OVER (ORDER BY d.updated_at DESC, d.id ASC) as prev_id,
                    LAG(d.title) OVER (ORDER BY d.updated_at DESC, d.id ASC) as prev_title,
                    LEAD(d.id) OVER (ORDER BY d.updated_at DESC, d.id ASC) as next_id,
                    LEAD(d.title) OVER (ORDER BY d.updated_at DESC, d.id ASC) as next_title,
                    COUNT(*) OVER () as total
                FROM documents d
                JOIN document_versions dv ON d.id = dv.document_id
                WHERE {}
            )
            SELECT prev_id, prev_title, next_id, next_title, row_num, total
            FROM ranked WHERE id = ?"#,
            conditions.join(" AND ")
        );

        let mut stmt = conn.prepare(&sql)?;

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(sid) = source_id {
            params_vec.push(Box::new(sid.to_string()));
        }

        for tag in tags {
            let tag_pattern = format!("%\"{}%", tag.to_lowercase());
            params_vec.push(Box::new(tag_pattern));
        }

        if let Some(q) = query {
            let query_pattern = format!("%{}%", q);
            params_vec.push(Box::new(query_pattern.clone()));
            params_vec.push(Box::new(query_pattern));
        }

        params_vec.push(Box::new(doc_id.to_string()));

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let result = stmt
            .query_row(params_refs.as_slice(), |row| {
                Ok(DocumentNavigation {
                    prev_id: row.get(0)?,
                    prev_title: row.get(1)?,
                    next_id: row.get(2)?,
                    next_title: row.get(3)?,
                    position: row.get::<_, i64>(4)? as u64,
                    total: row.get::<_, i64>(5)? as u64,
                })
            })
            .optional()?;

        Ok(result)
    }

    /// Search tags with fuzzy matching (for autocomplete).
    pub fn search_tags(&self, query: &str, limit: usize) -> Result<Vec<(String, usize)>> {
        let all_tags = self.get_all_tags()?;
        let query_lower = query.to_lowercase();

        let mut matches: Vec<_> = all_tags
            .into_iter()
            .filter(|(tag, _)| tag.to_lowercase().contains(&query_lower))
            .collect();

        matches.sort_by(|(a, count_a), (b, count_b)| {
            let a_starts = a.to_lowercase().starts_with(&query_lower);
            let b_starts = b.to_lowercase().starts_with(&query_lower);
            match (a_starts, b_starts) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => count_b.cmp(count_a),
            }
        });

        matches.truncate(limit);
        Ok(matches)
    }

    // ========== Helper methods for browse queries ==========

    fn build_browse_conditions(
        &self,
        types: &[String],
        source_id: Option<&str>,
        tags: &[String],
        query: Option<&str>,
    ) -> Vec<String> {
        let mut conditions: Vec<String> = vec!["1=1".to_string()];

        if source_id.is_some() {
            conditions.push("d.source_id = ?".to_string());
        }

        if !types.is_empty() {
            let valid_categories: Vec<&str> = types
                .iter()
                .filter_map(|t| match t.to_lowercase().as_str() {
                    "documents" | "pdf" | "text" | "email" => Some("documents"),
                    "images" => Some("images"),
                    "data" => Some("data"),
                    "archives" => Some("archives"),
                    "other" => Some("other"),
                    _ => None,
                })
                .collect();

            if !valid_categories.is_empty() {
                let mut unique_cats: Vec<&str> = valid_categories.clone();
                unique_cats.sort();
                unique_cats.dedup();

                if unique_cats.len() == 1 {
                    conditions.push(format!("d.category_id = '{}'", unique_cats[0]));
                } else {
                    let in_list = unique_cats
                        .iter()
                        .map(|c| format!("'{}'", c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    conditions.push(format!("d.category_id IN ({})", in_list));
                }
            }
        }

        for _ in tags.iter() {
            conditions.push("LOWER(d.tags) LIKE ?".to_string());
        }

        if query.is_some() {
            conditions.push("(d.title LIKE ? OR d.synopsis LIKE ?)".to_string());
        }

        conditions
    }

    fn build_type_conditions(&self, _types: &[String]) -> Option<String> {
        None
    }

    fn add_browse_params(
        &self,
        params_vec: &mut Vec<Box<dyn rusqlite::ToSql>>,
        source_id: Option<&str>,
        tags: &[String],
        query: Option<&str>,
    ) {
        if let Some(sid) = source_id {
            params_vec.push(Box::new(sid.to_string()));
        }

        for tag in tags {
            let tag_pattern = format!("%\"{}%", tag.to_lowercase());
            params_vec.push(Box::new(tag_pattern));
        }

        if let Some(q) = query {
            let query_pattern = format!("%{}%", q);
            params_vec.push(Box::new(query_pattern.clone()));
            params_vec.push(Box::new(query_pattern));
        }
    }

    #[allow(dead_code)]
    fn get_doc_id_at_position(
        &self,
        conn: &Connection,
        conditions: &[String],
        source_id: Option<&str>,
        tags: &[String],
        query: Option<&str>,
        position: i64,
    ) -> Result<Option<String>> {
        let sql = format!(
            r#"SELECT d.id FROM documents d
               JOIN document_versions dv ON d.id = dv.document_id
               WHERE {}
               ORDER BY d.updated_at DESC, d.id ASC
               LIMIT 1 OFFSET ?"#,
            conditions.join(" AND ")
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        self.add_browse_params(&mut params_vec, source_id, tags, query);
        params_vec.push(Box::new(position - 1));

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        Ok(conn
            .query_row(&sql, params_refs.as_slice(), |row| row.get::<_, String>(0))
            .ok())
    }
}
