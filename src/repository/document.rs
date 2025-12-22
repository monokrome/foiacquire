//! Document repository for SQLite persistence.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Row};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

use super::{parse_datetime, Result};
use crate::models::{
    Document, DocumentPage, DocumentStatus, DocumentVersion, PageOcrStatus, VirtualFile,
    VirtualFileStatus,
};

/// Get SQL condition for a document type category.
///
/// Returns the SQL WHERE clause fragment for filtering by the given type category.
/// Categories: "documents", "data", "images", "pdf", "text", "email", "other"
fn mime_type_condition(category: &str) -> Option<String> {
    match category.to_lowercase().as_str() {
        "pdf" => Some("dv.mime_type = 'application/pdf'".to_string()),
        "documents" => Some(
            "(dv.mime_type = 'application/pdf' OR dv.mime_type LIKE '%word%' \
             OR dv.mime_type = 'application/msword' OR dv.mime_type LIKE '%rfc822%' \
             OR dv.mime_type LIKE 'message/%' \
             OR (dv.mime_type LIKE 'text/%' AND dv.mime_type != 'text/csv'))"
                .to_string(),
        ),
        "data" => Some(
            "(dv.mime_type LIKE '%spreadsheet%' OR dv.mime_type LIKE '%excel%' \
             OR dv.mime_type = 'application/vnd.ms-excel' OR dv.mime_type = 'text/csv' \
             OR dv.mime_type = 'application/json' OR dv.mime_type = 'application/xml')"
                .to_string(),
        ),
        "images" => Some("dv.mime_type LIKE 'image/%'".to_string()),
        "text" => Some(
            "(dv.mime_type LIKE 'text/%' AND dv.mime_type != 'text/html' \
             AND dv.mime_type != 'text/csv')"
                .to_string(),
        ),
        "email" => {
            Some("(dv.mime_type LIKE '%rfc822%' OR dv.mime_type LIKE 'message/%')".to_string())
        }
        "other" => Some(
            "(dv.mime_type NOT LIKE 'image/%' AND dv.mime_type != 'application/pdf' \
             AND dv.mime_type NOT LIKE '%word%' AND dv.mime_type NOT LIKE '%spreadsheet%' \
             AND dv.mime_type NOT LIKE '%excel%' AND dv.mime_type NOT LIKE 'text/%' \
             AND dv.mime_type NOT LIKE '%rfc822%' AND dv.mime_type NOT LIKE 'message/%' \
             AND dv.mime_type != 'application/json' AND dv.mime_type != 'application/xml')"
                .to_string(),
        ),
        _ => None,
    }
}

/// Current storage format version. Increment when changing file naming scheme.
const STORAGE_FORMAT_VERSION: i32 = 12;

/// Partial document data loaded from a row, before versions are attached.
/// Used internally by bulk-load methods to avoid N+1 queries.
struct DocumentPartial {
    id: String,
    source_id: String,
    title: String,
    source_url: String,
    extracted_text: Option<String>,
    synopsis: Option<String>,
    tags: Vec<String>,
    status: DocumentStatus,
    metadata: serde_json::Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    discovery_method: String,
}

impl DocumentPartial {
    fn with_versions(self, versions: Vec<DocumentVersion>) -> Document {
        Document {
            id: self.id,
            source_id: self.source_id,
            title: self.title,
            source_url: self.source_url,
            versions,
            extracted_text: self.extracted_text,
            synopsis: self.synopsis,
            tags: self.tags,
            status: self.status,
            metadata: self.metadata,
            created_at: self.created_at,
            updated_at: self.updated_at,
            discovery_method: self.discovery_method,
        }
    }
}

/// Lightweight document summary for listings (excludes extracted_text for memory efficiency).
#[derive(Debug, Clone)]
pub struct DocumentSummary {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub source_url: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub status: DocumentStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Current version info (if any)
    pub current_version: Option<VersionSummary>,
}

/// Lightweight version summary.
#[derive(Debug, Clone)]
pub struct VersionSummary {
    pub content_hash: String,
    pub file_path: PathBuf,
    pub file_size: u64,
    pub mime_type: String,
    pub acquired_at: DateTime<Utc>,
    pub original_filename: Option<String>,
    pub server_date: Option<DateTime<Utc>>,
}

/// Navigation context for a document within a filtered list.
/// Uses window functions to efficiently find prev/next documents.
#[derive(Debug, Clone)]
pub struct DocumentNavigation {
    pub prev_id: Option<String>,
    pub prev_title: Option<String>,
    pub next_id: Option<String>,
    pub next_title: Option<String>,
    pub position: u64,
    pub total: u64,
}

/// Result of cursor-based pagination browse query.
#[derive(Debug, Clone)]
pub struct BrowseResult {
    pub documents: Vec<Document>,
    /// ID of the first document on the previous page (for "Previous" link)
    pub prev_cursor: Option<String>,
    /// ID of the first document on the next page (for "Next" link)
    pub next_cursor: Option<String>,
    /// Position of first document on this page (1-indexed)
    pub start_position: u64,
    /// Total documents matching filters
    pub total: u64,
}

/// SQLite-backed document repository.
pub struct DocumentRepository {
    db_path: PathBuf,
    documents_dir: PathBuf,
}

impl DocumentRepository {
    /// Create a new document repository.
    pub fn new(db_path: &Path, documents_dir: &Path) -> Result<Self> {
        let repo = Self {
            db_path: db_path.to_path_buf(),
            documents_dir: documents_dir.to_path_buf(),
        };
        repo.init_schema()?;
        repo.migrate_storage()?;
        Ok(repo)
    }

    fn connect(&self) -> Result<Connection> {
        super::connect(&self.db_path)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                title TEXT NOT NULL,
                source_url TEXT NOT NULL,
                extracted_text TEXT,
                synopsis TEXT,
                tags TEXT,
                status TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                estimated_date TEXT,
                date_confidence TEXT,
                date_source TEXT,
                manual_date TEXT,
                discovery_method TEXT NOT NULL DEFAULT 'import',
                category_id TEXT REFERENCES file_categories(id)
            );

            CREATE TABLE IF NOT EXISTS document_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                source_url TEXT,
                original_filename TEXT,
                server_date TEXT,
                page_count INTEGER,
                FOREIGN KEY (document_id) REFERENCES documents(id)
            );

            CREATE TABLE IF NOT EXISTS storage_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS virtual_files (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                version_id INTEGER NOT NULL,
                archive_path TEXT NOT NULL,
                filename TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                extracted_text TEXT,
                synopsis TEXT,
                tags TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES documents(id),
                FOREIGN KEY (version_id) REFERENCES document_versions(id)
            );

            CREATE TABLE IF NOT EXISTS document_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                version_id INTEGER NOT NULL,
                page_number INTEGER NOT NULL,
                pdf_text TEXT,
                ocr_text TEXT,
                final_text TEXT,
                ocr_status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES documents(id),
                FOREIGN KEY (version_id) REFERENCES document_versions(id),
                UNIQUE(document_id, version_id, page_number)
            );

            CREATE INDEX IF NOT EXISTS idx_documents_source
                ON documents(source_id);
            CREATE INDEX IF NOT EXISTS idx_documents_status
                ON documents(status);
            CREATE INDEX IF NOT EXISTS idx_documents_url
                ON documents(source_url);
            CREATE INDEX IF NOT EXISTS idx_versions_document
                ON document_versions(document_id);
            CREATE INDEX IF NOT EXISTS idx_versions_hash
                ON document_versions(content_hash);
            CREATE INDEX IF NOT EXISTS idx_versions_mime_type
                ON document_versions(mime_type);
            CREATE INDEX IF NOT EXISTS idx_documents_updated_at
                ON documents(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_virtual_files_document
                ON virtual_files(document_id);
            CREATE INDEX IF NOT EXISTS idx_virtual_files_version
                ON virtual_files(version_id);
            CREATE INDEX IF NOT EXISTS idx_virtual_files_status
                ON virtual_files(status);
            CREATE INDEX IF NOT EXISTS idx_document_pages_document
                ON document_pages(document_id);
            CREATE INDEX IF NOT EXISTS idx_document_pages_version
                ON document_pages(version_id);
            CREATE INDEX IF NOT EXISTS idx_document_pages_ocr_status
                ON document_pages(ocr_status);

            -- Composite indexes for common query patterns
            -- Browse queries filtered by source with sorting
            CREATE INDEX IF NOT EXISTS idx_documents_source_updated
                ON documents(source_id, updated_at DESC);

            -- OCR queries filter by source + status
            CREATE INDEX IF NOT EXISTS idx_documents_source_status
                ON documents(source_id, status);

            -- Summarization queries check synopsis IS NULL
            CREATE INDEX IF NOT EXISTS idx_documents_synopsis_null
                ON documents(source_id) WHERE synopsis IS NULL;

            -- Version lookups with mime_type filter (JOIN optimization)
            CREATE INDEX IF NOT EXISTS idx_versions_doc_mime
                ON document_versions(document_id, mime_type);

            -- Page lookups for specific document+version
            CREATE INDEX IF NOT EXISTS idx_pages_doc_version
                ON document_pages(document_id, version_id);

            -- Pages with OCR text (for summarization queries)
            CREATE INDEX IF NOT EXISTS idx_pages_with_text
                ON document_pages(document_id) WHERE final_text IS NOT NULL;

            -- Date-based filtering
            CREATE INDEX IF NOT EXISTS idx_documents_estimated_date
                ON documents(estimated_date) WHERE estimated_date IS NOT NULL;

            -- Documents with tags (for tag stats - only ~0.3% of docs have tags)
            CREATE INDEX IF NOT EXISTS idx_documents_with_tags
                ON documents(id) WHERE tags IS NOT NULL AND tags != '[]';

            -- NOTE: Category indexes and triggers are created by migration v12
            -- (init_schema runs before migrations, so category_id may not exist yet)

            CREATE TABLE IF NOT EXISTS document_annotations (
                document_id TEXT NOT NULL,
                annotation_type TEXT NOT NULL,
                completed_at TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                result TEXT,
                error TEXT,
                PRIMARY KEY (document_id, annotation_type),
                FOREIGN KEY (document_id) REFERENCES documents(id)
            );
            CREATE INDEX IF NOT EXISTS idx_annotations_type
                ON document_annotations(annotation_type);
            CREATE INDEX IF NOT EXISTS idx_annotations_completed
                ON document_annotations(completed_at);

            CREATE TABLE IF NOT EXISTS page_ocr_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_id INTEGER NOT NULL,
                backend TEXT NOT NULL,
                ocr_text TEXT,
                confidence REAL,
                processing_time_ms INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY (page_id) REFERENCES document_pages(id),
                UNIQUE(page_id, backend)
            );
            CREATE INDEX IF NOT EXISTS idx_page_ocr_results_page
                ON page_ocr_results(page_id);
            CREATE INDEX IF NOT EXISTS idx_page_ocr_results_backend
                ON page_ocr_results(backend);

            -- Document counts by source (maintained by triggers for O(1) lookups)
            CREATE TABLE IF NOT EXISTS document_counts (
                source_id TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0
            );

            -- Trigger to increment count on document INSERT
            -- Note: INSERT OR REPLACE is DELETE + INSERT, so this works correctly
            CREATE TRIGGER IF NOT EXISTS tr_documents_insert
            AFTER INSERT ON documents
            BEGIN
                INSERT INTO document_counts (source_id, count)
                VALUES (NEW.source_id, 1)
                ON CONFLICT(source_id) DO UPDATE SET count = count + 1;
            END;

            -- Trigger to decrement count on document DELETE
            CREATE TRIGGER IF NOT EXISTS tr_documents_delete
            AFTER DELETE ON documents
            BEGIN
                UPDATE document_counts SET count = count - 1
                WHERE source_id = OLD.source_id;
            END;

            -- NOTE: file_categories table and triggers are created by migration v12
            -- (init_schema runs before migrations, so category_id may not exist yet)
        "#,
        )?;
        Ok(())
    }

    /// Check and run storage migrations if needed.
    pub fn migrate_storage(&self) -> Result<()> {
        let conn = self.connect()?;

        // Get current storage version
        let current_version: i32 = conn
            .query_row(
                "SELECT value FROM storage_meta WHERE key = 'format_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);

        if current_version >= STORAGE_FORMAT_VERSION {
            return Ok(());
        }

        info!(
            "Migrating storage from version {} to {}",
            current_version, STORAGE_FORMAT_VERSION
        );

        // Add new columns if upgrading from version 2 to 3
        if current_version < 3 {
            // Add original_filename column
            if conn
                .execute(
                    "ALTER TABLE document_versions ADD COLUMN original_filename TEXT",
                    [],
                )
                .is_ok()
            {
                info!("Added original_filename column to document_versions");
            }
            // Add server_date column
            if conn
                .execute(
                    "ALTER TABLE document_versions ADD COLUMN server_date TEXT",
                    [],
                )
                .is_ok()
            {
                info!("Added server_date column to document_versions");
            }
        }

        // Add synopsis and tags columns if upgrading from version 3 to 4
        if current_version < 4 {
            // Add synopsis column
            if conn
                .execute("ALTER TABLE documents ADD COLUMN synopsis TEXT", [])
                .is_ok()
            {
                info!("Added synopsis column to documents");
            }
            // Add tags column (stored as JSON array string)
            if conn
                .execute("ALTER TABLE documents ADD COLUMN tags TEXT", [])
                .is_ok()
            {
                info!("Added tags column to documents");
            }
        }

        // Add virtual_files table for version 5
        if current_version < 5 {
            // The table is created in init_schema, but we need to ensure
            // it exists for existing databases being upgraded
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS virtual_files (
                    id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL,
                    version_id INTEGER NOT NULL,
                    archive_path TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    extracted_text TEXT,
                    synopsis TEXT,
                    tags TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (document_id) REFERENCES documents(id),
                    FOREIGN KEY (version_id) REFERENCES document_versions(id)
                );
                CREATE INDEX IF NOT EXISTS idx_virtual_files_document
                    ON virtual_files(document_id);
                CREATE INDEX IF NOT EXISTS idx_virtual_files_version
                    ON virtual_files(version_id);
                CREATE INDEX IF NOT EXISTS idx_virtual_files_status
                    ON virtual_files(status);
            "#,
            )?;
            info!("Added virtual_files table for archive contents");
        }

        // Add document_pages table for version 6 (per-page text extraction)
        if current_version < 6 {
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS document_pages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id TEXT NOT NULL,
                    version_id INTEGER NOT NULL,
                    page_number INTEGER NOT NULL,
                    pdf_text TEXT,
                    ocr_text TEXT,
                    final_text TEXT,
                    ocr_status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (document_id) REFERENCES documents(id),
                    FOREIGN KEY (version_id) REFERENCES document_versions(id),
                    UNIQUE(document_id, version_id, page_number)
                );
                CREATE INDEX IF NOT EXISTS idx_document_pages_document
                    ON document_pages(document_id);
                CREATE INDEX IF NOT EXISTS idx_document_pages_version
                    ON document_pages(version_id);
                CREATE INDEX IF NOT EXISTS idx_document_pages_ocr_status
                    ON document_pages(ocr_status);
            "#,
            )?;
            info!("Added document_pages table for per-page text extraction");
        }

        // Add page_count column for version 7 (cached page count)
        if current_version < 7
            && conn
                .execute(
                    "ALTER TABLE document_versions ADD COLUMN page_count INTEGER",
                    [],
                )
                .is_ok()
        {
            info!("Added page_count column to document_versions");
        }

        // Add date estimation and annotations for version 8
        if current_version < 8 {
            // Add estimated date columns to documents
            let _ = conn.execute(
                "ALTER TABLE documents ADD COLUMN estimated_date TEXT",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE documents ADD COLUMN date_confidence TEXT",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE documents ADD COLUMN date_source TEXT",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE documents ADD COLUMN manual_date TEXT",
                [],
            );
            info!("Added date estimation columns to documents");

            // Create document_annotations table
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS document_annotations (
                    document_id TEXT NOT NULL,
                    annotation_type TEXT NOT NULL,
                    completed_at TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    result TEXT,
                    error TEXT,
                    PRIMARY KEY (document_id, annotation_type),
                    FOREIGN KEY (document_id) REFERENCES documents(id)
                );
                CREATE INDEX IF NOT EXISTS idx_annotations_type
                    ON document_annotations(annotation_type);
                CREATE INDEX IF NOT EXISTS idx_annotations_completed
                    ON document_annotations(completed_at);
            "#,
            )?;
            info!("Added document_annotations table");
        }

        // Add page_ocr_results table for version 9 (alternative OCR backends)
        if current_version < 9 {
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS page_ocr_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    page_id INTEGER NOT NULL,
                    backend TEXT NOT NULL,
                    ocr_text TEXT,
                    confidence REAL,
                    processing_time_ms INTEGER,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (page_id) REFERENCES document_pages(id),
                    UNIQUE(page_id, backend)
                );
                CREATE INDEX IF NOT EXISTS idx_page_ocr_results_page
                    ON page_ocr_results(page_id);
                CREATE INDEX IF NOT EXISTS idx_page_ocr_results_backend
                    ON page_ocr_results(backend);
            "#,
            )?;
            info!("Added page_ocr_results table for alternative OCR backends");
        }

        // Add discovery_method column for version 10 (tracking document provenance)
        if current_version < 10 {
            conn.execute_batch(
                r#"
                ALTER TABLE documents ADD COLUMN discovery_method TEXT NOT NULL DEFAULT 'import';
            "#,
            )?;
            info!("Added discovery_method column to documents table");
        }

        // Add document_counts table with triggers for version 11 (O(1) count lookups)
        if current_version < 11 {
            conn.execute_batch(
                r#"
                -- Create counts table
                CREATE TABLE IF NOT EXISTS document_counts (
                    source_id TEXT PRIMARY KEY,
                    count INTEGER NOT NULL DEFAULT 0
                );

                -- Populate initial counts from existing documents
                INSERT OR REPLACE INTO document_counts (source_id, count)
                SELECT source_id, COUNT(*) FROM documents GROUP BY source_id;

                -- Create triggers (DROP first in case schema changed)
                DROP TRIGGER IF EXISTS tr_documents_insert;
                DROP TRIGGER IF EXISTS tr_documents_delete;

                CREATE TRIGGER tr_documents_insert
                AFTER INSERT ON documents
                BEGIN
                    INSERT INTO document_counts (source_id, count)
                    VALUES (NEW.source_id, 1)
                    ON CONFLICT(source_id) DO UPDATE SET count = count + 1;
                END;

                CREATE TRIGGER tr_documents_delete
                AFTER DELETE ON documents
                BEGIN
                    UPDATE document_counts SET count = count - 1
                    WHERE source_id = OLD.source_id;
                END;
            "#,
            )?;
            info!("Added document_counts table with triggers for O(1) count lookups");
        }

        // Add file_categories table and category_id column for version 12
        if current_version < 12 {
            // Add category_id column to documents
            let _ = conn.execute(
                "ALTER TABLE documents ADD COLUMN category_id TEXT REFERENCES file_categories(id)",
                [],
            );

            // Create file_categories table
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS file_categories (
                    id TEXT PRIMARY KEY,
                    description TEXT,
                    doc_count INTEGER NOT NULL DEFAULT 0
                );

                -- Pre-populate categories
                INSERT OR IGNORE INTO file_categories (id, description, doc_count) VALUES
                    ('documents', 'PDF, Word, text, and email documents', 0),
                    ('images', 'Image files (PNG, JPG, GIF, etc.)', 0),
                    ('data', 'Spreadsheets, CSV, JSON, and XML files', 0),
                    ('archives', 'ZIP, TAR, and other archive files', 0),
                    ('other', 'Other file types', 0);

                -- Create indexes for category filtering
                CREATE INDEX IF NOT EXISTS idx_documents_category
                    ON documents(category_id) WHERE category_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_documents_category_updated
                    ON documents(category_id, updated_at DESC) WHERE category_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_documents_category_source_updated
                    ON documents(category_id, source_id, updated_at DESC) WHERE category_id IS NOT NULL;

                -- Triggers to maintain file_categories.doc_count
                DROP TRIGGER IF EXISTS tr_category_count_insert;
                DROP TRIGGER IF EXISTS tr_category_count_delete;
                DROP TRIGGER IF EXISTS tr_category_count_update;

                CREATE TRIGGER tr_category_count_insert
                AFTER INSERT ON documents
                WHEN NEW.category_id IS NOT NULL
                BEGIN
                    UPDATE file_categories SET doc_count = doc_count + 1
                    WHERE id = NEW.category_id;
                END;

                CREATE TRIGGER tr_category_count_delete
                AFTER DELETE ON documents
                WHEN OLD.category_id IS NOT NULL
                BEGIN
                    UPDATE file_categories SET doc_count = doc_count - 1
                    WHERE id = OLD.category_id;
                END;

                CREATE TRIGGER tr_category_count_update
                AFTER UPDATE OF category_id ON documents
                WHEN OLD.category_id IS NOT NEW.category_id
                BEGIN
                    UPDATE file_categories SET doc_count = doc_count - 1
                    WHERE id = OLD.category_id AND OLD.category_id IS NOT NULL;
                    UPDATE file_categories SET doc_count = doc_count + 1
                    WHERE id = NEW.category_id AND NEW.category_id IS NOT NULL;
                END;
            "#,
            )?;

            // Backfill category_id based on existing document_versions mime_type
            // Uses the same logic as mime_type_condition but maps to category IDs
            info!("Backfilling category_id for existing documents...");
            conn.execute_batch(
                r#"
                -- Disable triggers temporarily for backfill (we'll compute counts at the end)
                DROP TRIGGER IF EXISTS tr_category_count_insert;
                DROP TRIGGER IF EXISTS tr_category_count_delete;
                DROP TRIGGER IF EXISTS tr_category_count_update;

                -- Set category_id based on first version's mime_type
                UPDATE documents SET category_id = (
                    SELECT CASE
                        WHEN dv.mime_type = 'application/pdf' THEN 'documents'
                        WHEN dv.mime_type LIKE '%word%' THEN 'documents'
                        WHEN dv.mime_type = 'application/msword' THEN 'documents'
                        WHEN dv.mime_type LIKE '%rfc822%' THEN 'documents'
                        WHEN dv.mime_type LIKE 'message/%' THEN 'documents'
                        WHEN dv.mime_type LIKE 'text/%' AND dv.mime_type != 'text/csv' THEN 'documents'
                        WHEN dv.mime_type LIKE 'image/%' THEN 'images'
                        WHEN dv.mime_type LIKE '%spreadsheet%' THEN 'data'
                        WHEN dv.mime_type LIKE '%excel%' THEN 'data'
                        WHEN dv.mime_type = 'application/vnd.ms-excel' THEN 'data'
                        WHEN dv.mime_type = 'text/csv' THEN 'data'
                        WHEN dv.mime_type = 'application/json' THEN 'data'
                        WHEN dv.mime_type = 'application/xml' THEN 'data'
                        WHEN dv.mime_type LIKE '%zip%' THEN 'archives'
                        WHEN dv.mime_type LIKE '%tar%' THEN 'archives'
                        WHEN dv.mime_type LIKE '%gzip%' THEN 'archives'
                        WHEN dv.mime_type LIKE '%compress%' THEN 'archives'
                        WHEN dv.mime_type = 'application/x-7z-compressed' THEN 'archives'
                        WHEN dv.mime_type = 'application/x-rar-compressed' THEN 'archives'
                        ELSE 'other'
                    END
                    FROM document_versions dv
                    WHERE dv.document_id = documents.id
                    ORDER BY dv.acquired_at ASC
                    LIMIT 1
                )
                WHERE category_id IS NULL;

                -- Compute counts from actual data
                UPDATE file_categories SET doc_count = (
                    SELECT COUNT(*) FROM documents WHERE category_id = file_categories.id
                );

                -- Re-create triggers
                CREATE TRIGGER tr_category_count_insert
                AFTER INSERT ON documents
                WHEN NEW.category_id IS NOT NULL
                BEGIN
                    UPDATE file_categories SET doc_count = doc_count + 1
                    WHERE id = NEW.category_id;
                END;

                CREATE TRIGGER tr_category_count_delete
                AFTER DELETE ON documents
                WHEN OLD.category_id IS NOT NULL
                BEGIN
                    UPDATE file_categories SET doc_count = doc_count - 1
                    WHERE id = OLD.category_id;
                END;

                CREATE TRIGGER tr_category_count_update
                AFTER UPDATE OF category_id ON documents
                WHEN OLD.category_id IS NOT NEW.category_id
                BEGIN
                    UPDATE file_categories SET doc_count = doc_count - 1
                    WHERE id = OLD.category_id AND OLD.category_id IS NOT NULL;
                    UPDATE file_categories SET doc_count = doc_count + 1
                    WHERE id = NEW.category_id AND NEW.category_id IS NOT NULL;
                END;
            "#,
            )?;

            info!("Added file_categories table and backfilled category_id for all documents");
        }

        // Get all versions that need migration
        let mut stmt = conn.prepare(
            "SELECT dv.id, dv.content_hash, dv.file_path, dv.mime_type, dv.source_url, d.title
             FROM document_versions dv
             JOIN documents d ON d.id = dv.document_id",
        )?;

        let versions: Vec<(i64, String, String, String, Option<String>, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut migrated = 0;
        let mut errors = 0;

        for (version_id, content_hash, old_path, mime_type, source_url, title) in versions {
            let old_path = PathBuf::from(&old_path);

            // Skip if file doesn't exist (already migrated or missing)
            if !old_path.exists() {
                continue;
            }

            // Compute new path
            let url = source_url.as_deref().unwrap_or("");
            let (basename, extension) = extract_filename_parts(url, &title, &mime_type);
            let filename = format!(
                "{}-{}.{}",
                sanitize_filename(&basename),
                &content_hash[..8],
                extension
            );
            let new_path = self.documents_dir.join(&content_hash[..2]).join(&filename);

            // Skip if already at correct path
            if old_path == new_path {
                continue;
            }

            // Create parent directory and move file
            if let Some(parent) = new_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    warn!("Failed to create directory {:?}: {}", parent, e);
                    errors += 1;
                    continue;
                }
            }

            match std::fs::rename(&old_path, &new_path) {
                Ok(_) => {
                    // Update database with new path
                    if let Err(e) = conn.execute(
                        "UPDATE document_versions SET file_path = ? WHERE id = ?",
                        params![new_path.to_string_lossy(), version_id],
                    ) {
                        warn!("Failed to update path in database: {}", e);
                        // Try to move file back
                        let _ = std::fs::rename(&new_path, &old_path);
                        errors += 1;
                    } else {
                        migrated += 1;
                    }
                }
                Err(e) => {
                    warn!("Failed to move {:?} to {:?}: {}", old_path, new_path, e);
                    errors += 1;
                }
            }
        }

        // Update version in database
        conn.execute(
            "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('format_version', ?)",
            params![STORAGE_FORMAT_VERSION.to_string()],
        )?;

        if migrated > 0 || errors > 0 {
            info!(
                "Storage migration complete: {} files migrated, {} errors",
                migrated, errors
            );
        }

        // Clean up empty directories
        self.cleanup_empty_dirs()?;

        Ok(())
    }

    /// Remove empty directories in the documents folder.
    fn cleanup_empty_dirs(&self) -> Result<()> {
        if let Ok(entries) = std::fs::read_dir(&self.documents_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Ok(mut dir) = std::fs::read_dir(&path) {
                        if dir.next().is_none() {
                            let _ = std::fs::remove_dir(&path);
                        }
                    }
                }
            }
        }
        Ok(())
    }

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
    /// This is much faster than checking each URL individually against the database.
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

        // First pass: collect document IDs and partial data
        let mut stmt = conn.prepare("SELECT * FROM documents WHERE source_id = ?")?;
        let rows: Vec<_> = stmt
            .query_map(params![source_id], |row| {
                let id: String = row.get("id")?;
                Ok((id, Self::row_to_document_partial(row)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Bulk load all versions
        let doc_ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let versions_map = self.load_versions_bulk(&conn, &doc_ids)?;

        // Combine into full documents
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

        // First pass: collect document IDs and partial data
        let mut stmt = conn.prepare("SELECT * FROM documents WHERE status = ?")?;
        let rows: Vec<_> = stmt
            .query_map(params![status.as_str()], |row| {
                let id: String = row.get("id")?;
                Ok((id, Self::row_to_document_partial(row)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Bulk load all versions
        let doc_ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let versions_map = self.load_versions_bulk(&conn, &doc_ids)?;

        // Combine into full documents
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

        // First pass: collect document IDs and partial data
        let mut stmt = conn.prepare("SELECT * FROM documents")?;
        let rows: Vec<_> = stmt
            .query_map([], |row| {
                let id: String = row.get("id")?;
                Ok((id, Self::row_to_document_partial(row)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Bulk load all versions
        let doc_ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let versions_map = self.load_versions_bulk(&conn, &doc_ids)?;

        // Combine into full documents
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
    /// Use this for listings and aggregate queries to save memory.
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
    /// More efficient than get_content_hashes() when you only need to check one hash.
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
        // Clone document data for retry closure
        let doc = doc.clone();

        super::with_retry(|| {
            let conn = self.connect()?;

            // Serialize tags as JSON array
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

            // Get existing version hashes
            let existing_hashes: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT content_hash FROM document_versions WHERE document_id = ?")?;
                let rows = stmt.query_map(params![doc.id], |row| row.get(0))?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };

            // Insert new versions
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
    /// Uses the trigger-maintained document_counts table.
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
    /// Uses the trigger-maintained document_counts table.
    /// Returns a HashMap of source_id -> count.
    pub fn get_all_source_counts(&self) -> Result<std::collections::HashMap<String, u64>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT source_id, count FROM document_counts")?;

        let mut counts = std::collections::HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;

        for row in rows {
            let (source_id, count) = row?;
            counts.insert(source_id, count);
        }

        Ok(counts)
    }

    /// Get document counts grouped by status in a single query (no N+1).
    /// Returns a HashMap of status -> count.
    pub fn count_all_by_status(&self) -> Result<std::collections::HashMap<String, u64>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT status, COUNT(*) FROM documents GROUP BY status")?;

        let mut counts = std::collections::HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;

        for row in rows {
            let (status, count) = row?;
            counts.insert(status, count);
        }

        Ok(counts)
    }

    /// MIME types supported by the OCR extractor.
    const OCR_SUPPORTED_MIME_TYPES: &'static [&'static str] = &[
        "application/pdf",
        "image/png",
        "image/jpeg",
        "image/tiff",
        "image/gif",
        "image/bmp",
        "text/plain",
        "text/html",
    ];

    /// Get documents needing OCR processing.
    /// Returns documents with 'downloaded' status that haven't been OCR'd yet.
    pub fn get_needing_ocr(&self, source_id: Option<&str>, limit: usize) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        // Only include MIME types supported by the OCR extractor
        // Join with document_versions to get mime_type (using the latest version)
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

    /// Count documents needing OCR.
    pub fn count_needing_ocr(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        // Only count MIME types supported by the OCR extractor
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

    /// Get documents needing LLM summarization.
    /// Returns documents that have pages with final_text but no synopsis.
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

    /// Count documents needing summarization.
    pub fn count_needing_summarization(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        // Count documents that have pages with final_text but no synopsis yet
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

    /// Get documents filtered by tag.
    pub fn get_by_tag(&self, tag: &str, source_id: Option<&str>) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        // Search for tag in the JSON array
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

    /// Get all unique tags across all documents.
    /// Optimized: uses SQLite json_each function instead of parsing in Rust.
    pub fn get_all_tags(&self) -> Result<Vec<(String, usize)>> {
        let conn = self.connect()?;

        // Use SQLite's json_each to expand tags array and count in SQL
        // This is much faster than loading all rows and parsing JSON in Rust
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

    /// Get the documents directory.
    pub fn documents_dir(&self) -> &Path {
        &self.documents_dir
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

    /// Get category statistics from file_categories table.
    /// O(1) lookup using pre-computed counts maintained by triggers.
    /// Returns (category_id, doc_count) pairs.
    pub fn get_category_stats(&self, source_id: Option<&str>) -> Result<Vec<(String, u64)>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            // Source-filtered: count from documents table
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
            // Global stats: read directly from file_categories (instant)
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
    /// NOTE: For category-based stats, use get_category_stats() which is faster.
    /// This method is kept for detailed MIME type analysis.
    pub fn get_type_stats(&self, source_id: Option<&str>) -> Result<Vec<(String, u64)>> {
        let conn = self.connect()?;

        if let Some(sid) = source_id {
            // Source-filtered: must join to documents table
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
            // Global stats: no join needed, much faster
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

        // Get MIME type condition for this category
        let mime_condition = match mime_type_condition(category) {
            Some(c) => c,
            None => return Ok(vec![]),
        };

        let sql = if let Some(_sid) = source_id {
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
    ///
    /// Optimized query that filters documents first, then joins versions.
    /// Uses OFFSET for pagination which works well with our indexes.
    ///
    /// If `cached_total` is provided, it will be used instead of computing the count.
    /// This allows the caller to provide a cached count for better performance.
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

        // Build filter conditions
        let doc_conditions = self.build_browse_conditions(types, source_id, tags, query);
        let type_condition = self.build_type_conditions(types);

        // Optimized query using CTE to filter documents first, then join versions.
        // Note: We skip the MAX(id) subquery for version selection because 99.99%
        // of documents have only one version. For rare multi-version docs, we'll
        // get any version which is acceptable for browse listings.
        let sql = if let Some(type_cond) = type_condition {
            // With type filter: use index hint to force source_updated index scan
            // This is faster than letting SQLite choose version-first scan
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
            // No type filter: use CTE for better performance
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
        params_vec.push(Box::new((limit + 1) as i64)); // Fetch one extra to detect next page
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
                versions: vec![crate::models::DocumentVersion {
                    id: 0,
                    content_hash: row.get(16)?,
                    file_path: std::path::PathBuf::from(file_path),
                    file_size: row.get::<_, i64>(11)? as u64,
                    mime_type: row.get(10)?,
                    acquired_at: parse_datetime(&row.get::<_, String>(13)?),
                    source_url: None,
                    original_filename: row.get(14)?,
                    server_date: row.get::<_, Option<String>>(15)?.map(|s| parse_datetime(&s)),
                    page_count: None,
                }],
                discovery_method: row.get(17)?,
            });
        }

        // Check if there's a next page (we fetched limit+1)
        let has_next = documents.len() > limit;
        if has_next {
            documents.pop(); // Remove the extra document
        }

        // Get total count for pagination info (use cached if provided)
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

    /// Helper to get document ID at a specific row position (for cursor calculation).
    fn get_doc_id_at_position(
        &self,
        conn: &Connection,
        conditions: &[String],
        source_id: Option<&str>,
        tags: &[String],
        query: Option<&str>,
        position: i64,
    ) -> Result<Option<String>> {
        // Use a simpler query - just get the ID at that offset
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
        params_vec.push(Box::new(position - 1)); // OFFSET is 0-indexed

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        Ok(conn
            .query_row(&sql, params_refs.as_slice(), |row| row.get::<_, String>(0))
            .ok())
    }

    /// Build filter conditions for browse queries.
    /// Uses category_id for type filtering (no version join needed).
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

        // Type filtering via category_id (denormalized, no JOIN needed)
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
                // Deduplicate categories
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

    /// Build type filter conditions for document versions.
    /// NOTE: Deprecated - type filtering now uses category_id in build_browse_conditions.
    /// Kept for backward compatibility but always returns None.
    fn build_type_conditions(&self, _types: &[String]) -> Option<String> {
        // Type filtering is now handled via category_id in build_browse_conditions
        // No longer need JOIN to document_versions for type filtering
        None
    }

    /// Add browse filter parameters to a params vector.
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

    /// Count documents matching the browse filters (for pagination).
    /// Optimized: skips version join when no type filter is specified.
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

        // Build params
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        self.add_browse_params(&mut params_vec, source_id, tags, query);

        let sql = if let Some(type_cond) = type_condition {
            // Need version join for type filtering - use optimized approach
            format!(
                r#"SELECT COUNT(DISTINCT d.id) FROM documents d
                   JOIN document_versions dv ON d.id = dv.document_id
                   WHERE {} AND {}"#,
                doc_conditions.join(" AND "),
                type_cond
            )
        } else {
            // No type filter - count documents directly (much faster)
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
    /// Returns the previous and next document IDs using window functions.
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

        // Type filter (same logic as browse)
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

        // Use window functions to get prev/next in a single query
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

        // Add the document ID as the last parameter
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

        // Sort by relevance (exact prefix match first, then by count)
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

    fn load_versions(
        &self,
        conn: &Connection,
        document_id: &str,
    ) -> rusqlite::Result<Vec<DocumentVersion>> {
        let mut stmt = conn.prepare(
            "SELECT * FROM document_versions WHERE document_id = ? ORDER BY acquired_at DESC",
        )?;

        let versions = stmt
            .query_map(params![document_id], Self::row_to_version)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(versions)
    }

    /// Load versions for multiple documents in batched queries.
    /// Returns a HashMap from document_id to list of versions.
    /// Batches queries to avoid SQLite's variable limit (999 per query).
    fn load_versions_bulk(
        &self,
        conn: &Connection,
        document_ids: &[String],
    ) -> rusqlite::Result<std::collections::HashMap<String, Vec<DocumentVersion>>> {
        if document_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let mut versions_map: std::collections::HashMap<String, Vec<DocumentVersion>> =
            std::collections::HashMap::new();

        // SQLite has a limit on SQL variables (typically 999), so batch the queries
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
                let version = Self::row_to_version(row)?;
                Ok((doc_id, version))
            })?;

            for result in versions {
                let (doc_id, version) = result?;
                versions_map.entry(doc_id).or_default().push(version);
            }
        }

        Ok(versions_map)
    }

    fn row_to_version(row: &Row) -> rusqlite::Result<DocumentVersion> {
        Ok(DocumentVersion {
            id: row.get("id")?,
            content_hash: row.get("content_hash")?,
            file_path: PathBuf::from(row.get::<_, String>("file_path")?),
            file_size: row.get::<_, i64>("file_size")? as u64,
            mime_type: row.get("mime_type")?,
            acquired_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("acquired_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            source_url: row.get("source_url")?,
            original_filename: row.get("original_filename")?,
            server_date: row
                .get::<_, Option<String>>("server_date")?
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            page_count: row.get::<_, Option<i64>>("page_count")?.map(|c| c as u32),
        })
    }

    /// Parse a document row into a partial document (without versions).
    /// Used by bulk load methods to avoid N+1 queries.
    fn row_to_document_partial(row: &Row) -> rusqlite::Result<DocumentPartial> {
        let metadata_str: String = row.get("metadata")?;
        let tags: Vec<String> = row
            .get::<_, Option<String>>("tags")?
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        Ok(DocumentPartial {
            id: row.get("id")?,
            source_id: row.get("source_id")?,
            title: row.get("title")?,
            source_url: row.get("source_url")?,
            extracted_text: row.get("extracted_text")?,
            synopsis: row.get("synopsis")?,
            tags,
            status: DocumentStatus::from_str(&row.get::<_, String>("status")?)
                .unwrap_or(DocumentStatus::Pending),
            metadata: serde_json::from_str(&metadata_str)
                .unwrap_or(serde_json::Value::Object(Default::default())),
            created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("created_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("updated_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            discovery_method: row.get("discovery_method")?,
        })
    }

    fn row_to_document(&self, conn: &Connection, row: &Row) -> rusqlite::Result<Document> {
        let id: String = row.get("id")?;
        let versions = self.load_versions(conn, &id)?;
        Self::row_to_document_with_versions(row, versions)
    }

    /// Convert a row to a Document with pre-loaded versions.
    /// Used by bulk load methods to avoid N+1 queries.
    fn row_to_document_with_versions(
        row: &Row,
        versions: Vec<DocumentVersion>,
    ) -> rusqlite::Result<Document> {
        let metadata_str: String = row.get("metadata")?;

        // Parse tags from JSON string (may be null in older databases)
        let tags: Vec<String> = row
            .get::<_, Option<String>>("tags")?
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        Ok(Document {
            id: row.get("id")?,
            source_id: row.get("source_id")?,
            title: row.get("title")?,
            source_url: row.get("source_url")?,
            versions,
            extracted_text: row.get("extracted_text")?,
            synopsis: row.get("synopsis")?,
            tags,
            status: DocumentStatus::from_str(&row.get::<_, String>("status")?)
                .unwrap_or(DocumentStatus::Pending),
            metadata: serde_json::from_str(&metadata_str).unwrap_or_default(),
            created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("created_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("updated_at")?)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            discovery_method: row.get("discovery_method")?,
        })
    }

    /// Convert a row to a lightweight DocumentSummary (no extracted_text, only current version).
    fn row_to_summary(&self, conn: &Connection, row: &Row) -> rusqlite::Result<DocumentSummary> {
        let id: String = row.get("id")?;

        // Parse tags from JSON string
        let tags: Vec<String> = row
            .get::<_, Option<String>>("tags")?
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        // Load only the current (most recent) version
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
    fn load_current_version(
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

    // ========== Virtual File Operations ==========

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
        let id = conn.query_row(
            "SELECT id FROM document_versions WHERE document_id = ? ORDER BY acquired_at DESC LIMIT 1",
            params![document_id],
            |row| row.get(0),
        ).optional()?;
        Ok(id)
    }

    /// Get archive documents that haven't been processed for virtual files yet.
    pub fn get_unprocessed_archives(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let conn = self.connect()?;

        // Find zip archives where the current version doesn't have virtual files
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

        // Find emails where the current version doesn't have virtual files
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

    // ==================== Document Pages ====================

    /// Save or update a document page.
    pub fn save_page(&self, page: &DocumentPage) -> Result<i64> {
        // Clone values needed for retry closure
        let document_id = page.document_id.clone();
        let version_id = page.version_id;
        let page_number = page.page_number;
        let pdf_text = page.pdf_text.clone();
        let ocr_text = page.ocr_text.clone();
        let final_text = page.final_text.clone();
        let ocr_status = page.ocr_status.as_str().to_string();

        super::with_retry(|| {
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
            "SELECT * FROM document_pages WHERE document_id = ? AND version_id = ? ORDER BY page_number"
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
        let conn = self.connect()?;

        let page = conn.query_row(
            "SELECT * FROM document_pages WHERE document_id = ? AND version_id = ? AND page_number = ?",
            params![document_id, version_id, page_number],
            |row| self.row_to_document_page(row),
        )
        .optional()?;

        Ok(page)
    }

    /// Get pages needing OCR (status = 'text_extracted' and sparse text).
    pub fn get_pages_needing_ocr(&self, limit: usize) -> Result<Vec<DocumentPage>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(&format!(
            "SELECT * FROM document_pages WHERE ocr_status = 'text_extracted' LIMIT {}",
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

    /// Count pages for a document version (without loading all page data).
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

        // Try to get cached count from version first
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

        // Fall back to counting pages in document_pages table
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

    /// Delete all pages for a document version (for re-processing).
    pub fn delete_pages(&self, document_id: &str, version_id: i64) -> Result<u64> {
        let conn = self.connect()?;

        let deleted = conn.execute(
            "DELETE FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
        )?;

        Ok(deleted as u64)
    }

    /// Check if all pages for a document version have completed OCR.
    /// Returns true if there are pages and all have ocr_status = 'ocr_complete'.
    pub fn are_all_pages_ocr_complete(&self, document_id: &str, version_id: i64) -> Result<bool> {
        let conn = self.connect()?;

        // Count total pages vs pages with ocr_complete status
        let (total, complete): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), SUM(CASE WHEN ocr_status = 'ocr_complete' THEN 1 ELSE 0 END)
             FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
            |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
        )?;

        Ok(total > 0 && total == complete)
    }

    /// Check if all pages for a document version are done processing.
    /// Returns true if all pages have a terminal status (ocr_complete, failed, or skipped).
    pub fn are_all_pages_complete(&self, document_id: &str, version_id: i64) -> Result<bool> {
        let conn = self.connect()?;

        // Count total pages vs pages with terminal status
        let (total, done): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), SUM(CASE WHEN ocr_status IN ('ocr_complete', 'failed', 'skipped') THEN 1 ELSE 0 END)
             FROM document_pages WHERE document_id = ? AND version_id = ?",
            params![document_id, version_id],
            |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
        )?;

        Ok(total > 0 && total == done)
    }

    /// Finalize a document by combining page text and setting status to OcrComplete.
    /// Also saves the combined text to a .txt file alongside the original document.
    /// Returns Ok(true) if finalized, Ok(false) if skipped (no pages or empty text).
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

        // Update document with combined text and OcrComplete status
        let mut updated_doc = doc.clone();
        updated_doc.extracted_text = Some(combined_text.clone());
        updated_doc.status = DocumentStatus::OcrComplete;
        updated_doc.updated_at = chrono::Utc::now();
        self.save(&updated_doc)?;

        // Save text file alongside original
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

    /// Find and finalize all documents that have all pages OCR complete but document status is not.
    /// This handles documents that were processed before incremental finalization was added.
    /// Returns the number of documents finalized.
    pub fn finalize_pending_documents(&self, source_id: Option<&str>) -> Result<usize> {
        let conn = self.connect()?;

        // Find documents where:
        // 1. status is not 'ocr_complete'
        // 2. All their pages have ocr_status = 'ocr_complete'
        // We use a subquery to check this condition efficiently
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

        drop(conn); // Release connection before calling finalize_document

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

    fn row_to_document_page(&self, row: &Row) -> rusqlite::Result<DocumentPage> {
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

    fn row_to_virtual_file(&self, row: &Row) -> rusqlite::Result<VirtualFile> {
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

    // === Date estimation methods ===

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
            params![manual_date.to_rfc3339(), Utc::now().to_rfc3339(), document_id],
        )?;
        Ok(())
    }

    /// Get documents that need date estimation.
    /// Returns documents where:
    /// - estimated_date is NULL
    /// - manual_date is NULL
    /// - No "date_detection" annotation exists (hasn't been processed yet)
    pub fn get_documents_needing_date_estimation(
        &self,
        source_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Option<String>, Option<DateTime<Utc>>, DateTime<Utc>, Option<String>)>> {
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
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let server_date: Option<DateTime<Utc>> = row
                    .get::<_, Option<String>>("server_date")?
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc));

                let acquired_at = DateTime::parse_from_rfc3339(&row.get::<_, String>("acquired_at")?)
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
    /// Must match criteria in get_documents_needing_date_estimation.
    pub fn count_documents_needing_date_estimation(&self, source_id: Option<&str>) -> Result<u64> {
        let conn = self.connect()?;

        // Must match the criteria in get_documents_needing_date_estimation
        // (including the JOIN to ensure we only count documents with versions)
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

    // === Annotation tracking methods ===

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
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let ids = stmt
            .query_map(params_refs.as_slice(), |row| row.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(ids)
    }

    // === Alternative OCR results methods ===

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

    /// Get all OCR results for a page (including alternative backends).
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

    /// Get OCR results for multiple pages in a single query (avoids N+1).
    /// Returns a HashMap of page_id -> Vec<(backend, ocr_text, confidence, processing_time_ms)>.
    pub fn get_pages_ocr_results_bulk(
        &self,
        page_ids: &[i64],
    ) -> Result<std::collections::HashMap<i64, Vec<(String, Option<String>, Option<f64>, Option<i64>)>>>
    {
        if page_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let conn = self.connect()?;

        // Build query with placeholders for all page IDs
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

        // Convert page_ids to params
        let params: Vec<&dyn rusqlite::ToSql> =
            page_ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();

        let rows = stmt.query_map(params.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<f64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })?;

        let mut results: std::collections::HashMap<
            i64,
            Vec<(String, Option<String>, Option<f64>, Option<i64>)>,
        > = std::collections::HashMap::new();

        for row in rows {
            let (page_id, backend, ocr_text, confidence, processing_time_ms) = row?;
            results
                .entry(page_id)
                .or_default()
                .push((backend, ocr_text, confidence, processing_time_ms));
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
}

trait OptionalExt<T> {
    fn optional(self) -> std::result::Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalExt<T> for std::result::Result<T, rusqlite::Error> {
    fn optional(self) -> std::result::Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Extract filename parts (basename and extension) from URL, title, or mime type.
pub fn extract_filename_parts(url: &str, title: &str, mime_type: &str) -> (String, String) {
    // Try to get filename from URL path
    if let Some(filename) = url.split('/').next_back() {
        if let Some(dot_pos) = filename.rfind('.') {
            let basename = &filename[..dot_pos];
            let ext = &filename[dot_pos + 1..];
            // Only use if it looks like a real extension
            if !basename.is_empty() && ext.len() <= 5 && ext.chars().all(|c| c.is_alphanumeric()) {
                return (basename.to_string(), ext.to_lowercase());
            }
        }
    }

    // Fall back to title + mime type extension
    let ext = match mime_type {
        "application/pdf" => "pdf",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "text/html" => "html",
        "text/plain" => "txt",
        "image/jpeg" => "jpg",
        "image/png" => "png",
        _ => "bin",
    };

    let basename = if title.is_empty() { "document" } else { title };
    (basename.to_string(), ext.to_string())
}

/// Sanitize a string for use as a filename.
pub fn sanitize_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    // Trim and limit length
    let trimmed = sanitized.trim().trim_matches('_');
    if trimmed.len() > 100 {
        trimmed[..100].to_string()
    } else if trimmed.is_empty() {
        "document".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_filename_from_url() {
        let (basename, ext) = extract_filename_parts(
            "https://example.com/docs/report.pdf",
            "Some Title",
            "application/pdf",
        );
        assert_eq!(basename, "report");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_extract_filename_fallback_to_mime() {
        let (basename, ext) = extract_filename_parts(
            "https://example.com/api/download?id=123",
            "Annual Report",
            "application/pdf",
        );
        assert_eq!(basename, "Annual Report");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_extract_filename_empty_title() {
        let (basename, ext) =
            extract_filename_parts("https://example.com/api/download", "", "application/pdf");
        assert_eq!(basename, "document");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_sanitize_filename_special_chars() {
        assert_eq!(
            sanitize_filename("file/with:bad*chars?"),
            "file_with_bad_chars"
        );
    }

    #[test]
    fn test_sanitize_filename_empty() {
        assert_eq!(sanitize_filename(""), "document");
    }

    #[test]
    fn test_sanitize_filename_long() {
        let long_name = "a".repeat(150);
        let sanitized = sanitize_filename(&long_name);
        assert_eq!(sanitized.len(), 100);
    }

    #[test]
    fn test_sanitize_filename_only_special() {
        assert_eq!(sanitize_filename("///"), "document");
    }
}
