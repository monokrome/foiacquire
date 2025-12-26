//! Database schema initialization and migrations.

use rusqlite::params;
use std::path::PathBuf;
use tracing::{info, warn};

use super::helpers::{extract_filename_parts, sanitize_filename};
use super::{DocumentRepository, STORAGE_FORMAT_VERSION};
use crate::repository::Result;

impl DocumentRepository {
    /// Initialize the database schema.
    pub(crate) fn init_schema(&self) -> Result<()> {
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
            CREATE INDEX IF NOT EXISTS idx_documents_source_updated
                ON documents(source_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_documents_source_status
                ON documents(source_id, status);
            CREATE INDEX IF NOT EXISTS idx_documents_synopsis_null
                ON documents(source_id) WHERE synopsis IS NULL;
            CREATE INDEX IF NOT EXISTS idx_versions_doc_mime
                ON document_versions(document_id, mime_type);
            CREATE INDEX IF NOT EXISTS idx_pages_doc_version
                ON document_pages(document_id, version_id);
            CREATE INDEX IF NOT EXISTS idx_pages_with_text
                ON document_pages(document_id) WHERE final_text IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_documents_estimated_date
                ON documents(estimated_date) WHERE estimated_date IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_documents_with_tags
                ON documents(id) WHERE tags IS NOT NULL AND tags != '[]';

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

            CREATE TABLE IF NOT EXISTS document_counts (
                source_id TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TRIGGER IF NOT EXISTS tr_documents_insert
            AFTER INSERT ON documents
            BEGIN
                INSERT INTO document_counts (source_id, count)
                VALUES (NEW.source_id, 1)
                ON CONFLICT(source_id) DO UPDATE SET count = count + 1;
            END;

            CREATE TRIGGER IF NOT EXISTS tr_documents_delete
            AFTER DELETE ON documents
            BEGIN
                UPDATE document_counts SET count = count - 1
                WHERE source_id = OLD.source_id;
            END;
        "#,
        )?;
        Ok(())
    }

    /// Check and run storage migrations if needed.
    pub(crate) fn migrate_storage(&self) -> Result<()> {
        let conn = self.connect()?;

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

        if current_version < 3 {
            let _ = conn.execute(
                "ALTER TABLE document_versions ADD COLUMN original_filename TEXT",
                [],
            );
            let _ = conn.execute(
                "ALTER TABLE document_versions ADD COLUMN server_date TEXT",
                [],
            );
            info!("Added original_filename and server_date columns");
        }

        if current_version < 4 {
            let _ = conn.execute("ALTER TABLE documents ADD COLUMN synopsis TEXT", []);
            let _ = conn.execute("ALTER TABLE documents ADD COLUMN tags TEXT", []);
            info!("Added synopsis and tags columns");
        }

        if current_version < 5 {
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
            info!("Added virtual_files table");
        }

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
            info!("Added document_pages table");
        }

        if current_version < 7 {
            let _ = conn.execute(
                "ALTER TABLE document_versions ADD COLUMN page_count INTEGER",
                [],
            );
            info!("Added page_count column");
        }

        if current_version < 8 {
            let _ = conn.execute("ALTER TABLE documents ADD COLUMN estimated_date TEXT", []);
            let _ = conn.execute("ALTER TABLE documents ADD COLUMN date_confidence TEXT", []);
            let _ = conn.execute("ALTER TABLE documents ADD COLUMN date_source TEXT", []);
            let _ = conn.execute("ALTER TABLE documents ADD COLUMN manual_date TEXT", []);
            info!("Added date estimation columns");

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
            info!("Added page_ocr_results table");
        }

        if current_version < 10 {
            let _ = conn.execute(
                "ALTER TABLE documents ADD COLUMN discovery_method TEXT NOT NULL DEFAULT 'import'",
                [],
            );
            info!("Added discovery_method column");
        }

        if current_version < 11 {
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS document_counts (
                    source_id TEXT PRIMARY KEY,
                    count INTEGER NOT NULL DEFAULT 0
                );
                INSERT OR REPLACE INTO document_counts (source_id, count)
                SELECT source_id, COUNT(*) FROM documents GROUP BY source_id;

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
            info!("Added document_counts table with triggers");
        }

        if current_version < 12 {
            let _ = conn.execute(
                "ALTER TABLE documents ADD COLUMN category_id TEXT REFERENCES file_categories(id)",
                [],
            );

            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS file_categories (
                    id TEXT PRIMARY KEY,
                    description TEXT,
                    doc_count INTEGER NOT NULL DEFAULT 0
                );

                INSERT OR IGNORE INTO file_categories (id, description, doc_count) VALUES
                    ('documents', 'PDF, Word, text, and email documents', 0),
                    ('images', 'Image files (PNG, JPG, GIF, etc.)', 0),
                    ('data', 'Spreadsheets, CSV, JSON, and XML files', 0),
                    ('archives', 'ZIP, TAR, and other archive files', 0),
                    ('other', 'Other file types', 0);

                CREATE INDEX IF NOT EXISTS idx_documents_category
                    ON documents(category_id) WHERE category_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_documents_category_updated
                    ON documents(category_id, updated_at DESC) WHERE category_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_documents_category_source_updated
                    ON documents(category_id, source_id, updated_at DESC) WHERE category_id IS NOT NULL;
            "#,
            )?;

            info!("Backfilling category_id for existing documents...");
            conn.execute_batch(
                r#"
                DROP TRIGGER IF EXISTS tr_category_count_insert;
                DROP TRIGGER IF EXISTS tr_category_count_delete;
                DROP TRIGGER IF EXISTS tr_category_count_update;

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

                UPDATE file_categories SET doc_count = (
                    SELECT COUNT(*) FROM documents WHERE category_id = file_categories.id
                );

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
            info!("Added file_categories table and backfilled category_id");
        }

        if current_version < 13 {
            // Add markup category for HTML/XML files
            conn.execute_batch(
                r#"
                INSERT OR IGNORE INTO file_categories (id, description, doc_count) VALUES
                    ('markup', 'HTML and XML markup files', 0);

                -- Recategorize HTML/XML from documents to markup
                UPDATE documents SET category_id = 'markup'
                WHERE id IN (
                    SELECT d.id FROM documents d
                    JOIN document_versions dv ON dv.document_id = d.id
                    WHERE dv.mime_type IN ('text/html', 'application/xhtml+xml', 'text/xml', 'application/xml')
                    GROUP BY d.id
                );

                -- Also move XML from data to markup
                UPDATE documents SET category_id = 'markup'
                WHERE id IN (
                    SELECT d.id FROM documents d
                    JOIN document_versions dv ON dv.document_id = d.id
                    WHERE dv.mime_type = 'application/xml'
                    GROUP BY d.id
                );

                -- Recalculate all category counts
                UPDATE file_categories SET doc_count = (
                    SELECT COUNT(*) FROM documents WHERE category_id = file_categories.id
                );
                "#,
            )?;
            info!("Added markup category and recategorized HTML/XML files");
        }

        // Migrate file paths if needed
        self.migrate_file_paths(&conn)?;

        conn.execute(
            "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('format_version', ?)",
            params![STORAGE_FORMAT_VERSION.to_string()],
        )?;

        self.cleanup_empty_dirs()?;

        Ok(())
    }

    /// Migrate file paths to new naming scheme.
    fn migrate_file_paths(&self, conn: &rusqlite::Connection) -> Result<()> {
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

            if !old_path.exists() {
                continue;
            }

            let url = source_url.as_deref().unwrap_or("");
            let (basename, extension) = extract_filename_parts(url, &title, &mime_type);
            let filename = format!(
                "{}-{}.{}",
                sanitize_filename(&basename),
                &content_hash[..8],
                extension
            );
            let new_path = self.documents_dir.join(&content_hash[..2]).join(&filename);

            if old_path == new_path {
                continue;
            }

            if let Some(parent) = new_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    warn!("Failed to create directory {:?}: {}", parent, e);
                    errors += 1;
                    continue;
                }
            }

            match std::fs::rename(&old_path, &new_path) {
                Ok(_) => {
                    if let Err(e) = conn.execute(
                        "UPDATE document_versions SET file_path = ? WHERE id = ?",
                        params![new_path.to_string_lossy(), version_id],
                    ) {
                        warn!("Failed to update path in database: {}", e);
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

        if migrated > 0 || errors > 0 {
            info!(
                "Storage migration: {} files migrated, {} errors",
                migrated, errors
            );
        }

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
}
