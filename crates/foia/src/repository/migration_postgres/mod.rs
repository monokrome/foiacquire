//! PostgreSQL implementation of database migration traits.
//!
//! Only compiled when the `postgres` feature is enabled.
//!
//! This module is split into submodules for maintainability:
//! - `mod.rs` (this file): PostgresMigrator struct, utilities
//! - `copy.rs`: COPY protocol bulk import methods
//! - `exporter.rs`: DatabaseExporter trait implementation
//! - `importer.rs`: DatabaseImporter trait implementation

mod copy;
mod exporter;
mod importer;

use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use super::util::{pg_to_diesel_error as pg_error, to_diesel_error};
use super::DieselError;

/// PostgreSQL database migrator.
pub struct PostgresMigrator {
    pub(crate) pool: Pool<AsyncPgConnection>,
    pub(crate) database_url: String,
    pub(crate) batch_size: usize,
    pub(crate) no_tls: bool,
}

impl PostgresMigrator {
    /// Create a new PostgreSQL migrator.
    pub async fn new(database_url: &str, no_tls: bool) -> Result<Self, DieselError> {
        let mgr = if no_tls {
            AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url)
        } else {
            let mut manager_config = ManagerConfig::default();
            manager_config.custom_setup = Box::new(super::pg_tls::establish_tls_connection);
            AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(
                database_url,
                manager_config,
            )
        };
        let pool = Pool::builder(mgr)
            .max_size(10)
            .build()
            .map_err(to_diesel_error)?;
        Ok(Self {
            pool,
            database_url: database_url.to_string(),
            batch_size: 1,
            no_tls,
        })
    }

    /// Set the batch size for bulk inserts.
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size.max(1);
    }

    /// Escape a value for COPY text format.
    /// NULL -> \N, backslash -> \\, tab -> \t, newline -> \n
    pub(crate) fn escape_copy_value(value: Option<&str>) -> String {
        match value {
            None => "\\N".to_string(),
            Some(s) => s
                .replace('\\', "\\\\")
                .replace('\t', "\\t")
                .replace('\n', "\\n")
                .replace('\r', "\\r"),
        }
    }

    /// Clear specific tables by name.
    /// Uses a single TRUNCATE statement for atomicity and proper FK handling.
    pub async fn clear_tables(&self, tables: &[&str]) -> Result<(), DieselError> {
        if tables.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let sql = format!("TRUNCATE {} RESTART IDENTITY", tables.join(", "));
        diesel::sql_query(sql).execute(&mut conn).await?;
        Ok(())
    }

    /// Reset sequence counters after COPY import.
    pub async fn reset_sequences(&self) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;

        diesel::sql_query(
            "SELECT setval('document_versions_id_seq', COALESCE((SELECT MAX(id) FROM document_versions), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        diesel::sql_query(
            "SELECT setval('document_pages_id_seq', COALESCE((SELECT MAX(id) FROM document_pages), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        diesel::sql_query(
            "SELECT setval('crawl_urls_id_seq', COALESCE((SELECT MAX(id) FROM crawl_urls), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        diesel::sql_query(
            "SELECT setval('crawl_requests_id_seq', COALESCE((SELECT MAX(id) FROM crawl_requests), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        Ok(())
    }

    /// Run ANALYZE on specified tables to update statistics.
    pub async fn analyze_tables(&self, tables: &[&str]) -> Result<(), DieselError> {
        if tables.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let sql = format!("ANALYZE {}", tables.join(", "));
        diesel::sql_query(sql).execute(&mut conn).await?;
        Ok(())
    }

    /// Run ANALYZE on all migration tables.
    pub async fn analyze_all(&self) -> Result<(), DieselError> {
        self.analyze_tables(&[
            "sources",
            "documents",
            "document_versions",
            "document_pages",
            "virtual_files",
            "crawl_urls",
            "crawl_requests",
            "crawl_config",
            "configuration_history",
            "rate_limit_state",
        ])
        .await
    }

    /// Get existing string IDs from a table.
    pub async fn get_existing_string_ids(
        &self,
        table: &str,
        id_column: &str,
        ids: &[String],
    ) -> Result<std::collections::HashSet<String>, DieselError> {
        use std::collections::HashSet;

        if ids.is_empty() {
            return Ok(HashSet::new());
        }

        let client = super::pg_tls::connect_raw(&self.database_url, self.no_tls)
            .await
            .map_err(|e| DieselError::QueryBuilderError(e.to_string().into()))?;

        let mut existing = HashSet::new();
        for chunk in ids.chunks(1000) {
            let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
            let sql = format!(
                "SELECT {} FROM {} WHERE {} IN ({})",
                id_column,
                table,
                id_column,
                placeholders.join(", ")
            );

            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = chunk
                .iter()
                .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            let rows = client.query(&sql, &params).await.map_err(pg_error)?;
            for row in rows {
                let id: String = row.get(0);
                existing.insert(id);
            }
        }

        Ok(existing)
    }

    /// Get existing integer IDs from a table.
    pub async fn get_existing_int_ids(
        &self,
        table: &str,
        id_column: &str,
        ids: &[i32],
    ) -> Result<std::collections::HashSet<i32>, DieselError> {
        use std::collections::HashSet;

        if ids.is_empty() {
            return Ok(HashSet::new());
        }

        let client = super::pg_tls::connect_raw(&self.database_url, self.no_tls)
            .await
            .map_err(|e| DieselError::QueryBuilderError(e.to_string().into()))?;

        let mut existing = HashSet::new();
        for chunk in ids.chunks(1000) {
            let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
            let sql = format!(
                "SELECT {} FROM {} WHERE {} IN ({})",
                id_column,
                table,
                id_column,
                placeholders.join(", ")
            );

            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = chunk
                .iter()
                .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            let rows = client.query(&sql, &params).await.map_err(pg_error)?;
            for row in rows {
                let id: i32 = row.get(0);
                existing.insert(id);
            }
        }

        Ok(existing)
    }

    /// Initialize the schema (create tables if they don't exist).
    pub async fn init_schema(&self) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;

        let statements = [
            r#"CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                last_scraped TEXT
            )"#,
            r#"CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES sources(id),
                title TEXT NOT NULL,
                source_url TEXT NOT NULL,
                extracted_text TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                synopsis TEXT,
                tags TEXT,
                estimated_date TEXT,
                date_confidence TEXT,
                date_source TEXT,
                manual_date TEXT,
                discovery_method TEXT NOT NULL DEFAULT 'seed',
                category_id TEXT
            )"#,
            r#"CREATE TABLE IF NOT EXISTS document_versions (
                id SERIAL PRIMARY KEY,
                document_id TEXT NOT NULL REFERENCES documents(id),
                content_hash TEXT NOT NULL,
                content_hash_blake3 TEXT,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                source_url TEXT,
                original_filename TEXT,
                server_date TEXT,
                page_count INTEGER
            )"#,
            r#"CREATE TABLE IF NOT EXISTS document_pages (
                id SERIAL PRIMARY KEY,
                document_id TEXT NOT NULL REFERENCES documents(id),
                version_id INTEGER NOT NULL,
                page_number INTEGER NOT NULL,
                pdf_text TEXT,
                ocr_text TEXT,
                final_text TEXT,
                ocr_status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE IF NOT EXISTS virtual_files (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL REFERENCES documents(id),
                version_id INTEGER NOT NULL,
                archive_path TEXT NOT NULL,
                filename TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                extracted_text TEXT,
                synopsis TEXT,
                tags TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE IF NOT EXISTS crawl_urls (
                id SERIAL PRIMARY KEY,
                url TEXT NOT NULL,
                source_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'discovered',
                discovery_method TEXT NOT NULL DEFAULT 'seed',
                parent_url TEXT,
                discovery_context TEXT NOT NULL DEFAULT '{}',
                depth INTEGER NOT NULL DEFAULT 0,
                discovered_at TEXT NOT NULL,
                fetched_at TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,
                etag TEXT,
                last_modified TEXT,
                content_hash TEXT,
                document_id TEXT,
                UNIQUE(source_id, url)
            )"#,
            r#"CREATE TABLE IF NOT EXISTS crawl_requests (
                id SERIAL PRIMARY KEY,
                source_id TEXT NOT NULL,
                url TEXT NOT NULL,
                method TEXT NOT NULL DEFAULT 'GET',
                request_headers TEXT NOT NULL DEFAULT '{}',
                request_at TEXT NOT NULL,
                response_status INTEGER,
                response_headers TEXT NOT NULL DEFAULT '{}',
                response_at TEXT,
                response_size INTEGER,
                duration_ms INTEGER,
                error TEXT,
                was_conditional INTEGER NOT NULL DEFAULT 0,
                was_not_modified INTEGER NOT NULL DEFAULT 0
            )"#,
            r#"CREATE TABLE IF NOT EXISTS crawl_config (
                source_id TEXT PRIMARY KEY,
                config_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE IF NOT EXISTS configuration_history (
                uuid TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                data TEXT NOT NULL,
                format TEXT NOT NULL DEFAULT 'json',
                hash TEXT NOT NULL
            )"#,
            r#"CREATE TABLE IF NOT EXISTS rate_limit_state (
                domain TEXT PRIMARY KEY,
                current_delay_ms INTEGER NOT NULL,
                in_backoff INTEGER NOT NULL DEFAULT 0,
                total_requests INTEGER NOT NULL DEFAULT 0,
                rate_limit_hits INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE IF NOT EXISTS page_ocr_results (
                id SERIAL PRIMARY KEY,
                page_id INTEGER NOT NULL REFERENCES document_pages(id) ON DELETE CASCADE,
                backend TEXT NOT NULL,
                text TEXT,
                confidence REAL,
                quality_score REAL,
                char_count INTEGER,
                word_count INTEGER,
                processing_time_ms INTEGER,
                error_message TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(page_id, backend)
            )"#,
            "CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id)",
            "CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status)",
            "CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(source_url)",
            "CREATE INDEX IF NOT EXISTS idx_document_versions_doc ON document_versions(document_id)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_urls_source_status ON crawl_urls(source_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_requests_source ON crawl_requests(source_id, request_at)",
            "CREATE INDEX IF NOT EXISTS idx_page_ocr_results_page ON page_ocr_results(page_id)",
            "CREATE INDEX IF NOT EXISTS idx_page_ocr_results_backend ON page_ocr_results(backend)",
            "CREATE INDEX IF NOT EXISTS idx_page_ocr_results_page_quality ON page_ocr_results(page_id, quality_score DESC NULLS LAST)",
        ];

        for stmt in statements {
            diesel::sql_query(stmt).execute(&mut conn).await?;
        }

        // Migrate existing OCR data from document_pages if not already migrated
        // This is idempotent due to ON CONFLICT DO NOTHING
        let data_migrations = [
            r#"INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
               SELECT id, 'tesseract', ocr_text, LENGTH(ocr_text),
                      array_length(regexp_split_to_array(ocr_text, '\s+'), 1),
                      COALESCE(updated_at::timestamptz, created_at::timestamptz, NOW())
               FROM document_pages
               WHERE ocr_text IS NOT NULL AND ocr_text != ''
               ON CONFLICT (page_id, backend) DO NOTHING"#,
            r#"INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
               SELECT id, 'pdftotext', pdf_text, LENGTH(pdf_text),
                      array_length(regexp_split_to_array(pdf_text, '\s+'), 1),
                      COALESCE(updated_at::timestamptz, created_at::timestamptz, NOW())
               FROM document_pages
               WHERE pdf_text IS NOT NULL AND pdf_text != ''
               ON CONFLICT (page_id, backend) DO NOTHING"#,
        ];

        for stmt in data_migrations {
            diesel::sql_query(stmt).execute(&mut conn).await?;
        }

        Ok(())
    }
}
