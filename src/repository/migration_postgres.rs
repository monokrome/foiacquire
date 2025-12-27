//! PostgreSQL implementation of database migration traits.
//!
//! Only compiled when the `postgres` feature is enabled.

#![cfg(feature = "postgres")]

use async_trait::async_trait;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures_util::pin_mut;
use tokio_postgres::NoTls;

use super::diesel_models::*;
use super::migration::{
    DatabaseExporter, DatabaseImporter, PortableConfigHistory, PortableCrawlConfig,
    PortableCrawlRequest, PortableCrawlUrl, PortableDocument, PortableDocumentPage,
    PortableDocumentVersion, PortableRateLimitState, PortableSource, PortableVirtualFile,
    ProgressCallback,
};
use super::util::{pg_to_diesel_error as pg_error, to_diesel_error};
use super::DieselError;
use crate::schema::*;

/// PostgreSQL database migrator.
pub struct PostgresMigrator {
    pool: Pool<AsyncPgConnection>,
    database_url: String,
    batch_size: usize,
}

impl PostgresMigrator {
    /// Create a new PostgreSQL migrator.
    pub async fn new(database_url: &str) -> Result<Self, DieselError> {
        let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
        let pool = Pool::builder(config)
            .max_size(10)
            .build()
            .map_err(to_diesel_error)?;
        Ok(Self {
            pool,
            database_url: database_url.to_string(),
            batch_size: 1,
        })
    }

    /// Set the batch size for bulk inserts.
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size.max(1);
    }

    /// Escape a value for COPY text format.
    /// NULL -> \N, backslash -> \\, tab -> \t, newline -> \n
    fn escape_copy_value(value: Option<&str>) -> String {
        match value {
            None => "\\N".to_string(),
            Some(s) => s
                .replace('\\', "\\\\")
                .replace('\t', "\\t")
                .replace('\n', "\\n")
                .replace('\r', "\\r"),
        }
    }

    /// Import documents using COPY protocol (much faster than INSERT).
    /// Requires table to be empty or will fail on duplicates.
    pub async fn copy_documents(
        &self,
        documents: &[PortableDocument],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        // Connect directly with tokio-postgres for COPY
        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        // Start COPY with explicit type annotation for bytes::Bytes
        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY documents (id, source_id, title, source_url, extracted_text, status, metadata,
                    created_at, updated_at, synopsis, tags, estimated_date, date_confidence, date_source,
                    manual_date, discovery_method, category_id)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000; // Send in chunks to avoid huge memory usage

        for chunk in documents.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 500); // Estimate avg row size

            for d in chunk {
                // Build tab-separated row
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&d.id)),
                    Self::escape_copy_value(Some(&d.source_id)),
                    Self::escape_copy_value(Some(&d.title)),
                    Self::escape_copy_value(Some(&d.source_url)),
                    Self::escape_copy_value(d.extracted_text.as_deref()),
                    Self::escape_copy_value(Some(&d.status)),
                    Self::escape_copy_value(Some(&d.metadata)),
                    Self::escape_copy_value(Some(&d.created_at)),
                    Self::escape_copy_value(Some(&d.updated_at)),
                    Self::escape_copy_value(d.synopsis.as_deref()),
                    Self::escape_copy_value(d.tags.as_deref()),
                    Self::escape_copy_value(d.estimated_date.as_deref()),
                    Self::escape_copy_value(d.date_confidence.as_deref()),
                    Self::escape_copy_value(d.date_source.as_deref()),
                    Self::escape_copy_value(d.manual_date.as_deref()),
                    Self::escape_copy_value(Some(&d.discovery_method)),
                    Self::escape_copy_value(d.category_id.as_deref()),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        // Finish COPY
        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import sources using COPY protocol.
    pub async fn copy_sources(
        &self,
        sources: &[PortableSource],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY sources (id, source_type, name, base_url, metadata, created_at, last_scraped)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut data = String::new();
        let mut count = 0;

        for s in sources {
            let row = format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                Self::escape_copy_value(Some(&s.id)),
                Self::escape_copy_value(Some(&s.source_type)),
                Self::escape_copy_value(Some(&s.name)),
                Self::escape_copy_value(Some(&s.base_url)),
                Self::escape_copy_value(Some(&s.metadata)),
                Self::escape_copy_value(Some(&s.created_at)),
                Self::escape_copy_value(s.last_scraped.as_deref()),
            );
            data.push_str(&row);
            count += 1;
        }

        sink.send(bytes::Bytes::from(data))
            .await
            .map_err(pg_error)?;

        sink.finish().await.map_err(pg_error)?;

        if let Some(ref cb) = progress {
            cb(count);
        }

        Ok(count)
    }

    /// Import document versions using COPY protocol.
    pub async fn copy_document_versions(
        &self,
        versions: &[PortableDocumentVersion],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY document_versions (id, document_id, content_hash, file_path, file_size,
                    mime_type, acquired_at, source_url, original_filename, server_date, page_count)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in versions.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 200);

            for v in chunk {
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    v.id,
                    Self::escape_copy_value(Some(&v.document_id)),
                    Self::escape_copy_value(Some(&v.content_hash)),
                    Self::escape_copy_value(Some(&v.file_path)),
                    v.file_size,
                    Self::escape_copy_value(Some(&v.mime_type)),
                    Self::escape_copy_value(Some(&v.acquired_at)),
                    Self::escape_copy_value(v.source_url.as_deref()),
                    Self::escape_copy_value(v.original_filename.as_deref()),
                    Self::escape_copy_value(v.server_date.as_deref()),
                    v.page_count
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "\\N".to_string()),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import document pages using COPY protocol.
    pub async fn copy_document_pages(
        &self,
        pages: &[PortableDocumentPage],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY document_pages (id, document_id, version_id, page_number, pdf_text,
                    ocr_text, final_text, ocr_status, created_at, updated_at)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in pages.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 500);

            for p in chunk {
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    p.id,
                    Self::escape_copy_value(Some(&p.document_id)),
                    p.version_id,
                    p.page_number,
                    Self::escape_copy_value(p.pdf_text.as_deref()),
                    Self::escape_copy_value(p.ocr_text.as_deref()),
                    Self::escape_copy_value(p.final_text.as_deref()),
                    Self::escape_copy_value(Some(&p.ocr_status)),
                    Self::escape_copy_value(Some(&p.created_at)),
                    Self::escape_copy_value(Some(&p.updated_at)),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import crawl URLs using COPY protocol.
    pub async fn copy_crawl_urls(
        &self,
        urls: &[PortableCrawlUrl],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY crawl_urls (id, url, source_id, status, discovery_method, parent_url,
                    discovery_context, depth, discovered_at, fetched_at, retry_count, last_error,
                    next_retry_at, etag, last_modified, content_hash, document_id)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in urls.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 300);

            for u in chunk {
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    u.id,
                    Self::escape_copy_value(Some(&u.url)),
                    Self::escape_copy_value(Some(&u.source_id)),
                    Self::escape_copy_value(Some(&u.status)),
                    Self::escape_copy_value(Some(&u.discovery_method)),
                    Self::escape_copy_value(u.parent_url.as_deref()),
                    Self::escape_copy_value(Some(&u.discovery_context)),
                    u.depth,
                    Self::escape_copy_value(Some(&u.discovered_at)),
                    Self::escape_copy_value(u.fetched_at.as_deref()),
                    u.retry_count,
                    Self::escape_copy_value(u.last_error.as_deref()),
                    Self::escape_copy_value(u.next_retry_at.as_deref()),
                    Self::escape_copy_value(u.etag.as_deref()),
                    Self::escape_copy_value(u.last_modified.as_deref()),
                    Self::escape_copy_value(u.content_hash.as_deref()),
                    Self::escape_copy_value(u.document_id.as_deref()),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import crawl requests using COPY protocol.
    pub async fn copy_crawl_requests(
        &self,
        requests: &[PortableCrawlRequest],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY crawl_requests (id, source_id, url, method, request_headers, request_at,
                    response_status, response_headers, response_at, response_size, duration_ms,
                    error, was_conditional, was_not_modified)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in requests.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 400);

            for r in chunk {
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    r.id,
                    Self::escape_copy_value(Some(&r.source_id)),
                    Self::escape_copy_value(Some(&r.url)),
                    Self::escape_copy_value(Some(&r.method)),
                    Self::escape_copy_value(Some(&r.request_headers)),
                    Self::escape_copy_value(Some(&r.request_at)),
                    r.response_status
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "\\N".to_string()),
                    Self::escape_copy_value(Some(&r.response_headers)),
                    Self::escape_copy_value(r.response_at.as_deref()),
                    r.response_size
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "\\N".to_string()),
                    r.duration_ms
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "\\N".to_string()),
                    Self::escape_copy_value(r.error.as_deref()),
                    r.was_conditional,
                    r.was_not_modified,
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import virtual files using COPY protocol.
    pub async fn copy_virtual_files(
        &self,
        files: &[PortableVirtualFile],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY virtual_files (id, document_id, version_id, archive_path, filename,
                    mime_type, file_size, extracted_text, synopsis, tags, status,
                    created_at, updated_at)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in files.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 300);

            for f in chunk {
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&f.id)),
                    Self::escape_copy_value(Some(&f.document_id)),
                    f.version_id,
                    Self::escape_copy_value(Some(&f.archive_path)),
                    Self::escape_copy_value(Some(&f.filename)),
                    Self::escape_copy_value(Some(&f.mime_type)),
                    f.file_size,
                    Self::escape_copy_value(f.extracted_text.as_deref()),
                    Self::escape_copy_value(f.synopsis.as_deref()),
                    Self::escape_copy_value(f.tags.as_deref()),
                    Self::escape_copy_value(Some(&f.status)),
                    Self::escape_copy_value(Some(&f.created_at)),
                    Self::escape_copy_value(Some(&f.updated_at)),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import crawl configs using COPY protocol.
    pub async fn copy_crawl_configs(
        &self,
        configs: &[PortableCrawlConfig],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY crawl_config (source_id, config_hash, updated_at)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in configs.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 100);

            for c in chunk {
                let row = format!(
                    "{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&c.source_id)),
                    Self::escape_copy_value(Some(&c.config_hash)),
                    Self::escape_copy_value(Some(&c.updated_at)),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import config history using COPY protocol.
    pub async fn copy_config_history(
        &self,
        history: &[PortableConfigHistory],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY configuration_history (uuid, created_at, data, format, hash)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in history.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 500);

            for h in chunk {
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&h.uuid)),
                    Self::escape_copy_value(Some(&h.created_at)),
                    Self::escape_copy_value(Some(&h.data)),
                    Self::escape_copy_value(Some(&h.format)),
                    Self::escape_copy_value(Some(&h.hash)),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Import rate limit states using COPY protocol.
    pub async fn copy_rate_limit_states(
        &self,
        states: &[PortableRateLimitState],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        use futures_util::SinkExt;
        use tokio_postgres::CopyInSink;

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let sink: CopyInSink<bytes::Bytes> = client
            .copy_in(
                "COPY rate_limit_state (domain, current_delay_ms, in_backoff, total_requests,
                    rate_limit_hits, updated_at)
                 FROM STDIN WITH (FORMAT text)",
            )
            .await
            .map_err(pg_error)?;

        pin_mut!(sink);

        let mut count = 0;
        let batch_size = 1000;

        for chunk in states.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * 100);

            for s in chunk {
                let row = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&s.domain)),
                    s.current_delay_ms,
                    s.in_backoff,
                    s.total_requests,
                    s.rate_limit_hits,
                    Self::escape_copy_value(Some(&s.updated_at)),
                );
                data.push_str(&row);
                count += 1;
            }

            sink.send(bytes::Bytes::from(data))
                .await
                .map_err(pg_error)?;

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        sink.finish().await.map_err(pg_error)?;

        Ok(count)
    }

    /// Clear specific tables by name.
    /// Uses a single TRUNCATE statement for atomicity and proper FK handling.
    pub async fn clear_tables(&self, tables: &[&str]) -> Result<(), DieselError> {
        if tables.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        // Use TRUNCATE with all tables at once - PostgreSQL handles FK ordering
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

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        // Query in batches to avoid huge IN clauses
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

        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

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

        // Postgres requires separate statements
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
            "CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id)",
            "CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(source_url)",
            "CREATE INDEX IF NOT EXISTS idx_document_versions_doc ON document_versions(document_id)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_urls_source_status ON crawl_urls(source_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_requests_source ON crawl_requests(source_id, request_at)",
        ];

        for stmt in statements {
            diesel::sql_query(stmt).execute(&mut conn).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl DatabaseExporter for PostgresMigrator {
    async fn export_sources(&self) -> Result<Vec<PortableSource>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<SourceRecord> = sources::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableSource::from).collect())
    }

    async fn export_documents(&self) -> Result<Vec<PortableDocument>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<DocumentRecord> = documents::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableDocument::from).collect())
    }

    async fn export_document_versions(&self) -> Result<Vec<PortableDocumentVersion>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<DocumentVersionRecord> = document_versions::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableDocumentVersion::from)
            .collect())
    }

    async fn export_document_pages(&self) -> Result<Vec<PortableDocumentPage>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<DocumentPageRecord> = document_pages::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableDocumentPage::from)
            .collect())
    }

    async fn export_virtual_files(&self) -> Result<Vec<PortableVirtualFile>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<VirtualFileRecord> = virtual_files::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableVirtualFile::from).collect())
    }

    async fn export_crawl_urls(&self) -> Result<Vec<PortableCrawlUrl>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<CrawlUrlRecord> = crawl_urls::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableCrawlUrl::from).collect())
    }

    async fn export_crawl_requests(&self) -> Result<Vec<PortableCrawlRequest>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<CrawlRequestRecord> = crawl_requests::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableCrawlRequest::from)
            .collect())
    }

    async fn export_crawl_configs(&self) -> Result<Vec<PortableCrawlConfig>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<CrawlConfigRecord> = crawl_config::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableCrawlConfig::from).collect())
    }

    async fn export_config_history(&self) -> Result<Vec<PortableConfigHistory>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<ConfigHistoryRecord> =
            configuration_history::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableConfigHistory::from)
            .collect())
    }

    async fn export_rate_limit_states(&self) -> Result<Vec<PortableRateLimitState>, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let records: Vec<RateLimitStateRecord> = rate_limit_state::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableRateLimitState::from)
            .collect())
    }
}

#[async_trait]
impl DatabaseImporter for PostgresMigrator {
    async fn clear_all(&self) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;

        // Delete in correct order to respect foreign key constraints
        diesel::delete(virtual_files::table)
            .execute(&mut conn)
            .await?;
        diesel::delete(document_pages::table)
            .execute(&mut conn)
            .await?;
        diesel::delete(document_versions::table)
            .execute(&mut conn)
            .await?;
        diesel::delete(documents::table).execute(&mut conn).await?;
        diesel::delete(crawl_requests::table)
            .execute(&mut conn)
            .await?;
        diesel::delete(crawl_urls::table).execute(&mut conn).await?;
        diesel::delete(crawl_config::table)
            .execute(&mut conn)
            .await?;
        diesel::delete(sources::table).execute(&mut conn).await?;
        diesel::delete(configuration_history::table)
            .execute(&mut conn)
            .await?;
        diesel::delete(rate_limit_state::table)
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    async fn import_sources(
        &self,
        sources_data: &[PortableSource],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for s in sources_data {
            // Use ON CONFLICT for Postgres upsert
            diesel::sql_query(
                "INSERT INTO sources (id, source_type, name, base_url, metadata, created_at, last_scraped)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (id) DO UPDATE SET
                    source_type = EXCLUDED.source_type,
                    name = EXCLUDED.name,
                    base_url = EXCLUDED.base_url,
                    metadata = EXCLUDED.metadata,
                    created_at = EXCLUDED.created_at,
                    last_scraped = EXCLUDED.last_scraped"
            )
            .bind::<diesel::sql_types::Text, _>(&s.id)
            .bind::<diesel::sql_types::Text, _>(&s.source_type)
            .bind::<diesel::sql_types::Text, _>(&s.name)
            .bind::<diesel::sql_types::Text, _>(&s.base_url)
            .bind::<diesel::sql_types::Text, _>(&s.metadata)
            .bind::<diesel::sql_types::Text, _>(&s.created_at)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&s.last_scraped)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_documents(
        &self,
        documents_data: &[PortableDocument],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;
        let batch_size = self.batch_size;
        let mut in_transaction = false;

        for d in documents_data {
            // Start transaction at beginning of each batch
            if batch_size > 1 && count % batch_size == 0 {
                diesel::sql_query("BEGIN").execute(&mut conn).await?;
                in_transaction = true;
            }

            diesel::sql_query(
                "INSERT INTO documents (id, source_id, title, source_url, extracted_text, status, metadata,
                    created_at, updated_at, synopsis, tags, estimated_date, date_confidence, date_source,
                    manual_date, discovery_method, category_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                 ON CONFLICT (id) DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    title = EXCLUDED.title,
                    source_url = EXCLUDED.source_url,
                    extracted_text = EXCLUDED.extracted_text,
                    status = EXCLUDED.status,
                    metadata = EXCLUDED.metadata,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    synopsis = EXCLUDED.synopsis,
                    tags = EXCLUDED.tags,
                    estimated_date = EXCLUDED.estimated_date,
                    date_confidence = EXCLUDED.date_confidence,
                    date_source = EXCLUDED.date_source,
                    manual_date = EXCLUDED.manual_date,
                    discovery_method = EXCLUDED.discovery_method,
                    category_id = EXCLUDED.category_id"
            )
            .bind::<diesel::sql_types::Text, _>(&d.id)
            .bind::<diesel::sql_types::Text, _>(&d.source_id)
            .bind::<diesel::sql_types::Text, _>(&d.title)
            .bind::<diesel::sql_types::Text, _>(&d.source_url)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.extracted_text)
            .bind::<diesel::sql_types::Text, _>(&d.status)
            .bind::<diesel::sql_types::Text, _>(&d.metadata)
            .bind::<diesel::sql_types::Text, _>(&d.created_at)
            .bind::<diesel::sql_types::Text, _>(&d.updated_at)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.synopsis)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.tags)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.estimated_date)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.date_confidence)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.date_source)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.manual_date)
            .bind::<diesel::sql_types::Text, _>(&d.discovery_method)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&d.category_id)
            .execute(&mut conn)
            .await?;
            count += 1;

            // Commit at end of each batch
            if batch_size > 1 && count % batch_size == 0 && in_transaction {
                diesel::sql_query("COMMIT").execute(&mut conn).await?;
                in_transaction = false;
            }

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        // Commit any remaining records
        if in_transaction {
            diesel::sql_query("COMMIT").execute(&mut conn).await?;
        }

        Ok(count)
    }

    async fn import_document_versions(
        &self,
        versions: &[PortableDocumentVersion],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for v in versions {
            // For SERIAL columns, we need to handle ID insertion specially
            // Use OVERRIDING SYSTEM VALUE to insert specific IDs
            diesel::sql_query(
                "INSERT INTO document_versions (id, document_id, content_hash, file_path, file_size,
                    mime_type, acquired_at, source_url, original_filename, server_date, page_count)
                 OVERRIDING SYSTEM VALUE
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT (id) DO UPDATE SET
                    document_id = EXCLUDED.document_id,
                    content_hash = EXCLUDED.content_hash,
                    file_path = EXCLUDED.file_path,
                    file_size = EXCLUDED.file_size,
                    mime_type = EXCLUDED.mime_type,
                    acquired_at = EXCLUDED.acquired_at,
                    source_url = EXCLUDED.source_url,
                    original_filename = EXCLUDED.original_filename,
                    server_date = EXCLUDED.server_date,
                    page_count = EXCLUDED.page_count"
            )
            .bind::<diesel::sql_types::Integer, _>(v.id)
            .bind::<diesel::sql_types::Text, _>(&v.document_id)
            .bind::<diesel::sql_types::Text, _>(&v.content_hash)
            .bind::<diesel::sql_types::Text, _>(&v.file_path)
            .bind::<diesel::sql_types::Integer, _>(v.file_size)
            .bind::<diesel::sql_types::Text, _>(&v.mime_type)
            .bind::<diesel::sql_types::Text, _>(&v.acquired_at)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&v.source_url)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&v.original_filename)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&v.server_date)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(v.page_count)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        // Reset sequence to max id + 1
        diesel::sql_query(
            "SELECT setval('document_versions_id_seq', COALESCE((SELECT MAX(id) FROM document_versions), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        Ok(count)
    }

    async fn import_document_pages(
        &self,
        pages: &[PortableDocumentPage],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for p in pages {
            diesel::sql_query(
                "INSERT INTO document_pages (id, document_id, version_id, page_number, pdf_text,
                    ocr_text, final_text, ocr_status, created_at, updated_at)
                 OVERRIDING SYSTEM VALUE
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 ON CONFLICT (id) DO UPDATE SET
                    document_id = EXCLUDED.document_id,
                    version_id = EXCLUDED.version_id,
                    page_number = EXCLUDED.page_number,
                    pdf_text = EXCLUDED.pdf_text,
                    ocr_text = EXCLUDED.ocr_text,
                    final_text = EXCLUDED.final_text,
                    ocr_status = EXCLUDED.ocr_status,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at",
            )
            .bind::<diesel::sql_types::Integer, _>(p.id)
            .bind::<diesel::sql_types::Text, _>(&p.document_id)
            .bind::<diesel::sql_types::Integer, _>(p.version_id)
            .bind::<diesel::sql_types::Integer, _>(p.page_number)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&p.pdf_text)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&p.ocr_text)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&p.final_text)
            .bind::<diesel::sql_types::Text, _>(&p.ocr_status)
            .bind::<diesel::sql_types::Text, _>(&p.created_at)
            .bind::<diesel::sql_types::Text, _>(&p.updated_at)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        diesel::sql_query(
            "SELECT setval('document_pages_id_seq', COALESCE((SELECT MAX(id) FROM document_pages), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        Ok(count)
    }

    async fn import_virtual_files(
        &self,
        files: &[PortableVirtualFile],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for f in files {
            diesel::sql_query(
                "INSERT INTO virtual_files (id, document_id, version_id, archive_path, filename,
                    mime_type, file_size, extracted_text, synopsis, tags, status, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                 ON CONFLICT (id) DO UPDATE SET
                    document_id = EXCLUDED.document_id,
                    version_id = EXCLUDED.version_id,
                    archive_path = EXCLUDED.archive_path,
                    filename = EXCLUDED.filename,
                    mime_type = EXCLUDED.mime_type,
                    file_size = EXCLUDED.file_size,
                    extracted_text = EXCLUDED.extracted_text,
                    synopsis = EXCLUDED.synopsis,
                    tags = EXCLUDED.tags,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at"
            )
            .bind::<diesel::sql_types::Text, _>(&f.id)
            .bind::<diesel::sql_types::Text, _>(&f.document_id)
            .bind::<diesel::sql_types::Integer, _>(f.version_id)
            .bind::<diesel::sql_types::Text, _>(&f.archive_path)
            .bind::<diesel::sql_types::Text, _>(&f.filename)
            .bind::<diesel::sql_types::Text, _>(&f.mime_type)
            .bind::<diesel::sql_types::Integer, _>(f.file_size)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&f.extracted_text)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&f.synopsis)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&f.tags)
            .bind::<diesel::sql_types::Text, _>(&f.status)
            .bind::<diesel::sql_types::Text, _>(&f.created_at)
            .bind::<diesel::sql_types::Text, _>(&f.updated_at)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_crawl_urls(
        &self,
        urls: &[PortableCrawlUrl],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for u in urls {
            diesel::sql_query(
                "INSERT INTO crawl_urls (id, url, source_id, status, discovery_method, parent_url,
                    discovery_context, depth, discovered_at, fetched_at, retry_count, last_error,
                    next_retry_at, etag, last_modified, content_hash, document_id)
                 OVERRIDING SYSTEM VALUE
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                 ON CONFLICT (id) DO UPDATE SET
                    url = EXCLUDED.url,
                    source_id = EXCLUDED.source_id,
                    status = EXCLUDED.status,
                    discovery_method = EXCLUDED.discovery_method,
                    parent_url = EXCLUDED.parent_url,
                    discovery_context = EXCLUDED.discovery_context,
                    depth = EXCLUDED.depth,
                    discovered_at = EXCLUDED.discovered_at,
                    fetched_at = EXCLUDED.fetched_at,
                    retry_count = EXCLUDED.retry_count,
                    last_error = EXCLUDED.last_error,
                    next_retry_at = EXCLUDED.next_retry_at,
                    etag = EXCLUDED.etag,
                    last_modified = EXCLUDED.last_modified,
                    content_hash = EXCLUDED.content_hash,
                    document_id = EXCLUDED.document_id",
            )
            .bind::<diesel::sql_types::Integer, _>(u.id)
            .bind::<diesel::sql_types::Text, _>(&u.url)
            .bind::<diesel::sql_types::Text, _>(&u.source_id)
            .bind::<diesel::sql_types::Text, _>(&u.status)
            .bind::<diesel::sql_types::Text, _>(&u.discovery_method)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.parent_url)
            .bind::<diesel::sql_types::Text, _>(&u.discovery_context)
            .bind::<diesel::sql_types::Integer, _>(u.depth)
            .bind::<diesel::sql_types::Text, _>(&u.discovered_at)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.fetched_at)
            .bind::<diesel::sql_types::Integer, _>(u.retry_count)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.last_error)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.next_retry_at)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.etag)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.last_modified)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.content_hash)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&u.document_id)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        diesel::sql_query(
            "SELECT setval('crawl_urls_id_seq', COALESCE((SELECT MAX(id) FROM crawl_urls), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        Ok(count)
    }

    async fn import_crawl_requests(
        &self,
        requests: &[PortableCrawlRequest],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for r in requests {
            diesel::sql_query(
                "INSERT INTO crawl_requests (id, source_id, url, method, request_headers, request_at,
                    response_status, response_headers, response_at, response_size, duration_ms, error,
                    was_conditional, was_not_modified)
                 OVERRIDING SYSTEM VALUE
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                 ON CONFLICT (id) DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    url = EXCLUDED.url,
                    method = EXCLUDED.method,
                    request_headers = EXCLUDED.request_headers,
                    request_at = EXCLUDED.request_at,
                    response_status = EXCLUDED.response_status,
                    response_headers = EXCLUDED.response_headers,
                    response_at = EXCLUDED.response_at,
                    response_size = EXCLUDED.response_size,
                    duration_ms = EXCLUDED.duration_ms,
                    error = EXCLUDED.error,
                    was_conditional = EXCLUDED.was_conditional,
                    was_not_modified = EXCLUDED.was_not_modified"
            )
            .bind::<diesel::sql_types::Integer, _>(r.id)
            .bind::<diesel::sql_types::Text, _>(&r.source_id)
            .bind::<diesel::sql_types::Text, _>(&r.url)
            .bind::<diesel::sql_types::Text, _>(&r.method)
            .bind::<diesel::sql_types::Text, _>(&r.request_headers)
            .bind::<diesel::sql_types::Text, _>(&r.request_at)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(r.response_status)
            .bind::<diesel::sql_types::Text, _>(&r.response_headers)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&r.response_at)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(r.response_size)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(r.duration_ms)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&r.error)
            .bind::<diesel::sql_types::Integer, _>(r.was_conditional)
            .bind::<diesel::sql_types::Integer, _>(r.was_not_modified)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        diesel::sql_query(
            "SELECT setval('crawl_requests_id_seq', COALESCE((SELECT MAX(id) FROM crawl_requests), 0) + 1, false)"
        )
        .execute(&mut conn)
        .await?;

        Ok(count)
    }

    async fn import_crawl_configs(
        &self,
        configs: &[PortableCrawlConfig],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for c in configs {
            diesel::sql_query(
                "INSERT INTO crawl_config (source_id, config_hash, updated_at)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (source_id) DO UPDATE SET
                    config_hash = EXCLUDED.config_hash,
                    updated_at = EXCLUDED.updated_at",
            )
            .bind::<diesel::sql_types::Text, _>(&c.source_id)
            .bind::<diesel::sql_types::Text, _>(&c.config_hash)
            .bind::<diesel::sql_types::Text, _>(&c.updated_at)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_config_history(
        &self,
        history: &[PortableConfigHistory],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for h in history {
            diesel::sql_query(
                "INSERT INTO configuration_history (uuid, created_at, data, format, hash)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (uuid) DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    data = EXCLUDED.data,
                    format = EXCLUDED.format,
                    hash = EXCLUDED.hash",
            )
            .bind::<diesel::sql_types::Text, _>(&h.uuid)
            .bind::<diesel::sql_types::Text, _>(&h.created_at)
            .bind::<diesel::sql_types::Text, _>(&h.data)
            .bind::<diesel::sql_types::Text, _>(&h.format)
            .bind::<diesel::sql_types::Text, _>(&h.hash)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_rate_limit_states(
        &self,
        states: &[PortableRateLimitState],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;
        let mut count = 0;

        for s in states {
            diesel::sql_query(
                "INSERT INTO rate_limit_state (domain, current_delay_ms, in_backoff, total_requests,
                    rate_limit_hits, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (domain) DO UPDATE SET
                    current_delay_ms = EXCLUDED.current_delay_ms,
                    in_backoff = EXCLUDED.in_backoff,
                    total_requests = EXCLUDED.total_requests,
                    rate_limit_hits = EXCLUDED.rate_limit_hits,
                    updated_at = EXCLUDED.updated_at"
            )
            .bind::<diesel::sql_types::Text, _>(&s.domain)
            .bind::<diesel::sql_types::Integer, _>(s.current_delay_ms)
            .bind::<diesel::sql_types::Integer, _>(s.in_backoff)
            .bind::<diesel::sql_types::Integer, _>(s.total_requests)
            .bind::<diesel::sql_types::Integer, _>(s.rate_limit_hits)
            .bind::<diesel::sql_types::Text, _>(&s.updated_at)
            .execute(&mut conn)
            .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }
}
