//! COPY protocol bulk import methods for PostgreSQL.
//!
//! These methods use PostgreSQL's COPY protocol for fast bulk imports.
//! Much faster than individual INSERT statements for large datasets.

use futures_util::pin_mut;
use futures_util::SinkExt;
use tokio_postgres::CopyInSink;
use tokio_postgres::NoTls;

use super::PostgresMigrator;
use crate::repository::migration::{
    PortableConfigHistory, PortableCrawlConfig, PortableCrawlRequest, PortableCrawlUrl,
    PortableDocument, PortableDocumentPage, PortableDocumentVersion, PortableRateLimitState,
    PortableSource, PortableVirtualFile, ProgressCallback,
};
use crate::repository::util::pg_to_diesel_error as pg_error;
use crate::repository::DieselError;

impl PostgresMigrator {
    /// Create a COPY sink for the given table and columns.
    async fn create_copy_sink(
        &self,
        copy_sql: &str,
    ) -> Result<CopyInSink<bytes::Bytes>, DieselError> {
        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .map_err(pg_error)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        client.copy_in(copy_sql).await.map_err(pg_error)
    }

    /// Execute a batched COPY operation with progress reporting.
    async fn copy_batched<T, F>(
        &self,
        copy_sql: &str,
        items: &[T],
        batch_size: usize,
        capacity_per_item: usize,
        format_row: F,
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError>
    where
        F: Fn(&T) -> String,
    {
        let sink = self.create_copy_sink(copy_sql).await?;
        pin_mut!(sink);

        let mut count = 0;

        for chunk in items.chunks(batch_size) {
            let mut data = String::with_capacity(chunk.len() * capacity_per_item);

            for item in chunk {
                data.push_str(&format_row(item));
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
    /// Import documents using COPY protocol (much faster than INSERT).
    pub async fn copy_documents(
        &self,
        documents: &[PortableDocument],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY documents (id, source_id, title, source_url, extracted_text, status, metadata,
                created_at, updated_at, synopsis, tags, estimated_date, date_confidence, date_source,
                manual_date, discovery_method, category_id)
             FROM STDIN WITH (FORMAT text)",
            documents,
            1000,
            500,
            |d| {
                format!(
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
                )
            },
            progress,
        )
        .await
    }

    /// Import sources using COPY protocol.
    pub async fn copy_sources(
        &self,
        sources: &[PortableSource],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY sources (id, source_type, name, base_url, metadata, created_at, last_scraped)
             FROM STDIN WITH (FORMAT text)",
            sources,
            1000,
            200,
            |s| {
                format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&s.id)),
                    Self::escape_copy_value(Some(&s.source_type)),
                    Self::escape_copy_value(Some(&s.name)),
                    Self::escape_copy_value(Some(&s.base_url)),
                    Self::escape_copy_value(Some(&s.metadata)),
                    Self::escape_copy_value(Some(&s.created_at)),
                    Self::escape_copy_value(s.last_scraped.as_deref()),
                )
            },
            progress,
        )
        .await
    }

    /// Import document versions using COPY protocol.
    pub async fn copy_document_versions(
        &self,
        versions: &[PortableDocumentVersion],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY document_versions (id, document_id, content_hash, content_hash_blake3,
                file_path, file_size, mime_type, acquired_at, source_url, original_filename,
                server_date, page_count)
             FROM STDIN WITH (FORMAT text)",
            versions,
            1000,
            200,
            |v| {
                format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    v.id,
                    Self::escape_copy_value(Some(&v.document_id)),
                    Self::escape_copy_value(Some(&v.content_hash)),
                    Self::escape_copy_value(v.content_hash_blake3.as_deref()),
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
                )
            },
            progress,
        )
        .await
    }

    /// Import document pages using COPY protocol.
    pub async fn copy_document_pages(
        &self,
        pages: &[PortableDocumentPage],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY document_pages (id, document_id, version_id, page_number, pdf_text,
                ocr_text, final_text, ocr_status, created_at, updated_at)
             FROM STDIN WITH (FORMAT text)",
            pages,
            1000,
            500,
            |p| {
                format!(
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
                )
            },
            progress,
        )
        .await
    }

    /// Import crawl URLs using COPY protocol.
    pub async fn copy_crawl_urls(
        &self,
        urls: &[PortableCrawlUrl],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY crawl_urls (id, url, source_id, status, discovery_method, parent_url,
                discovery_context, depth, discovered_at, fetched_at, retry_count, last_error,
                next_retry_at, etag, last_modified, content_hash, document_id)
             FROM STDIN WITH (FORMAT text)",
            urls,
            1000,
            300,
            |u| {
                format!(
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
                )
            },
            progress,
        )
        .await
    }

    /// Import crawl requests using COPY protocol.
    pub async fn copy_crawl_requests(
        &self,
        requests: &[PortableCrawlRequest],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY crawl_requests (id, source_id, url, method, request_headers, request_at,
                response_status, response_headers, response_at, response_size, duration_ms,
                error, was_conditional, was_not_modified)
             FROM STDIN WITH (FORMAT text)",
            requests,
            1000,
            400,
            |r| {
                format!(
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
                )
            },
            progress,
        )
        .await
    }

    /// Import virtual files using COPY protocol.
    pub async fn copy_virtual_files(
        &self,
        files: &[PortableVirtualFile],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY virtual_files (id, document_id, version_id, archive_path, filename,
                mime_type, file_size, extracted_text, synopsis, tags, status,
                created_at, updated_at)
             FROM STDIN WITH (FORMAT text)",
            files,
            1000,
            300,
            |f| {
                format!(
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
                )
            },
            progress,
        )
        .await
    }

    /// Import crawl configs using COPY protocol.
    pub async fn copy_crawl_configs(
        &self,
        configs: &[PortableCrawlConfig],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY crawl_config (source_id, config_hash, updated_at)
             FROM STDIN WITH (FORMAT text)",
            configs,
            1000,
            100,
            |c| {
                format!(
                    "{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&c.source_id)),
                    Self::escape_copy_value(Some(&c.config_hash)),
                    Self::escape_copy_value(Some(&c.updated_at)),
                )
            },
            progress,
        )
        .await
    }

    /// Import config history using COPY protocol.
    pub async fn copy_config_history(
        &self,
        history: &[PortableConfigHistory],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY configuration_history (uuid, created_at, data, format, hash)
             FROM STDIN WITH (FORMAT text)",
            history,
            1000,
            500,
            |h| {
                format!(
                    "{}\t{}\t{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&h.uuid)),
                    Self::escape_copy_value(Some(&h.created_at)),
                    Self::escape_copy_value(Some(&h.data)),
                    Self::escape_copy_value(Some(&h.format)),
                    Self::escape_copy_value(Some(&h.hash)),
                )
            },
            progress,
        )
        .await
    }

    /// Import rate limit states using COPY protocol.
    pub async fn copy_rate_limit_states(
        &self,
        states: &[PortableRateLimitState],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        self.copy_batched(
            "COPY rate_limit_state (domain, current_delay_ms, in_backoff, total_requests,
                rate_limit_hits, updated_at)
             FROM STDIN WITH (FORMAT text)",
            states,
            1000,
            100,
            |s| {
                format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\n",
                    Self::escape_copy_value(Some(&s.domain)),
                    s.current_delay_ms,
                    s.in_backoff,
                    s.total_requests,
                    s.rate_limit_hits,
                    Self::escape_copy_value(Some(&s.updated_at)),
                )
            },
            progress,
        )
        .await
    }
}
