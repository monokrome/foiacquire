//! DatabaseImporter trait implementation for PostgreSQL.

use async_trait::async_trait;
use diesel_async::RunQueryDsl;

use super::PostgresMigrator;
use crate::repository::migration::{
    DatabaseImporter, PortableConfigHistory, PortableCrawlConfig, PortableCrawlRequest,
    PortableCrawlUrl, PortableDocument, PortableDocumentPage, PortableDocumentVersion,
    PortableRateLimitState, PortableSource, PortableVirtualFile, ProgressCallback,
};
use crate::repository::util::to_diesel_error;
use crate::repository::DieselError;
use crate::schema::*;

#[async_trait]
impl DatabaseImporter for PostgresMigrator {
    async fn clear_all(&self) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await.map_err(to_diesel_error)?;

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

            if batch_size > 1 && count % batch_size == 0 && in_transaction {
                diesel::sql_query("COMMIT").execute(&mut conn).await?;
                in_transaction = false;
            }

            if let Some(ref cb) = progress {
                cb(count);
            }
        }

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
            diesel::sql_query(
                "INSERT INTO document_versions (id, document_id, content_hash, content_hash_blake3,
                    file_path, file_size, mime_type, acquired_at, source_url, original_filename,
                    server_date, page_count)
                 OVERRIDING SYSTEM VALUE
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                 ON CONFLICT (id) DO UPDATE SET
                    document_id = EXCLUDED.document_id,
                    content_hash = EXCLUDED.content_hash,
                    content_hash_blake3 = EXCLUDED.content_hash_blake3,
                    file_path = EXCLUDED.file_path,
                    file_size = EXCLUDED.file_size,
                    mime_type = EXCLUDED.mime_type,
                    acquired_at = EXCLUDED.acquired_at,
                    source_url = EXCLUDED.source_url,
                    original_filename = EXCLUDED.original_filename,
                    server_date = EXCLUDED.server_date,
                    page_count = EXCLUDED.page_count",
            )
            .bind::<diesel::sql_types::Integer, _>(v.id)
            .bind::<diesel::sql_types::Text, _>(&v.document_id)
            .bind::<diesel::sql_types::Text, _>(&v.content_hash)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&v.content_hash_blake3)
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
