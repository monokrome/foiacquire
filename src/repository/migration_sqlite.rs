//! SQLite implementation of database migration traits.

use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::diesel_models::*;
use super::diesel_pool::AsyncSqlitePool;
use super::migration::{
    DatabaseExporter, DatabaseImporter, PortableConfigHistory, PortableCrawlConfig,
    PortableCrawlRequest, PortableCrawlUrl, PortableDocument, PortableDocumentPage,
    PortableDocumentVersion, PortableRateLimitState, PortableSource, PortableVirtualFile,
    ProgressCallback,
};
use super::DieselError;
use crate::schema::*;

/// SQLite database migrator.
pub struct SqliteMigrator {
    pool: AsyncSqlitePool,
}

impl SqliteMigrator {
    /// Create a new SQLite migrator.
    pub fn new(pool: AsyncSqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DatabaseExporter for SqliteMigrator {
    async fn export_sources(&self) -> Result<Vec<PortableSource>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<SourceRecord> = sources::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableSource::from).collect())
    }

    async fn export_documents(&self) -> Result<Vec<PortableDocument>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<DocumentRecord> = documents::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableDocument::from).collect())
    }

    async fn export_document_versions(&self) -> Result<Vec<PortableDocumentVersion>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<DocumentVersionRecord> = document_versions::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableDocumentVersion::from)
            .collect())
    }

    async fn export_document_pages(&self) -> Result<Vec<PortableDocumentPage>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<DocumentPageRecord> = document_pages::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableDocumentPage::from)
            .collect())
    }

    async fn export_virtual_files(&self) -> Result<Vec<PortableVirtualFile>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<VirtualFileRecord> = virtual_files::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableVirtualFile::from).collect())
    }

    async fn export_crawl_urls(&self) -> Result<Vec<PortableCrawlUrl>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<CrawlUrlRecord> = crawl_urls::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableCrawlUrl::from).collect())
    }

    async fn export_crawl_requests(&self) -> Result<Vec<PortableCrawlRequest>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<CrawlRequestRecord> = crawl_requests::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableCrawlRequest::from)
            .collect())
    }

    async fn export_crawl_configs(&self) -> Result<Vec<PortableCrawlConfig>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<CrawlConfigRecord> = crawl_config::table.load(&mut conn).await?;
        Ok(records.into_iter().map(PortableCrawlConfig::from).collect())
    }

    async fn export_config_history(&self) -> Result<Vec<PortableConfigHistory>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<ConfigHistoryRecord> =
            configuration_history::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableConfigHistory::from)
            .collect())
    }

    async fn export_rate_limit_states(&self) -> Result<Vec<PortableRateLimitState>, DieselError> {
        let mut conn = self.pool.get().await?;
        let records: Vec<RateLimitStateRecord> = rate_limit_state::table.load(&mut conn).await?;
        Ok(records
            .into_iter()
            .map(PortableRateLimitState::from)
            .collect())
    }
}

#[async_trait]
impl DatabaseImporter for SqliteMigrator {
    async fn clear_all(&self) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

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
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for s in sources_data {
            diesel::replace_into(sources::table)
                .values((
                    sources::id.eq(&s.id),
                    sources::source_type.eq(&s.source_type),
                    sources::name.eq(&s.name),
                    sources::base_url.eq(&s.base_url),
                    sources::metadata.eq(&s.metadata),
                    sources::created_at.eq(&s.created_at),
                    sources::last_scraped.eq(&s.last_scraped),
                ))
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
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for d in documents_data {
            diesel::replace_into(documents::table)
                .values((
                    documents::id.eq(&d.id),
                    documents::source_id.eq(&d.source_id),
                    documents::title.eq(&d.title),
                    documents::source_url.eq(&d.source_url),
                    documents::extracted_text.eq(&d.extracted_text),
                    documents::status.eq(&d.status),
                    documents::metadata.eq(&d.metadata),
                    documents::created_at.eq(&d.created_at),
                    documents::updated_at.eq(&d.updated_at),
                    documents::synopsis.eq(&d.synopsis),
                    documents::tags.eq(&d.tags),
                    documents::estimated_date.eq(&d.estimated_date),
                    documents::date_confidence.eq(&d.date_confidence),
                    documents::date_source.eq(&d.date_source),
                    documents::manual_date.eq(&d.manual_date),
                    documents::discovery_method.eq(&d.discovery_method),
                    documents::category_id.eq(&d.category_id),
                ))
                .execute(&mut conn)
                .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_document_versions(
        &self,
        versions: &[PortableDocumentVersion],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for v in versions {
            diesel::replace_into(document_versions::table)
                .values((
                    document_versions::id.eq(v.id),
                    document_versions::document_id.eq(&v.document_id),
                    document_versions::content_hash.eq(&v.content_hash),
                    document_versions::content_hash_blake3.eq(&v.content_hash_blake3),
                    document_versions::file_path.eq(&v.file_path),
                    document_versions::file_size.eq(v.file_size),
                    document_versions::mime_type.eq(&v.mime_type),
                    document_versions::acquired_at.eq(&v.acquired_at),
                    document_versions::source_url.eq(&v.source_url),
                    document_versions::original_filename.eq(&v.original_filename),
                    document_versions::server_date.eq(&v.server_date),
                    document_versions::page_count.eq(v.page_count),
                ))
                .execute(&mut conn)
                .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_document_pages(
        &self,
        pages: &[PortableDocumentPage],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for p in pages {
            diesel::replace_into(document_pages::table)
                .values((
                    document_pages::id.eq(p.id),
                    document_pages::document_id.eq(&p.document_id),
                    document_pages::version_id.eq(p.version_id),
                    document_pages::page_number.eq(p.page_number),
                    document_pages::pdf_text.eq(&p.pdf_text),
                    document_pages::ocr_text.eq(&p.ocr_text),
                    document_pages::final_text.eq(&p.final_text),
                    document_pages::ocr_status.eq(&p.ocr_status),
                    document_pages::created_at.eq(&p.created_at),
                    document_pages::updated_at.eq(&p.updated_at),
                ))
                .execute(&mut conn)
                .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_virtual_files(
        &self,
        files: &[PortableVirtualFile],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for f in files {
            diesel::replace_into(virtual_files::table)
                .values((
                    virtual_files::id.eq(&f.id),
                    virtual_files::document_id.eq(&f.document_id),
                    virtual_files::version_id.eq(f.version_id),
                    virtual_files::archive_path.eq(&f.archive_path),
                    virtual_files::filename.eq(&f.filename),
                    virtual_files::mime_type.eq(&f.mime_type),
                    virtual_files::file_size.eq(f.file_size),
                    virtual_files::extracted_text.eq(&f.extracted_text),
                    virtual_files::synopsis.eq(&f.synopsis),
                    virtual_files::tags.eq(&f.tags),
                    virtual_files::status.eq(&f.status),
                    virtual_files::created_at.eq(&f.created_at),
                    virtual_files::updated_at.eq(&f.updated_at),
                ))
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
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for u in urls {
            diesel::replace_into(crawl_urls::table)
                .values((
                    crawl_urls::id.eq(u.id),
                    crawl_urls::url.eq(&u.url),
                    crawl_urls::source_id.eq(&u.source_id),
                    crawl_urls::status.eq(&u.status),
                    crawl_urls::discovery_method.eq(&u.discovery_method),
                    crawl_urls::parent_url.eq(&u.parent_url),
                    crawl_urls::discovery_context.eq(&u.discovery_context),
                    crawl_urls::depth.eq(u.depth),
                    crawl_urls::discovered_at.eq(&u.discovered_at),
                    crawl_urls::fetched_at.eq(&u.fetched_at),
                    crawl_urls::retry_count.eq(u.retry_count),
                    crawl_urls::last_error.eq(&u.last_error),
                    crawl_urls::next_retry_at.eq(&u.next_retry_at),
                    crawl_urls::etag.eq(&u.etag),
                    crawl_urls::last_modified.eq(&u.last_modified),
                    crawl_urls::content_hash.eq(&u.content_hash),
                    crawl_urls::document_id.eq(&u.document_id),
                ))
                .execute(&mut conn)
                .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_crawl_requests(
        &self,
        requests: &[PortableCrawlRequest],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for r in requests {
            diesel::replace_into(crawl_requests::table)
                .values((
                    crawl_requests::id.eq(r.id),
                    crawl_requests::source_id.eq(&r.source_id),
                    crawl_requests::url.eq(&r.url),
                    crawl_requests::method.eq(&r.method),
                    crawl_requests::request_headers.eq(&r.request_headers),
                    crawl_requests::request_at.eq(&r.request_at),
                    crawl_requests::response_status.eq(r.response_status),
                    crawl_requests::response_headers.eq(&r.response_headers),
                    crawl_requests::response_at.eq(&r.response_at),
                    crawl_requests::response_size.eq(r.response_size),
                    crawl_requests::duration_ms.eq(r.duration_ms),
                    crawl_requests::error.eq(&r.error),
                    crawl_requests::was_conditional.eq(r.was_conditional),
                    crawl_requests::was_not_modified.eq(r.was_not_modified),
                ))
                .execute(&mut conn)
                .await?;
            count += 1;
            if let Some(ref cb) = progress {
                cb(count);
            }
        }

        Ok(count)
    }

    async fn import_crawl_configs(
        &self,
        configs: &[PortableCrawlConfig],
        progress: Option<ProgressCallback>,
    ) -> Result<usize, DieselError> {
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for c in configs {
            diesel::replace_into(crawl_config::table)
                .values((
                    crawl_config::source_id.eq(&c.source_id),
                    crawl_config::config_hash.eq(&c.config_hash),
                    crawl_config::updated_at.eq(&c.updated_at),
                ))
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
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for h in history {
            diesel::replace_into(configuration_history::table)
                .values((
                    configuration_history::uuid.eq(&h.uuid),
                    configuration_history::created_at.eq(&h.created_at),
                    configuration_history::data.eq(&h.data),
                    configuration_history::format.eq(&h.format),
                    configuration_history::hash.eq(&h.hash),
                ))
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
        let mut conn = self.pool.get().await?;
        let mut count = 0;

        for s in states {
            diesel::replace_into(rate_limit_state::table)
                .values((
                    rate_limit_state::domain.eq(&s.domain),
                    rate_limit_state::current_delay_ms.eq(s.current_delay_ms),
                    rate_limit_state::in_backoff.eq(s.in_backoff),
                    rate_limit_state::total_requests.eq(s.total_requests),
                    rate_limit_state::rate_limit_hits.eq(s.rate_limit_hits),
                    rate_limit_state::updated_at.eq(&s.updated_at),
                ))
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
