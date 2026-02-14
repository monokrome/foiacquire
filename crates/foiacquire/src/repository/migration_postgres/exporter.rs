//! DatabaseExporter trait implementation for PostgreSQL.

use async_trait::async_trait;
use diesel_async::RunQueryDsl;

use super::PostgresMigrator;
use crate::repository::migration::{
    DatabaseExporter, PortableConfigHistory, PortableCrawlConfig, PortableCrawlRequest,
    PortableCrawlUrl, PortableDocument, PortableDocumentPage, PortableDocumentVersion,
    PortableRateLimitState, PortableSource, PortableVirtualFile,
};
use crate::repository::models::*;
use crate::repository::util::to_diesel_error;
use crate::repository::DieselError;
use crate::schema::*;

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
