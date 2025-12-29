//! URL CRUD operations for the crawl repository.

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::DieselCrawlRepository;
use crate::models::CrawlUrl;
use crate::repository::diesel_models::CrawlUrlRecord;
use crate::repository::pool::DieselError;
use crate::schema::crawl_urls;
use crate::with_conn;

impl DieselCrawlRepository {
    /// Add a discovered URL if not already known.
    pub async fn add_url(&self, crawl_url: &CrawlUrl) -> Result<bool, DieselError> {
        let status = crawl_url.status.as_str().to_string();
        let discovery_method = crawl_url.discovery_method.as_str().to_string();
        let discovery_context = serde_json::to_string(&crawl_url.discovery_context)
            .unwrap_or_else(|_| "{}".to_string());
        let depth = crawl_url.depth as i32;
        let discovered_at = crawl_url.discovered_at.to_rfc3339();
        let retry_count = crawl_url.retry_count as i32;
        let fetched_at = crawl_url.fetched_at.map(|dt| dt.to_rfc3339());
        let next_retry_at = crawl_url.next_retry_at.map(|dt| dt.to_rfc3339());

        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let exists: i64 = crawl_urls::table
                .filter(crawl_urls::source_id.eq(&crawl_url.source_id))
                .filter(crawl_urls::url.eq(&crawl_url.url))
                .select(count_star())
                .first(&mut conn)
                .await?;

            if exists > 0 {
                return Ok(false);
            }

            diesel::insert_into(crawl_urls::table)
                .values((
                    crawl_urls::url.eq(&crawl_url.url),
                    crawl_urls::source_id.eq(&crawl_url.source_id),
                    crawl_urls::status.eq(&status),
                    crawl_urls::discovery_method.eq(&discovery_method),
                    crawl_urls::parent_url.eq(&crawl_url.parent_url),
                    crawl_urls::discovery_context.eq(&discovery_context),
                    crawl_urls::depth.eq(depth),
                    crawl_urls::discovered_at.eq(&discovered_at),
                    crawl_urls::fetched_at.eq(&fetched_at),
                    crawl_urls::retry_count.eq(retry_count),
                    crawl_urls::last_error.eq(&crawl_url.last_error),
                    crawl_urls::next_retry_at.eq(&next_retry_at),
                    crawl_urls::etag.eq(&crawl_url.etag),
                    crawl_urls::last_modified.eq(&crawl_url.last_modified),
                    crawl_urls::content_hash.eq(&crawl_url.content_hash),
                    crawl_urls::document_id.eq(&crawl_url.document_id),
                ))
                .execute(&mut conn)
                .await?;

            Ok(true)
        })
    }

    /// Get a URL by source and URL string.
    pub async fn get_url(
        &self,
        source_id: &str,
        url: &str,
    ) -> Result<Option<CrawlUrl>, DieselError> {
        with_conn!(self.pool, conn, {
            crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(crawl_urls::url.eq(url))
                .first::<CrawlUrlRecord>(&mut conn)
                .await
                .optional()
                .map(|r| r.map(CrawlUrl::from))
        })
    }

    /// Check if a URL exists.
    #[allow(dead_code)]
    pub async fn url_exists(&self, source_id: &str, url: &str) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(crawl_urls::url.eq(url))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count > 0)
        })
    }

    /// Update a URL's status and metadata.
    pub async fn update_url(&self, crawl_url: &CrawlUrl) -> Result<(), DieselError> {
        let status = crawl_url.status.as_str().to_string();
        let fetched_at = crawl_url.fetched_at.map(|dt| dt.to_rfc3339());
        let next_retry_at = crawl_url.next_retry_at.map(|dt| dt.to_rfc3339());
        let retry_count = crawl_url.retry_count as i32;

        with_conn!(self.pool, conn, {
            diesel::update(
                crawl_urls::table
                    .filter(crawl_urls::source_id.eq(&crawl_url.source_id))
                    .filter(crawl_urls::url.eq(&crawl_url.url)),
            )
            .set((
                crawl_urls::status.eq(&status),
                crawl_urls::fetched_at.eq(&fetched_at),
                crawl_urls::retry_count.eq(retry_count),
                crawl_urls::last_error.eq(&crawl_url.last_error),
                crawl_urls::next_retry_at.eq(&next_retry_at),
                crawl_urls::etag.eq(&crawl_url.etag),
                crawl_urls::last_modified.eq(&crawl_url.last_modified),
                crawl_urls::content_hash.eq(&crawl_url.content_hash),
                crawl_urls::document_id.eq(&crawl_url.document_id),
            ))
            .execute(&mut conn)
            .await?;

            Ok(())
        })
    }

    /// Get URLs needing refresh (older than cutoff date).
    pub async fn get_urls_needing_refresh(
        &self,
        source_id: &str,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let cutoff_str = cutoff.to_rfc3339();
        let limit = limit as i64;

        with_conn!(self.pool, conn, {
            crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(crawl_urls::status.eq("fetched"))
                .filter(crawl_urls::fetched_at.lt(&cutoff_str))
                .order(crawl_urls::fetched_at.asc())
                .limit(limit)
                .load::<CrawlUrlRecord>(&mut conn)
                .await
                .map(|records| records.into_iter().map(CrawlUrl::from).collect())
        })
    }

    /// Mark a URL for refresh by resetting its status to discovered.
    pub async fn mark_url_for_refresh(
        &self,
        source_id: &str,
        url: &str,
    ) -> Result<(), DieselError> {
        with_conn!(self.pool, conn, {
            diesel::update(
                crawl_urls::table
                    .filter(crawl_urls::source_id.eq(source_id))
                    .filter(crawl_urls::url.eq(url)),
            )
            .set(crawl_urls::status.eq("discovered"))
            .execute(&mut conn)
            .await?;
            Ok(())
        })
    }

    /// Get failed URLs.
    pub async fn get_failed_urls(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let limit = limit as i64;

        with_conn!(self.pool, conn, {
            let mut query = crawl_urls::table
                .filter(
                    crawl_urls::status
                        .eq("failed")
                        .or(crawl_urls::status.eq("exhausted")),
                )
                .order(crawl_urls::fetched_at.desc())
                .limit(limit)
                .into_boxed();

            if let Some(sid) = source_id {
                query = query.filter(crawl_urls::source_id.eq(sid));
            }

            query
                .load::<CrawlUrlRecord>(&mut conn)
                .await
                .map(|records| records.into_iter().map(CrawlUrl::from).collect())
        })
    }

    /// Count URLs for a source.
    pub async fn count_by_source(&self, source_id: &str) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count as u64)
        })
    }
}
