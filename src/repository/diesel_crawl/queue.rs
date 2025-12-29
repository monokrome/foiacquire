//! Queue and claiming operations for the crawl repository.

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};

use super::DieselCrawlRepository;
use crate::models::{CrawlUrl, UrlStatus};
use crate::repository::diesel_models::CrawlUrlRecord;
use crate::repository::pool::DieselError;
use crate::schema::crawl_urls;
use crate::with_conn;

impl DieselCrawlRepository {
    /// Get URLs that need to be fetched.
    pub async fn get_pending_urls(
        &self,
        source_id: &str,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let limit = limit as i64;
        with_conn!(self.pool, conn, {
            crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(
                    crawl_urls::status
                        .eq("discovered")
                        .or(crawl_urls::status.eq("fetching")),
                )
                .order((crawl_urls::depth.asc(), crawl_urls::discovered_at.asc()))
                .limit(limit)
                .load::<CrawlUrlRecord>(&mut conn)
                .await
                .map(|records| records.into_iter().map(CrawlUrl::from).collect())
        })
    }

    /// Atomically claim a pending URL for processing.
    pub async fn claim_pending_url(
        &self,
        source_id: Option<&str>,
    ) -> Result<Option<CrawlUrl>, DieselError> {
        let source_id = source_id.map(|s| s.to_string());

        with_conn!(self.pool, conn, {
            conn.transaction(|conn| {
                let source_id = source_id.clone();
                Box::pin(async move {
                    let mut query = crawl_urls::table
                        .filter(crawl_urls::status.eq("discovered"))
                        .order((crawl_urls::depth.asc(), crawl_urls::discovered_at.asc()))
                        .limit(1)
                        .into_boxed();

                    if let Some(ref sid) = source_id {
                        query = query.filter(crawl_urls::source_id.eq(sid));
                    }

                    let record: Option<CrawlUrlRecord> = query.first(conn).await.optional()?;

                    if let Some(record) = record {
                        diesel::update(
                            crawl_urls::table
                                .filter(crawl_urls::source_id.eq(&record.source_id))
                                .filter(crawl_urls::url.eq(&record.url)),
                        )
                        .set(crawl_urls::status.eq("fetching"))
                        .execute(conn)
                        .await?;

                        let mut crawl_url = CrawlUrl::from(record);
                        crawl_url.status = UrlStatus::Fetching;
                        Ok(Some(crawl_url))
                    } else {
                        Ok(None)
                    }
                })
            })
            .await
        })
    }

    /// Get failed URLs that are ready for retry.
    pub async fn get_retryable_urls(
        &self,
        source_id: &str,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let limit = limit as i64;
        let now = Utc::now().to_rfc3339();

        with_conn!(self.pool, conn, {
            // First, update any URLs whose retry time has passed to 'discovered'
            diesel::update(
                crawl_urls::table
                    .filter(crawl_urls::source_id.eq(source_id))
                    .filter(crawl_urls::status.eq("failed"))
                    .filter(crawl_urls::next_retry_at.le(&now)),
            )
            .set(crawl_urls::status.eq("discovered"))
            .execute(&mut conn)
            .await?;

            // Then fetch URLs that are now ready
            crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(crawl_urls::status.eq("discovered"))
                .filter(crawl_urls::retry_count.gt(0))
                .order((crawl_urls::depth.asc(), crawl_urls::discovered_at.asc()))
                .limit(limit)
                .load::<CrawlUrlRecord>(&mut conn)
                .await
                .map(|records| records.into_iter().map(CrawlUrl::from).collect())
        })
    }
}
