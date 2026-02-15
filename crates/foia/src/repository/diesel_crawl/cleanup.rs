//! Cleanup operations for the crawl repository.

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::DieselCrawlRepository;
use crate::repository::pool::DieselError;
use crate::schema::{crawl_config, crawl_requests, crawl_urls};
use crate::with_conn;

impl DieselCrawlRepository {
    /// Clear pending crawl state for a source (keeps fetched URLs).
    #[allow(dead_code)]
    pub async fn clear_source(&self, source_id: &str) -> Result<(), DieselError> {
        with_conn!(self.pool, conn, {
            diesel::delete(
                crawl_urls::table
                    .filter(crawl_urls::source_id.eq(source_id))
                    .filter(
                        crawl_urls::status
                            .eq("discovered")
                            .or(crawl_urls::status.eq("fetching"))
                            .or(crawl_urls::status.eq("failed")),
                    ),
            )
            .execute(&mut conn)
            .await?;

            diesel::delete(crawl_requests::table.filter(crawl_requests::source_id.eq(source_id)))
                .execute(&mut conn)
                .await?;

            Ok(())
        })
    }

    /// Clear ALL crawl state for a source.
    pub async fn clear_source_all(&self, source_id: &str) -> Result<(), DieselError> {
        with_conn!(self.pool, conn, {
            diesel::delete(crawl_urls::table.filter(crawl_urls::source_id.eq(source_id)))
                .execute(&mut conn)
                .await?;

            diesel::delete(crawl_requests::table.filter(crawl_requests::source_id.eq(source_id)))
                .execute(&mut conn)
                .await?;

            diesel::delete(crawl_config::table.filter(crawl_config::source_id.eq(source_id)))
                .execute(&mut conn)
                .await?;

            Ok(())
        })
    }
}
