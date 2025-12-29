//! Config hash management operations for the crawl repository.

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::DieselCrawlRepository;
use crate::repository::pool::DieselError;
use crate::schema::crawl_config;
use crate::with_conn;

impl DieselCrawlRepository {
    /// Check if config has changed since last crawl.
    pub async fn check_config_changed(
        &self,
        source_id: &str,
        current_hash: &str,
    ) -> Result<bool, DieselError> {
        with_conn!(self.pool, conn, {
            let stored_hash: Option<String> = crawl_config::table
                .filter(crawl_config::source_id.eq(source_id))
                .select(crawl_config::config_hash)
                .first(&mut conn)
                .await
                .optional()?;

            Ok(stored_hash.as_deref() != Some(current_hash))
        })
    }

    /// Store the current config hash.
    pub async fn store_config_hash(
        &self,
        source_id: &str,
        config_hash: &str,
    ) -> Result<(), DieselError> {
        let now = Utc::now().to_rfc3339();

        with_conn!(self.pool, conn, {
            // Try to update first
            let updated =
                diesel::update(crawl_config::table.filter(crawl_config::source_id.eq(source_id)))
                    .set((
                        crawl_config::config_hash.eq(config_hash),
                        crawl_config::updated_at.eq(&now),
                    ))
                    .execute(&mut conn)
                    .await?;

            // If no row was updated, insert
            if updated == 0 {
                diesel::insert_into(crawl_config::table)
                    .values((
                        crawl_config::source_id.eq(source_id),
                        crawl_config::config_hash.eq(config_hash),
                        crawl_config::updated_at.eq(&now),
                    ))
                    .execute(&mut conn)
                    .await?;
            }

            Ok(())
        })
    }
}
