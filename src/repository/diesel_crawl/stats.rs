//! Statistics and analytics operations for the crawl repository.

use std::collections::HashMap;

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::{CrawlState, CrawlStats, DieselCrawlRepository, RequestStats, StatusCount};
use crate::models::CrawlUrl;
use crate::repository::diesel_models::CrawlUrlRecord;
use crate::repository::pool::DieselError;
use crate::schema::crawl_urls;
use crate::with_conn;

impl DieselCrawlRepository {
    /// Count URLs by status for a source.
    pub async fn count_by_status(
        &self,
        source_id: &str,
    ) -> Result<HashMap<String, u64>, DieselError> {
        with_conn!(self.pool, conn, {
            let counts: Vec<StatusCount> = diesel::sql_query(
                "SELECT status, COUNT(*) as count FROM crawl_urls WHERE source_id = $1 GROUP BY status"
            )
            .bind::<diesel::sql_types::Text, _>(source_id)
            .load(&mut conn)
            .await?;

            Ok(counts
                .into_iter()
                .map(|sc| (sc.status, sc.count as u64))
                .collect())
        })
    }

    /// Count pending URLs for a source.
    #[allow(dead_code)]
    pub async fn count_pending(&self, source_id: &str) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(
                    crawl_urls::status
                        .eq("discovered")
                        .or(crawl_urls::status.eq("fetching")),
                )
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count as u64)
        })
    }

    /// Get overall crawl state for a source.
    pub async fn get_crawl_state(&self, source_id: &str) -> Result<CrawlState, DieselError> {
        let counts = self.count_by_status(source_id).await?;

        let urls_discovered = counts.values().sum();
        let urls_fetched = *counts.get("fetched").unwrap_or(&0);
        let urls_pending =
            *counts.get("discovered").unwrap_or(&0) + *counts.get("fetching").unwrap_or(&0);
        let urls_failed =
            *counts.get("failed").unwrap_or(&0) + *counts.get("exhausted").unwrap_or(&0);

        Ok(CrawlState {
            urls_discovered,
            urls_fetched,
            urls_pending,
            urls_failed,
            has_pending_urls: urls_pending > 0,
            last_crawl_started: None, // Would need to track this separately
            last_crawl_completed: None,
        })
    }

    /// Get request statistics for a source.
    pub async fn get_request_stats(&self, source_id: &str) -> Result<RequestStats, DieselError> {
        #[derive(QueryableByName)]
        struct StatsRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            success_200: i64,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            not_modified_304: i64,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            errors: i64,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            avg_duration_ms: i64,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            total_bytes: i64,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            total_requests: i64,
        }

        with_conn!(self.pool, conn, {
            let result: StatsRow = diesel::sql_query(
                r#"
                SELECT
                    COALESCE(SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END), 0) as success_200,
                    COALESCE(SUM(CASE WHEN response_status = 304 THEN 1 ELSE 0 END), 0) as not_modified_304,
                    COALESCE(SUM(CASE WHEN error IS NOT NULL OR response_status >= 400 THEN 1 ELSE 0 END), 0) as errors,
                    COALESCE(AVG(duration_ms), 0) as avg_duration_ms,
                    COALESCE(SUM(response_size), 0) as total_bytes,
                    COUNT(*) as total_requests
                FROM crawl_requests
                WHERE source_id = $1
                "#,
            )
            .bind::<diesel::sql_types::Text, _>(source_id)
            .get_result(&mut conn)
            .await?;

            Ok(RequestStats {
                success_200: result.success_200 as u64,
                not_modified_304: result.not_modified_304 as u64,
                errors: result.errors as u64,
                avg_duration_ms: result.avg_duration_ms as u64,
                total_bytes: result.total_bytes as u64,
                total_requests: result.total_requests as u64,
            })
        })
    }

    /// Get combined crawl and request stats for a source.
    pub async fn get_all_stats_for_source(
        &self,
        source_id: &str,
    ) -> Result<CrawlStats, DieselError> {
        let crawl_state = self.get_crawl_state(source_id).await?;
        let request_stats = self.get_request_stats(source_id).await?;

        Ok(CrawlStats {
            urls_pending: crawl_state.urls_pending,
            urls_discovered: crawl_state.urls_discovered,
            urls_fetched: crawl_state.urls_fetched,
            urls_failed: crawl_state.urls_failed,
            crawl_state,
            request_stats,
        })
    }

    /// Get combined stats for all sources.
    pub async fn get_all_stats(&self) -> Result<HashMap<String, CrawlStats>, DieselError> {
        // Get all distinct source IDs
        #[derive(QueryableByName)]
        struct SourceIdRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            source_id: String,
        }

        let source_ids: Vec<SourceIdRow> = with_conn!(self.pool, conn, {
            diesel_async::RunQueryDsl::load(
                diesel::sql_query("SELECT DISTINCT source_id FROM crawl_urls"),
                &mut conn,
            )
            .await
        })?;

        let mut stats = HashMap::new();
        for row in source_ids {
            if let Ok(source_stats) = self.get_all_stats_for_source(&row.source_id).await {
                stats.insert(row.source_id, source_stats);
            }
        }

        Ok(stats)
    }

    /// Get recent downloads with details.
    pub async fn get_recent_downloads(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let limit = limit as i64;

        with_conn!(self.pool, conn, {
            let mut query = crawl_urls::table
                .filter(crawl_urls::status.eq("fetched"))
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

    /// Get all request stats for all sources.
    pub async fn get_all_request_stats(
        &self,
    ) -> Result<HashMap<String, RequestStats>, DieselError> {
        #[derive(QueryableByName)]
        struct SourceIdRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            source_id: String,
        }

        let source_ids: Vec<SourceIdRow> = with_conn!(self.pool, conn, {
            diesel_async::RunQueryDsl::load(
                diesel::sql_query("SELECT DISTINCT source_id FROM crawl_requests"),
                &mut conn,
            )
            .await
        })?;

        let mut stats = HashMap::new();
        for row in source_ids {
            if let Ok(request_stats) = self.get_request_stats(&row.source_id).await {
                stats.insert(row.source_id, request_stats);
            }
        }

        Ok(stats)
    }
}
