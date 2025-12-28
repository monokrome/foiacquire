//! Diesel-based crawl repository.
//!
//! Uses diesel-async for async database support. Works with both SQLite and PostgreSQL.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, RunQueryDsl};

use super::diesel_context::DbPool;
use super::diesel_models::{CrawlRequestRecord, CrawlUrlRecord};
use super::diesel_pool::DieselError;
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{CrawlRequest, CrawlUrl, DiscoveryMethod, UrlStatus};
use crate::schema::{crawl_config, crawl_requests, crawl_urls};
use crate::with_diesel_conn;

/// Convert a database record to a domain model.
impl From<CrawlUrlRecord> for CrawlUrl {
    fn from(record: CrawlUrlRecord) -> Self {
        let discovery_context: HashMap<String, serde_json::Value> =
            serde_json::from_str(&record.discovery_context).unwrap_or_default();

        CrawlUrl {
            url: record.url,
            source_id: record.source_id,
            status: UrlStatus::from_str(&record.status).unwrap_or(UrlStatus::Discovered),
            discovery_method: DiscoveryMethod::from_str(&record.discovery_method)
                .unwrap_or(DiscoveryMethod::Seed),
            parent_url: record.parent_url,
            discovery_context,
            depth: record.depth as u32,
            discovered_at: parse_datetime(&record.discovered_at),
            fetched_at: parse_datetime_opt(record.fetched_at),
            retry_count: record.retry_count as u32,
            last_error: record.last_error,
            next_retry_at: parse_datetime_opt(record.next_retry_at),
            etag: record.etag,
            last_modified: record.last_modified,
            content_hash: record.content_hash,
            document_id: record.document_id,
        }
    }
}

impl From<CrawlRequestRecord> for CrawlRequest {
    fn from(record: CrawlRequestRecord) -> Self {
        CrawlRequest {
            id: Some(record.id as i64),
            source_id: record.source_id,
            url: record.url,
            method: record.method,
            request_headers: serde_json::from_str(&record.request_headers).unwrap_or_default(),
            request_at: parse_datetime(&record.request_at),
            response_status: record.response_status.map(|s| s as u16),
            response_headers: serde_json::from_str(&record.response_headers).unwrap_or_default(),
            response_at: parse_datetime_opt(record.response_at),
            response_size: record.response_size.map(|s| s as u64),
            duration_ms: record.duration_ms.map(|d| d as u64),
            error: record.error,
            was_conditional: record.was_conditional != 0,
            was_not_modified: record.was_not_modified != 0,
        }
    }
}

/// Diesel-based crawl repository with compile-time query checking.
#[derive(Clone)]
pub struct DieselCrawlRepository {
    pool: DbPool,
}

impl DieselCrawlRepository {
    /// Create a new Diesel crawl repository.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    // ========================================================================
    // URL Operations
    // ========================================================================

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

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                use diesel::dsl::count_star;
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
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                use diesel::dsl::count_star;
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
            }
        }
    }

    /// Get a specific URL's crawl state.
    pub async fn get_url(
        &self,
        source_id: &str,
        url: &str,
    ) -> Result<Option<CrawlUrl>, DieselError> {
        with_diesel_conn!(self.pool, conn, {
            crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(crawl_urls::url.eq(url))
                .first::<CrawlUrlRecord>(&mut conn)
                .await
                .optional()
                .map(|opt| opt.map(CrawlUrl::from))
        })
    }

    /// Check if a URL has already been discovered.
    #[allow(dead_code)]
    pub async fn url_exists(&self, source_id: &str, url: &str) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_diesel_conn!(self.pool, conn, {
            let count: i64 = crawl_urls::table
                .filter(crawl_urls::source_id.eq(source_id))
                .filter(crawl_urls::url.eq(url))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count > 0)
        })
    }

    /// Update an existing URL's state.
    pub async fn update_url(&self, crawl_url: &CrawlUrl) -> Result<(), DieselError> {
        let status = crawl_url.status.as_str().to_string();
        let fetched_at = crawl_url.fetched_at.map(|dt| dt.to_rfc3339());
        let retry_count = crawl_url.retry_count as i32;
        let next_retry_at = crawl_url.next_retry_at.map(|dt| dt.to_rfc3339());

        with_diesel_conn!(self.pool, conn, {
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

    // ========================================================================
    // Claiming Operations
    // ========================================================================

    /// Get URLs that need to be fetched.
    pub async fn get_pending_urls(
        &self,
        source_id: &str,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let limit = limit as i64;
        with_diesel_conn!(self.pool, conn, {
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

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
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
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
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
            }
        }
    }

    /// Get failed URLs that are ready for retry.
    pub async fn get_retryable_urls(
        &self,
        source_id: &str,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let exhausted_cutoff = (now - chrono::Duration::days(70)).to_rfc3339();
        let limit = limit as i64;

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                diesel::sql_query(
                    r#"SELECT id, url, source_id, status, discovery_method, parent_url,
                              discovery_context, depth, discovered_at, fetched_at, retry_count,
                              last_error, next_retry_at, etag, last_modified, content_hash, document_id
                       FROM crawl_urls
                       WHERE source_id = ?
                       AND (
                           (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= ?))
                           OR (status = 'exhausted' AND (next_retry_at IS NULL OR next_retry_at < ?))
                       )
                       ORDER BY retry_count ASC, discovered_at ASC
                       LIMIT ?"#,
                )
                .bind::<diesel::sql_types::Text, _>(source_id)
                .bind::<diesel::sql_types::Text, _>(&now_str)
                .bind::<diesel::sql_types::Text, _>(&exhausted_cutoff)
                .bind::<diesel::sql_types::BigInt, _>(limit)
                .load::<CrawlUrlRecordRaw>(&mut conn)
                .await
                .map(|records| records.into_iter().map(CrawlUrl::from).collect())
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                diesel::sql_query(
                    r#"SELECT id, url, source_id, status, discovery_method, parent_url,
                              discovery_context, depth, discovered_at, fetched_at, retry_count,
                              last_error, next_retry_at, etag, last_modified, content_hash, document_id
                       FROM crawl_urls
                       WHERE source_id = $1
                       AND (
                           (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= $2))
                           OR (status = 'exhausted' AND (next_retry_at IS NULL OR next_retry_at < $3))
                       )
                       ORDER BY retry_count ASC, discovered_at ASC
                       LIMIT $4"#,
                )
                .bind::<diesel::sql_types::Text, _>(source_id)
                .bind::<diesel::sql_types::Text, _>(&now_str)
                .bind::<diesel::sql_types::Text, _>(&exhausted_cutoff)
                .bind::<diesel::sql_types::BigInt, _>(limit)
                .load::<CrawlUrlRecordRaw>(&mut conn)
                .await
                .map(|records| records.into_iter().map(CrawlUrl::from).collect())
            }
        }
    }

    // ========================================================================
    // Request Logging
    // ========================================================================

    /// Log a completed request.
    pub async fn log_request(&self, request: &CrawlRequest) -> Result<i64, DieselError> {
        let request_headers =
            serde_json::to_string(&request.request_headers).unwrap_or_else(|_| "{}".to_string());
        let request_at = request.request_at.to_rfc3339();
        let response_status = request.response_status.map(|s| s as i32);
        let response_headers =
            serde_json::to_string(&request.response_headers).unwrap_or_else(|_| "{}".to_string());
        let response_at = request.response_at.map(|dt| dt.to_rfc3339());
        let response_size = request.response_size.map(|s| s as i32);
        let duration_ms = request.duration_ms.map(|d| d as i32);
        let was_conditional = if request.was_conditional { 1 } else { 0 };
        let was_not_modified = if request.was_not_modified { 1 } else { 0 };

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                diesel::insert_into(crawl_requests::table)
                    .values((
                        crawl_requests::source_id.eq(&request.source_id),
                        crawl_requests::url.eq(&request.url),
                        crawl_requests::method.eq(&request.method),
                        crawl_requests::request_headers.eq(&request_headers),
                        crawl_requests::request_at.eq(&request_at),
                        crawl_requests::response_status.eq(&response_status),
                        crawl_requests::response_headers.eq(&response_headers),
                        crawl_requests::response_at.eq(&response_at),
                        crawl_requests::response_size.eq(&response_size),
                        crawl_requests::duration_ms.eq(&duration_ms),
                        crawl_requests::error.eq(&request.error),
                        crawl_requests::was_conditional.eq(was_conditional),
                        crawl_requests::was_not_modified.eq(was_not_modified),
                    ))
                    .execute(&mut conn)
                    .await?;

                diesel::sql_query("SELECT last_insert_rowid()")
                    .get_result::<LastInsertRowId>(&mut conn)
                    .await
                    .map(|r| r.id)
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;

                // PostgreSQL uses RETURNING to get the inserted ID
                let result: LastInsertId = diesel::sql_query(
                    r#"INSERT INTO crawl_requests
                       (source_id, url, method, request_headers, request_at, response_status,
                        response_headers, response_at, response_size, duration_ms, error,
                        was_conditional, was_not_modified)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                       RETURNING id"#,
                )
                .bind::<diesel::sql_types::Text, _>(&request.source_id)
                .bind::<diesel::sql_types::Text, _>(&request.url)
                .bind::<diesel::sql_types::Text, _>(&request.method)
                .bind::<diesel::sql_types::Text, _>(&request_headers)
                .bind::<diesel::sql_types::Text, _>(&request_at)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                    &response_status,
                )
                .bind::<diesel::sql_types::Text, _>(&response_headers)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&response_at)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(&response_size)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(&duration_ms)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&request.error)
                .bind::<diesel::sql_types::Integer, _>(was_conditional)
                .bind::<diesel::sql_types::Integer, _>(was_not_modified)
                .get_result(&mut conn)
                .await?;

                Ok(result.id as i64)
            }
        }
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Count URLs by status for a source.
    pub async fn count_by_status(
        &self,
        source_id: &str,
    ) -> Result<HashMap<String, u64>, DieselError> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                let rows: Vec<StatusCount> = diesel::sql_query(
                    "SELECT status, COUNT(*) as count FROM crawl_urls WHERE source_id = ? GROUP BY status",
                )
                .bind::<diesel::sql_types::Text, _>(source_id)
                .load::<StatusCount>(&mut conn)
                .await?;

                let mut counts = HashMap::new();
                for StatusCount { status, count } in rows {
                    counts.insert(status, count as u64);
                }
                Ok(counts)
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                let rows: Vec<StatusCount> = diesel::sql_query(
                    "SELECT status, COUNT(*) as count FROM crawl_urls WHERE source_id = $1 GROUP BY status",
                )
                .bind::<diesel::sql_types::Text, _>(source_id)
                .load::<StatusCount>(&mut conn)
                .await?;

                let mut counts = HashMap::new();
                for StatusCount { status, count } in rows {
                    counts.insert(status, count as u64);
                }
                Ok(counts)
            }
        }
    }

    /// Count total pending URLs.
    pub async fn count_pending(&self, source_id: &str) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
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
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
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
            }
        }
    }

    // ========================================================================
    // Config Operations
    // ========================================================================

    /// Check if config has changed since last crawl.
    pub async fn check_config_changed(
        &self,
        source_id: &str,
        current_hash: &str,
    ) -> Result<bool, DieselError> {
        with_diesel_conn!(self.pool, conn, {
            let stored_hash: Option<String> = crawl_config::table
                .find(source_id)
                .select(crawl_config::config_hash)
                .first(&mut conn)
                .await
                .optional()?;
            Ok(stored_hash.is_none_or(|h| h != current_hash))
        })
    }

    /// Store the current config hash.
    pub async fn store_config_hash(
        &self,
        source_id: &str,
        config_hash: &str,
    ) -> Result<(), DieselError> {
        let updated_at = Utc::now().to_rfc3339();

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                diesel::replace_into(crawl_config::table)
                    .values((
                        crawl_config::source_id.eq(source_id),
                        crawl_config::config_hash.eq(config_hash),
                        crawl_config::updated_at.eq(&updated_at),
                    ))
                    .execute(&mut conn)
                    .await?;
                Ok(())
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                diesel::sql_query(
                    r#"INSERT INTO crawl_config (source_id, config_hash, updated_at)
                       VALUES ($1, $2, $3)
                       ON CONFLICT (source_id) DO UPDATE SET
                           config_hash = EXCLUDED.config_hash,
                           updated_at = EXCLUDED.updated_at"#,
                )
                .bind::<diesel::sql_types::Text, _>(source_id)
                .bind::<diesel::sql_types::Text, _>(config_hash)
                .bind::<diesel::sql_types::Text, _>(&updated_at)
                .execute(&mut conn)
                .await?;
                Ok(())
            }
        }
    }

    // ========================================================================
    // Additional Stats Methods
    // ========================================================================

    /// Get crawl state for a source.
    pub async fn get_crawl_state(&self, source_id: &str) -> Result<CrawlState, DieselError> {
        let counts = self.count_by_status(source_id).await?;
        let pending = self.count_pending(source_id).await?;
        let discovered = *counts.get("discovered").unwrap_or(&0);
        let fetched = *counts.get("fetched").unwrap_or(&0);
        let failed = *counts.get("failed").unwrap_or(&0) + *counts.get("exhausted").unwrap_or(&0);

        Ok(CrawlState {
            urls_discovered: discovered,
            urls_fetched: fetched,
            urls_pending: pending,
            urls_failed: failed,
            has_pending_urls: pending > 0,
            last_crawl_started: None,
            last_crawl_completed: None,
        })
    }

    /// Get request statistics from crawl_requests table.
    pub async fn get_request_stats(&self, source_id: &str) -> Result<RequestStats, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct StatsRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            total_requests: i64,
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
        }

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                let query = format!(
                    r#"SELECT
                        COUNT(*) as total_requests,
                        SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END) as success_200,
                        SUM(CASE WHEN was_not_modified = 1 THEN 1 ELSE 0 END) as not_modified_304,
                        SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as errors,
                        COALESCE(AVG(duration_ms), 0) as avg_duration_ms,
                        COALESCE(SUM(response_size), 0) as total_bytes
                       FROM crawl_requests
                       WHERE source_id = '{}'"#,
                    source_id.replace('\'', "''")
                );

                let results: Vec<StatsRow> =
                    diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;

                #[allow(clippy::get_first)]
                if let Some(row) = results.get(0) {
                    Ok(RequestStats {
                        total_requests: row.total_requests as u64,
                        success_200: row.success_200 as u64,
                        not_modified_304: row.not_modified_304 as u64,
                        errors: row.errors as u64,
                        avg_duration_ms: row.avg_duration_ms as u64,
                        total_bytes: row.total_bytes as u64,
                    })
                } else {
                    Ok(RequestStats::default())
                }
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                let results: Vec<StatsRow> = diesel::sql_query(
                    r#"SELECT
                        COUNT(*) as total_requests,
                        SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END) as success_200,
                        SUM(CASE WHEN was_not_modified = 1 THEN 1 ELSE 0 END) as not_modified_304,
                        SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as errors,
                        COALESCE(AVG(duration_ms), 0)::bigint as avg_duration_ms,
                        COALESCE(SUM(response_size), 0) as total_bytes
                       FROM crawl_requests
                       WHERE source_id = $1"#,
                )
                .bind::<diesel::sql_types::Text, _>(source_id)
                .load(&mut conn)
                .await?;

                #[allow(clippy::get_first)]
                if let Some(row) = results.get(0) {
                    Ok(RequestStats {
                        total_requests: row.total_requests as u64,
                        success_200: row.success_200 as u64,
                        not_modified_304: row.not_modified_304 as u64,
                        errors: row.errors as u64,
                        avg_duration_ms: row.avg_duration_ms as u64,
                        total_bytes: row.total_bytes as u64,
                    })
                } else {
                    Ok(RequestStats::default())
                }
            }
        }
    }

    /// Get all stats for a source.
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

    /// Get all stats for all sources.
    pub async fn get_all_stats(&self) -> Result<HashMap<String, CrawlStats>, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct SourceIdRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            source_id: String,
        }

        let source_ids: Vec<SourceIdRow> = match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query("SELECT DISTINCT source_id FROM crawl_urls"),
                    &mut conn,
                )
                .await?
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query("SELECT DISTINCT source_id FROM crawl_urls"),
                    &mut conn,
                )
                .await?
            }
        };

        let mut stats = HashMap::new();
        for row in source_ids {
            if let Ok(crawl_stats) = self.get_all_stats_for_source(&row.source_id).await {
                stats.insert(row.source_id, crawl_stats);
            }
        }

        Ok(stats)
    }

    /// Get recently fetched URLs.
    pub async fn get_recent_downloads(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let limit = limit as i64;

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
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
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
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
            }
        }
    }

    /// Get failed URLs.
    pub async fn get_failed_urls(
        &self,
        source_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let limit = limit as i64;

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
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
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
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
            }
        }
    }

    // ========================================================================
    // Cleanup Operations
    // ========================================================================

    /// Clear pending crawl state for a source (keeps fetched URLs).
    #[allow(dead_code)]
    pub async fn clear_source(&self, source_id: &str) -> Result<(), DieselError> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
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

                diesel::delete(
                    crawl_requests::table.filter(crawl_requests::source_id.eq(source_id)),
                )
                .execute(&mut conn)
                .await?;

                Ok(())
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
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

                diesel::delete(
                    crawl_requests::table.filter(crawl_requests::source_id.eq(source_id)),
                )
                .execute(&mut conn)
                .await?;

                Ok(())
            }
        }
    }

    /// Clear ALL crawl state for a source.
    pub async fn clear_source_all(&self, source_id: &str) -> Result<(), DieselError> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                diesel::delete(crawl_urls::table.filter(crawl_urls::source_id.eq(source_id)))
                    .execute(&mut conn)
                    .await?;

                diesel::delete(
                    crawl_requests::table.filter(crawl_requests::source_id.eq(source_id)),
                )
                .execute(&mut conn)
                .await?;

                diesel::delete(crawl_config::table.filter(crawl_config::source_id.eq(source_id)))
                    .execute(&mut conn)
                    .await?;

                Ok(())
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                diesel::delete(crawl_urls::table.filter(crawl_urls::source_id.eq(source_id)))
                    .execute(&mut conn)
                    .await?;

                diesel::delete(
                    crawl_requests::table.filter(crawl_requests::source_id.eq(source_id)),
                )
                .execute(&mut conn)
                .await?;

                diesel::delete(crawl_config::table.filter(crawl_config::source_id.eq(source_id)))
                    .execute(&mut conn)
                    .await?;

                Ok(())
            }
        }
    }

    // ========================================================================
    // Additional Methods (for compatibility)
    // ========================================================================

    /// Count URLs for a source.
    pub async fn count_by_source(&self, source_id: &str) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                let count: i64 = crawl_urls::table
                    .filter(crawl_urls::source_id.eq(source_id))
                    .select(count_star())
                    .first(&mut conn)
                    .await?;
                Ok(count as u64)
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                let count: i64 = crawl_urls::table
                    .filter(crawl_urls::source_id.eq(source_id))
                    .select(count_star())
                    .first(&mut conn)
                    .await?;
                Ok(count as u64)
            }
        }
    }

    /// Get all request stats for all sources.
    pub async fn get_all_request_stats(
        &self,
    ) -> Result<HashMap<String, RequestStats>, DieselError> {
        #[derive(diesel::QueryableByName)]
        struct SourceIdRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            source_id: String,
        }

        let source_ids: Vec<SourceIdRow> = match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query("SELECT DISTINCT source_id FROM crawl_requests"),
                    &mut conn,
                )
                .await?
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                diesel_async::RunQueryDsl::load(
                    diesel::sql_query("SELECT DISTINCT source_id FROM crawl_requests"),
                    &mut conn,
                )
                .await?
            }
        };

        let mut stats = HashMap::new();
        for row in source_ids {
            if let Ok(request_stats) = self.get_request_stats(&row.source_id).await {
                stats.insert(row.source_id, request_stats);
            }
        }

        Ok(stats)
    }

    /// Get URLs needing refresh (older than cutoff date).
    pub async fn get_urls_needing_refresh(
        &self,
        source_id: &str,
        cutoff: chrono::DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<CrawlUrl>, DieselError> {
        let cutoff_str = cutoff.to_rfc3339();
        let limit = limit as i64;

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                crawl_urls::table
                    .filter(crawl_urls::source_id.eq(source_id))
                    .filter(crawl_urls::status.eq("fetched"))
                    .filter(crawl_urls::fetched_at.lt(&cutoff_str))
                    .order(crawl_urls::fetched_at.asc())
                    .limit(limit)
                    .load::<CrawlUrlRecord>(&mut conn)
                    .await
                    .map(|records| records.into_iter().map(CrawlUrl::from).collect())
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                crawl_urls::table
                    .filter(crawl_urls::source_id.eq(source_id))
                    .filter(crawl_urls::status.eq("fetched"))
                    .filter(crawl_urls::fetched_at.lt(&cutoff_str))
                    .order(crawl_urls::fetched_at.asc())
                    .limit(limit)
                    .load::<CrawlUrlRecord>(&mut conn)
                    .await
                    .map(|records| records.into_iter().map(CrawlUrl::from).collect())
            }
        }
    }

    /// Mark a URL for refresh by resetting its status to discovered.
    pub async fn mark_url_for_refresh(
        &self,
        source_id: &str,
        url: &str,
    ) -> Result<(), DieselError> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                diesel::update(
                    crawl_urls::table
                        .filter(crawl_urls::source_id.eq(source_id))
                        .filter(crawl_urls::url.eq(url)),
                )
                .set(crawl_urls::status.eq("discovered"))
                .execute(&mut conn)
                .await?;
                Ok(())
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                diesel::update(
                    crawl_urls::table
                        .filter(crawl_urls::source_id.eq(source_id))
                        .filter(crawl_urls::url.eq(url)),
                )
                .set(crawl_urls::status.eq("discovered"))
                .execute(&mut conn)
                .await?;
                Ok(())
            }
        }
    }
}

/// Crawl state statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrawlState {
    pub urls_discovered: u64,
    pub urls_fetched: u64,
    pub urls_pending: u64,
    pub urls_failed: u64,
    pub has_pending_urls: bool,
    pub last_crawl_started: Option<String>,
    pub last_crawl_completed: Option<String>,
}

impl CrawlState {
    /// Check if there's resumable work.
    pub fn needs_resume(&self) -> bool {
        self.has_pending_urls || self.urls_pending > 0
    }

    /// Check if the crawl is complete (fetched > 0 and no pending work).
    pub fn is_complete(&self) -> bool {
        self.urls_fetched > 0 && !self.needs_resume()
    }
}

/// Request statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestStats {
    pub success_200: u64,
    pub not_modified_304: u64,
    pub errors: u64,
    pub avg_duration_ms: u64,
    pub total_bytes: u64,
    pub total_requests: u64,
}

/// Combined crawl statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrawlStats {
    pub crawl_state: CrawlState,
    pub request_stats: RequestStats,
    /// Convenience accessors for urls_pending (delegates to crawl_state)
    pub urls_pending: u64,
    pub urls_discovered: u64,
    pub urls_fetched: u64,
    pub urls_failed: u64,
}

// Helper struct for SQL query results
#[derive(QueryableByName)]
struct StatusCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
#[allow(dead_code)]
struct LastInsertRowId {
    #[diesel(sql_type = diesel::sql_types::BigInt, column_name = "last_insert_rowid()")]
    id: i64,
}

#[derive(QueryableByName)]
#[allow(dead_code)]
struct LastInsertId {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
}

/// Raw crawl URL record for QueryableByName (used with sql_query).
#[derive(QueryableByName, Debug)]
struct CrawlUrlRecordRaw {
    #[allow(dead_code)]
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    url: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    source_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    discovery_method: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    parent_url: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    discovery_context: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    depth: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    discovered_at: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    fetched_at: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    retry_count: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    last_error: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    next_retry_at: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    etag: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    last_modified: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    content_hash: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    document_id: Option<String>,
}

impl From<CrawlUrlRecordRaw> for CrawlUrl {
    fn from(record: CrawlUrlRecordRaw) -> Self {
        let discovery_context: HashMap<String, serde_json::Value> =
            serde_json::from_str(&record.discovery_context).unwrap_or_default();

        CrawlUrl {
            url: record.url,
            source_id: record.source_id,
            status: UrlStatus::from_str(&record.status).unwrap_or(UrlStatus::Discovered),
            discovery_method: DiscoveryMethod::from_str(&record.discovery_method)
                .unwrap_or(DiscoveryMethod::Seed),
            parent_url: record.parent_url,
            discovery_context,
            depth: record.depth as u32,
            discovered_at: parse_datetime(&record.discovered_at),
            fetched_at: parse_datetime_opt(record.fetched_at),
            retry_count: record.retry_count as u32,
            last_error: record.last_error,
            next_retry_at: parse_datetime_opt(record.next_retry_at),
            etag: record.etag,
            last_modified: record.last_modified,
            content_hash: record.content_hash,
            document_id: record.document_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::diesel_pool::AsyncSqlitePool;
    use super::*;
    use diesel_async::SimpleAsyncConnection;
    use tempfile::tempdir;

    async fn setup_test_db() -> (DbPool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_url = db_path.display().to_string();

        let sqlite_pool = AsyncSqlitePool::new(&db_url, 5);
        let mut conn = sqlite_pool.get().await.unwrap();

        conn.batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS crawl_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            );

            CREATE TABLE IF NOT EXISTS crawl_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            );

            CREATE TABLE IF NOT EXISTS crawl_config (
                source_id TEXT PRIMARY KEY,
                config_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            "#,
        )
        .await
        .unwrap();

        (DbPool::Sqlite(sqlite_pool), dir)
    }

    #[tokio::test]
    async fn test_crawl_url_crud() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselCrawlRepository::new(pool);

        // Create a crawl URL
        let crawl_url = CrawlUrl::new(
            "https://example.com/page".to_string(),
            "test-source".to_string(),
            DiscoveryMethod::Seed,
            None,
            0,
        );

        // Add URL
        let added = repo.add_url(&crawl_url).await.unwrap();
        assert!(added);

        // Check exists
        assert!(repo
            .url_exists("test-source", "https://example.com/page")
            .await
            .unwrap());

        // Try to add duplicate
        let duplicate = repo.add_url(&crawl_url).await.unwrap();
        assert!(!duplicate);

        // Get URL
        let fetched = repo
            .get_url("test-source", "https://example.com/page")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.url, "https://example.com/page");
        assert_eq!(fetched.status, UrlStatus::Discovered);

        // Get pending URLs
        let pending = repo.get_pending_urls("test-source", 10).await.unwrap();
        assert_eq!(pending.len(), 1);

        // Count by status
        let counts = repo.count_by_status("test-source").await.unwrap();
        assert_eq!(*counts.get("discovered").unwrap_or(&0), 1);
    }

    #[tokio::test]
    async fn test_claim_pending_url() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselCrawlRepository::new(pool);

        // Add a URL
        let crawl_url = CrawlUrl::new(
            "https://example.com/claim-test".to_string(),
            "test-source".to_string(),
            DiscoveryMethod::Seed,
            None,
            0,
        );
        repo.add_url(&crawl_url).await.unwrap();

        // Claim URL
        let claimed = repo
            .claim_pending_url(Some("test-source"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.url, "https://example.com/claim-test");
        assert_eq!(claimed.status, UrlStatus::Fetching);

        // Verify no more pending
        let pending = repo.claim_pending_url(Some("test-source")).await.unwrap();
        assert!(pending.is_none());
    }

    #[tokio::test]
    async fn test_config_hash() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselCrawlRepository::new(pool);

        // Initially should indicate change
        let changed = repo
            .check_config_changed("test-source", "hash1")
            .await
            .unwrap();
        assert!(changed);

        // Store hash
        repo.store_config_hash("test-source", "hash1")
            .await
            .unwrap();

        // Now should not indicate change
        let changed = repo
            .check_config_changed("test-source", "hash1")
            .await
            .unwrap();
        assert!(!changed);

        // Different hash should indicate change
        let changed = repo
            .check_config_changed("test-source", "hash2")
            .await
            .unwrap();
        assert!(changed);
    }
}
