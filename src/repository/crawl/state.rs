//! Crawl state and statistics queries.

use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::HashMap;

use super::{CrawlRepository, Result};
use crate::models::{CrawlState, RequestStats};

impl CrawlRepository {
    /// Get aggregate crawl state for a source.
    pub fn get_crawl_state(&self, source_id: &str) -> Result<CrawlState> {
        let conn = self.connect()?;

        let mut status_counts: HashMap<String, u64> = HashMap::new();
        {
            let mut stmt = conn.prepare(
                r#"
                SELECT status, COUNT(*) as count
                FROM crawl_urls WHERE source_id = ?
                GROUP BY status
            "#,
            )?;

            let rows = stmt.query_map(params![source_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?;

            for row in rows {
                let (status, count) = row?;
                status_counts.insert(status, count);
            }
        }

        let timing: (Option<String>, Option<String>, Option<String>) = conn.query_row(
            r#"
            SELECT
                MIN(discovered_at) as first_discovered,
                MAX(fetched_at) as last_fetched,
                MIN(CASE WHEN status IN ('discovered', 'fetching')
                    THEN discovered_at END) as oldest_pending
            FROM crawl_urls WHERE source_id = ?
            "#,
            params![source_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;

        let unexplored_count: i64 = conn.query_row(
            r#"
            SELECT COUNT(*) FROM crawl_urls u1
            WHERE u1.source_id = ?
            AND u1.status = 'fetched'
            AND u1.discovery_method IN ('html_link', 'pagination', 'api_result')
            AND NOT EXISTS (
                SELECT 1 FROM crawl_urls u2
                WHERE u2.source_id = u1.source_id
                AND u2.parent_url = u1.url
            )
            AND u1.depth < 10
            "#,
            params![source_id],
            |row| row.get(0),
        )?;

        let urls_discovered: u64 = status_counts.values().sum();
        let urls_fetched = *status_counts.get("fetched").unwrap_or(&0);
        let urls_failed = status_counts.get("failed").unwrap_or(&0)
            + status_counts.get("exhausted").unwrap_or(&0);
        let urls_pending = status_counts.get("discovered").unwrap_or(&0)
            + status_counts.get("fetching").unwrap_or(&0);

        Ok(CrawlState {
            source_id: source_id.to_string(),
            last_crawl_started: timing
                .0
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            last_crawl_completed: if urls_pending == 0 {
                timing
                    .1
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
            } else {
                None
            },
            urls_discovered,
            urls_fetched,
            urls_failed,
            urls_pending,
            has_pending_urls: urls_pending > 0,
            has_unexplored_branches: unexplored_count > 0,
            oldest_pending_url: timing
                .2
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc)),
        })
    }

    /// Count crawl URLs for a source.
    pub fn count_by_source(&self, source_id: &str) -> Result<u64> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM crawl_urls WHERE source_id = ?",
            params![source_id],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get request statistics for a source.
    pub fn get_request_stats(&self, source_id: &str) -> Result<RequestStats> {
        let conn = self.connect()?;

        let stats = conn.query_row(
            r#"
            SELECT
                COUNT(*) as total_requests,
                SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END) as success_200,
                SUM(CASE WHEN response_status = 304 THEN 1 ELSE 0 END) as not_modified_304,
                SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END) as errors,
                SUM(was_conditional) as conditional_requests,
                AVG(duration_ms) as avg_duration_ms,
                SUM(response_size) as total_bytes
            FROM crawl_requests
            WHERE source_id = ?
            "#,
            params![source_id],
            |row| {
                Ok(RequestStats {
                    total_requests: row.get::<_, i64>(0)? as u64,
                    success_200: row.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
                    not_modified_304: row.get::<_, Option<i64>>(2)?.unwrap_or(0) as u64,
                    errors: row.get::<_, Option<i64>>(3)?.unwrap_or(0) as u64,
                    conditional_requests: row.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                    avg_duration_ms: row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                    total_bytes: row.get::<_, Option<i64>>(6)?.unwrap_or(0) as u64,
                })
            },
        )?;

        Ok(stats)
    }

    /// Get request statistics for all sources (bulk query).
    pub fn get_all_request_stats(&self) -> Result<HashMap<String, RequestStats>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            r#"
            SELECT
                source_id,
                COUNT(*) as total_requests,
                SUM(CASE WHEN response_status = 200 THEN 1 ELSE 0 END) as success_200,
                SUM(CASE WHEN response_status = 304 THEN 1 ELSE 0 END) as not_modified_304,
                SUM(CASE WHEN response_status >= 400 THEN 1 ELSE 0 END) as errors,
                SUM(was_conditional) as conditional_requests,
                AVG(duration_ms) as avg_duration_ms,
                SUM(response_size) as total_bytes
            FROM crawl_requests
            GROUP BY source_id
            "#,
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                RequestStats {
                    total_requests: row.get::<_, i64>(1)? as u64,
                    success_200: row.get::<_, Option<i64>>(2)?.unwrap_or(0) as u64,
                    not_modified_304: row.get::<_, Option<i64>>(3)?.unwrap_or(0) as u64,
                    errors: row.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                    conditional_requests: row.get::<_, Option<i64>>(5)?.unwrap_or(0) as u64,
                    avg_duration_ms: row.get::<_, Option<f64>>(6)?.unwrap_or(0.0),
                    total_bytes: row.get::<_, Option<i64>>(7)?.unwrap_or(0) as u64,
                },
            ))
        })?;

        let mut stats = HashMap::new();
        for row in rows {
            let (source_id, request_stats) = row?;
            stats.insert(source_id, request_stats);
        }

        Ok(stats)
    }

    /// Get aggregate stats across all sources (bulk query).
    pub fn get_all_stats(&self) -> Result<HashMap<String, CrawlState>> {
        let conn = self.connect()?;

        // Bulk query 1: Get all status counts grouped by source
        let mut status_by_source: HashMap<String, HashMap<String, u64>> = HashMap::new();
        {
            let mut stmt = conn.prepare(
                r#"
                SELECT source_id, status, COUNT(*) as count
                FROM crawl_urls
                GROUP BY source_id, status
                "#,
            )?;

            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)? as u64,
                ))
            })?;

            for row in rows {
                let (source_id, status, count) = row?;
                status_by_source
                    .entry(source_id)
                    .or_default()
                    .insert(status, count);
            }
        }

        // Bulk query 2: Get timing info for all sources
        #[allow(clippy::type_complexity)]
        let mut timing_by_source: HashMap<
            String,
            (Option<String>, Option<String>, Option<String>),
        > = HashMap::new();
        {
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    source_id,
                    MIN(discovered_at) as first_discovered,
                    MAX(fetched_at) as last_fetched,
                    MIN(CASE WHEN status IN ('discovered', 'fetching')
                        THEN discovered_at END) as oldest_pending
                FROM crawl_urls
                GROUP BY source_id
                "#,
            )?;

            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?;

            for row in rows {
                let (source_id, first, last, oldest) = row?;
                timing_by_source.insert(source_id, (first, last, oldest));
            }
        }

        // Bulk query 3: Get unexplored branch counts for all sources
        let mut unexplored_by_source: HashMap<String, i64> = HashMap::new();
        {
            let mut stmt = conn.prepare(
                r#"
                SELECT u1.source_id, COUNT(*) FROM crawl_urls u1
                WHERE u1.status = 'fetched'
                AND u1.discovery_method IN ('html_link', 'pagination', 'api_result')
                AND NOT EXISTS (
                    SELECT 1 FROM crawl_urls u2
                    WHERE u2.source_id = u1.source_id
                    AND u2.parent_url = u1.url
                )
                AND u1.depth < 10
                GROUP BY u1.source_id
                "#,
            )?;

            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;

            for row in rows {
                let (source_id, count) = row?;
                unexplored_by_source.insert(source_id, count);
            }
        }

        // Build CrawlState for each source
        let mut stats = HashMap::new();
        for (source_id, status_counts) in status_by_source {
            let timing = timing_by_source
                .get(&source_id)
                .cloned()
                .unwrap_or((None, None, None));

            let unexplored_count = unexplored_by_source.get(&source_id).copied().unwrap_or(0);

            let urls_discovered: u64 = status_counts.values().sum();
            let urls_fetched = *status_counts.get("fetched").unwrap_or(&0);
            let urls_failed = status_counts.get("failed").unwrap_or(&0)
                + status_counts.get("exhausted").unwrap_or(&0);
            let urls_pending = status_counts.get("discovered").unwrap_or(&0)
                + status_counts.get("fetching").unwrap_or(&0);

            let state = CrawlState {
                source_id: source_id.clone(),
                last_crawl_started: timing
                    .0
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc)),
                last_crawl_completed: if urls_pending == 0 {
                    timing
                        .1
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc))
                } else {
                    None
                },
                urls_discovered,
                urls_fetched,
                urls_failed,
                urls_pending,
                has_pending_urls: urls_pending > 0,
                has_unexplored_branches: unexplored_count > 0,
                oldest_pending_url: timing
                    .2
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc)),
            };

            stats.insert(source_id, state);
        }

        Ok(stats)
    }
}
