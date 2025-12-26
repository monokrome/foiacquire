//! URL claiming operations for crawl repository.

use chrono::Utc;
use rusqlite::params;

use super::helpers::row_to_crawl_url;
use super::{CrawlRepository, Result};
use crate::models::{CrawlUrl, UrlStatus};

impl CrawlRepository {
    /// Get URLs that need to be fetched.
    pub fn get_pending_urls(&self, source_id: &str, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_urls
            WHERE source_id = ?
            AND status IN ('discovered', 'fetching')
            ORDER BY depth ASC, discovered_at ASC
            LIMIT ?
        "#,
        )?;

        let urls = stmt
            .query_map(params![source_id, limit], |row| row_to_crawl_url(row))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(urls)
    }

    /// Atomically claim a pending URL for processing.
    pub fn claim_pending_url(&self, source_id: Option<&str>) -> Result<Option<CrawlUrl>> {
        let conn = self.connect()?;

        conn.execute("BEGIN IMMEDIATE", [])?;

        let result: std::result::Result<Option<CrawlUrl>, super::super::RepositoryError> = (|| {
            let query_result = if let Some(sid) = source_id {
                conn.query_row(
                    r#"
                    SELECT * FROM crawl_urls
                    WHERE source_id = ? AND status = 'discovered'
                    ORDER BY depth ASC, discovered_at ASC
                    LIMIT 1
                    "#,
                    params![sid],
                    |row| row_to_crawl_url(row),
                )
            } else {
                conn.query_row(
                    r#"
                    SELECT * FROM crawl_urls
                    WHERE status = 'discovered'
                    ORDER BY depth ASC, discovered_at ASC
                    LIMIT 1
                    "#,
                    [],
                    |row| row_to_crawl_url(row),
                )
            };

            match query_result {
                Ok(mut crawl_url) => {
                    conn.execute(
                        "UPDATE crawl_urls SET status = 'fetching' WHERE source_id = ? AND url = ?",
                        params![crawl_url.source_id, crawl_url.url],
                    )?;
                    crawl_url.status = UrlStatus::Fetching;
                    Ok(Some(crawl_url))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })(
        );

        if result.is_ok() {
            conn.execute("COMMIT", [])?;
        } else {
            let _ = conn.execute("ROLLBACK", []);
        }

        result
    }

    /// Atomically claim multiple pending URLs for processing.
    pub fn claim_pending_urls(&self, source_id: Option<&str>, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;

        conn.execute("BEGIN IMMEDIATE", [])?;

        let result: std::result::Result<Vec<CrawlUrl>, super::super::RepositoryError> = (|| {
            let mut stmt = conn.prepare(
                r#"
                SELECT * FROM crawl_urls
                WHERE (?1 IS NULL OR source_id = ?1) AND status = 'discovered'
                ORDER BY depth ASC, discovered_at ASC
                LIMIT ?2
            "#,
            )?;
            let urls: Vec<CrawlUrl> = stmt
                .query_map(params![source_id, limit], |row| row_to_crawl_url(row))?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            for url in &urls {
                conn.execute(
                    "UPDATE crawl_urls SET status = 'fetching' WHERE source_id = ? AND url = ?",
                    params![url.source_id, url.url],
                )?;
            }

            Ok(urls
                .into_iter()
                .map(|mut u| {
                    u.status = UrlStatus::Fetching;
                    u
                })
                .collect())
        })(
        );

        if result.is_ok() {
            conn.execute("COMMIT", [])?;
        } else {
            let _ = conn.execute("ROLLBACK", []);
        }

        result
    }

    /// Get failed URLs that are ready for retry.
    pub fn get_retryable_urls(&self, source_id: &str, limit: u32) -> Result<Vec<CrawlUrl>> {
        let conn = self.connect()?;
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let exhausted_cutoff = (now - chrono::Duration::days(70)).to_rfc3339();

        let mut stmt = conn.prepare(
            r#"
            SELECT * FROM crawl_urls
            WHERE source_id = ?
            AND (
                (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= ?))
                OR (status = 'exhausted' AND (next_retry_at IS NULL OR next_retry_at < ?))
            )
            ORDER BY retry_count ASC, discovered_at ASC
            LIMIT ?
        "#,
        )?;

        let urls = stmt
            .query_map(
                params![source_id, now_str, exhausted_cutoff, limit],
                |row| row_to_crawl_url(row),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(urls)
    }
}
