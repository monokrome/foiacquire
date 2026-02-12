//! Diesel-backed rate limiter for persistent multi-process coordination.
//!
//! Stores rate limit state in SQLite/PostgreSQL for persistence across restarts
//! and coordination between multiple scraper processes.

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use tracing::warn;

use super::backend::{DomainRateState, RateLimitBackend, RateLimitError, RateLimitResult};
use crate::repository::pool::DbPool;
use crate::repository::{NewRateLimitState, RateLimitStateRecord};
use crate::schema::rate_limit_state;
use crate::with_conn_split;

/// Diesel-backed rate limit storage (SQLite/PostgreSQL).
#[derive(Clone)]
pub struct DieselRateLimitBackend {
    pool: DbPool,
    base_delay_ms: u64,
}

impl DieselRateLimitBackend {
    /// Create a new Diesel rate limit backend.
    pub fn new(pool: DbPool, base_delay_ms: u64) -> Self {
        Self {
            pool,
            base_delay_ms,
        }
    }

    /// Create from a SQLite file path.
    pub fn from_sqlite_path(path: &std::path::Path, base_delay_ms: u64) -> Self {
        Self {
            pool: DbPool::sqlite_from_path(path),
            base_delay_ms,
        }
    }

    /// Convert a database record to domain state.
    fn record_to_state(record: RateLimitStateRecord) -> DomainRateState {
        DomainRateState {
            domain: record.domain,
            current_delay_ms: record.current_delay_ms.max(0) as u64,
            last_request_at: None,    // Not stored in DB, managed at runtime
            consecutive_successes: 0, // Reset on load
            in_backoff: record.in_backoff != 0,
            total_requests: record.total_requests.max(0) as u64,
            rate_limit_hits: record.rate_limit_hits.max(0) as u64,
        }
    }

    /// Save a domain state to the database.
    async fn save_state(&self, state: &DomainRateState) -> RateLimitResult<()> {
        let now = Utc::now().to_rfc3339();
        let domain = &state.domain;
        let current_delay_ms = i32::try_from(state.current_delay_ms).unwrap_or(i32::MAX);
        let in_backoff = i32::from(state.in_backoff);
        let total_requests = i32::try_from(state.total_requests).unwrap_or(i32::MAX);
        let rate_limit_hits = i32::try_from(state.rate_limit_hits).unwrap_or(i32::MAX);

        with_conn_split!(self.pool,
            sqlite: conn => {
                diesel::replace_into(rate_limit_state::table)
                    .values(NewRateLimitState {
                        domain,
                        current_delay_ms,
                        in_backoff,
                        total_requests,
                        rate_limit_hits,
                        updated_at: &now,
                    })
                    .execute(&mut conn)
                    .await
                    .map_err(|e| RateLimitError::Database(e.to_string()))?;
                Ok(())
            },
            postgres: conn => {
                use diesel::upsert::excluded;
                diesel::insert_into(rate_limit_state::table)
                    .values(NewRateLimitState {
                        domain,
                        current_delay_ms,
                        in_backoff,
                        total_requests,
                        rate_limit_hits,
                        updated_at: &now,
                    })
                    .on_conflict(rate_limit_state::domain)
                    .do_update()
                    .set((
                        rate_limit_state::current_delay_ms.eq(excluded(rate_limit_state::current_delay_ms)),
                        rate_limit_state::in_backoff.eq(excluded(rate_limit_state::in_backoff)),
                        rate_limit_state::total_requests.eq(excluded(rate_limit_state::total_requests)),
                        rate_limit_state::rate_limit_hits.eq(excluded(rate_limit_state::rate_limit_hits)),
                        rate_limit_state::updated_at.eq(excluded(rate_limit_state::updated_at)),
                    ))
                    .execute(&mut conn)
                    .await
                    .map_err(|e| RateLimitError::Database(e.to_string()))?;
                Ok(())
            }
        )
    }

    /// Load a domain state from the database.
    async fn load_state(&self, domain: &str) -> RateLimitResult<Option<DomainRateState>> {
        let result: Option<RateLimitStateRecord> = with_conn_split!(self.pool,
            sqlite: conn => {
                rate_limit_state::table
                    .find(domain)
                    .first::<RateLimitStateRecord>(&mut conn)
                    .await
                    .optional()
                    .map_err(|e| RateLimitError::Database(e.to_string()))?
            },
            postgres: conn => {
                rate_limit_state::table
                    .find(domain)
                    .first::<RateLimitStateRecord>(&mut conn)
                    .await
                    .optional()
                    .map_err(|e| RateLimitError::Database(e.to_string()))?
            }
        );

        Ok(result.map(Self::record_to_state))
    }
}

#[async_trait]
impl RateLimitBackend for DieselRateLimitBackend {
    async fn get_or_create_domain(
        &self,
        domain: &str,
        base_delay_ms: u64,
    ) -> RateLimitResult<DomainRateState> {
        // Try to load existing
        if let Some(state) = self.load_state(domain).await? {
            return Ok(state);
        }

        // Create new
        let state = DomainRateState::new(domain.to_string(), base_delay_ms);
        self.save_state(&state).await?;
        Ok(state)
    }

    async fn update_domain(&self, state: &DomainRateState) -> RateLimitResult<()> {
        self.save_state(state).await
    }

    async fn acquire(&self, domain: &str, base_delay_ms: u64) -> RateLimitResult<Duration> {
        // Load current state
        let mut state = self.get_or_create_domain(domain, base_delay_ms).await?;

        // Calculate wait time based on last_request_at
        // Note: For true distributed locking, you'd use database-level locking
        // This implementation is simpler - each process tracks its own timing
        let wait_time = state.time_until_ready();

        // Update request count
        state.total_requests += 1;
        state.last_request_at = Some(Utc::now().timestamp_millis());

        // Persist the update
        if let Err(e) = self.save_state(&state).await {
            warn!("Failed to persist rate limit state for {}: {}", domain, e);
        }

        Ok(wait_time)
    }

    async fn record_403(&self, _domain: &str, _url: &str) -> RateLimitResult<()> {
        // For SQLite backend, we don't track individual 403 URLs
        // Just increment the rate_limit_hits counter when triggered
        // The RateLimiter layer handles the actual 403 pattern detection in memory
        Ok(())
    }

    async fn get_403_count(&self, _domain: &str, _window_ms: u64) -> RateLimitResult<usize> {
        // 403 tracking is handled in memory by RateLimiter
        Ok(0)
    }

    async fn clear_403s(&self, _domain: &str) -> RateLimitResult<()> {
        // 403 tracking is handled in memory by RateLimiter
        Ok(())
    }

    async fn cleanup_expired_403s(&self, _window_ms: u64) -> RateLimitResult<u64> {
        // 403 tracking is handled in memory by RateLimiter
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::migrations;
    use tempfile::tempdir;

    async fn setup_test_db() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let docs_dir = dir.path().join("docs");
        std::fs::create_dir_all(&docs_dir).unwrap();

        let db_url = format!("sqlite:{}", db_path.display());
        migrations::run_migrations(&db_url, false).await.unwrap();
        (dir, db_path)
    }

    #[tokio::test]
    async fn test_get_or_create_domain() {
        let (_dir, db_path) = setup_test_db().await;
        let backend = DieselRateLimitBackend::from_sqlite_path(&db_path, 100);

        let state = backend
            .get_or_create_domain("example.com", 200)
            .await
            .unwrap();
        assert_eq!(state.domain, "example.com");
        assert_eq!(state.current_delay_ms, 200);
        assert!(!state.in_backoff);
    }

    #[tokio::test]
    async fn test_persistence_across_loads() {
        let (_dir, db_path) = setup_test_db().await;

        // Create and update state
        {
            let backend = DieselRateLimitBackend::from_sqlite_path(&db_path, 100);
            let mut state = backend
                .get_or_create_domain("example.com", 100)
                .await
                .unwrap();
            state.current_delay_ms = 500;
            state.in_backoff = true;
            state.rate_limit_hits = 5;
            backend.update_domain(&state).await.unwrap();
        }

        // Load with new backend instance
        {
            let backend = DieselRateLimitBackend::from_sqlite_path(&db_path, 100);
            let state = backend
                .get_or_create_domain("example.com", 100)
                .await
                .unwrap();
            assert_eq!(state.current_delay_ms, 500);
            assert!(state.in_backoff);
            assert_eq!(state.rate_limit_hits, 5);
        }
    }

    #[tokio::test]
    async fn test_acquire_increments_requests() {
        let (_dir, db_path) = setup_test_db().await;
        let backend = DieselRateLimitBackend::from_sqlite_path(&db_path, 100);

        backend.acquire("example.com", 100).await.unwrap();
        backend.acquire("example.com", 100).await.unwrap();

        let state = backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        assert_eq!(state.total_requests, 2);
    }

    #[tokio::test]
    async fn test_multiple_domains() {
        let (_dir, db_path) = setup_test_db().await;
        let backend = DieselRateLimitBackend::from_sqlite_path(&db_path, 100);

        backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        backend.get_or_create_domain("test.org", 200).await.unwrap();

        let s1 = backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        let s2 = backend.get_or_create_domain("test.org", 200).await.unwrap();

        assert_eq!(s1.current_delay_ms, 100);
        assert_eq!(s2.current_delay_ms, 200);
    }
}
