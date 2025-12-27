//! SQLite-backed rate limiter for multi-process coordination.
//!
//! This module is currently stubbed out pending Diesel migration.
//! Rate limiting still works in-memory, but state is not persisted across runs.

use std::time::Duration;

use async_trait::async_trait;

use super::rate_limit_backend::{DomainRateState, RateLimitBackend, RateLimitResult};

/// SQLx-backed rate limit storage (currently stubbed).
#[derive(Clone)]
pub struct SqliteRateLimitBackend {
    _base_delay_ms: u64,
}

impl SqliteRateLimitBackend {
    /// Create a new SQLite rate limit backend (stubbed).
    pub fn new_stub(base_delay_ms: u64) -> Self {
        Self {
            _base_delay_ms: base_delay_ms,
        }
    }
}

#[async_trait]
impl RateLimitBackend for SqliteRateLimitBackend {
    async fn get_or_create_domain(
        &self,
        domain: &str,
        base_delay_ms: u64,
    ) -> RateLimitResult<DomainRateState> {
        // Return a fresh state (no persistence)
        Ok(DomainRateState::new(domain.to_string(), base_delay_ms))
    }

    async fn update_domain(&self, _state: &DomainRateState) -> RateLimitResult<()> {
        // No-op (no persistence)
        Ok(())
    }

    async fn acquire(&self, _domain: &str, _base_delay_ms: u64) -> RateLimitResult<Duration> {
        // No waiting (in-memory rate limiting still works via RateLimiter)
        Ok(Duration::ZERO)
    }

    async fn record_403(&self, _domain: &str, _url: &str) -> RateLimitResult<()> {
        // No-op (no persistence)
        Ok(())
    }

    async fn get_403_count(&self, _domain: &str, _window_ms: u64) -> RateLimitResult<usize> {
        // Always return 0 (no persistence)
        Ok(0)
    }

    async fn clear_403s(&self, _domain: &str) -> RateLimitResult<()> {
        // No-op (no persistence)
        Ok(())
    }

    async fn cleanup_expired_403s(&self, _window_ms: u64) -> RateLimitResult<u64> {
        // No-op (no persistence)
        Ok(0)
    }
}
