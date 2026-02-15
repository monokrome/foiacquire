//! Pluggable backend trait for rate limiting storage.
//!
//! Allows swapping between in-memory (single process), SQLite (multi-process),
//! or external backends like Redis (distributed).

#![allow(dead_code)]

use async_trait::async_trait;
use std::time::Duration;

/// Result type for rate limit operations.
pub type RateLimitResult<T> = Result<T, RateLimitError>;

/// Errors from rate limit backend operations.
#[derive(Debug, thiserror::Error)]
pub enum RateLimitError {
    #[error("Database error: {0}")]
    Database(String),
    #[error("Backend unavailable: {0}")]
    Unavailable(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<diesel::result::Error> for RateLimitError {
    fn from(e: diesel::result::Error) -> Self {
        RateLimitError::Database(e.to_string())
    }
}

/// State for a domain's rate limiting.
#[derive(Debug, Clone)]
pub struct DomainRateState {
    pub domain: String,
    pub current_delay_ms: u64,
    pub last_request_at: Option<i64>, // Unix timestamp ms
    pub consecutive_successes: u32,
    pub in_backoff: bool,
    pub total_requests: u64,
    pub rate_limit_hits: u64,
}

impl DomainRateState {
    pub fn new(domain: String, base_delay_ms: u64) -> Self {
        Self {
            domain,
            current_delay_ms: base_delay_ms,
            last_request_at: None,
            consecutive_successes: 0,
            in_backoff: false,
            total_requests: 0,
            rate_limit_hits: 0,
        }
    }

    pub fn current_delay(&self) -> Duration {
        Duration::from_millis(self.current_delay_ms)
    }

    pub fn time_until_ready(&self) -> Duration {
        match self.last_request_at {
            Some(last_ms) => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let elapsed_ms = (now_ms - last_ms).max(0) as u64;
                if elapsed_ms >= self.current_delay_ms {
                    Duration::ZERO
                } else {
                    Duration::from_millis(self.current_delay_ms - elapsed_ms)
                }
            }
            None => Duration::ZERO,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.time_until_ready() == Duration::ZERO
    }
}

/// A 403 response record for pattern detection.
#[derive(Debug, Clone)]
pub struct ForbiddenRecord {
    pub domain: String,
    pub url: String,
    pub timestamp_ms: i64,
}

/// Trait for rate limit storage backends.
///
/// Implementations must be thread-safe and handle concurrent access.
#[async_trait]
pub trait RateLimitBackend: Send + Sync {
    /// Get or create state for a domain.
    async fn get_or_create_domain(
        &self,
        domain: &str,
        base_delay_ms: u64,
    ) -> RateLimitResult<DomainRateState>;

    /// Update domain state after a request.
    async fn update_domain(&self, state: &DomainRateState) -> RateLimitResult<()>;

    /// Atomically acquire a request slot for a domain.
    /// Returns the wait time (0 if ready now), and marks the request as started.
    async fn acquire(&self, domain: &str, base_delay_ms: u64) -> RateLimitResult<Duration>;

    /// Record a 403 response for pattern detection.
    async fn record_403(&self, domain: &str, url: &str) -> RateLimitResult<()>;

    /// Get count of unique URLs that got 403 within the time window.
    async fn get_403_count(&self, domain: &str, window_ms: u64) -> RateLimitResult<usize>;

    /// Clear 403 records for a domain.
    async fn clear_403s(&self, domain: &str) -> RateLimitResult<()>;

    /// Clean up expired 403 records (housekeeping).
    async fn cleanup_expired_403s(&self, window_ms: u64) -> RateLimitResult<u64>;
}
