//! Rate limiter configuration and types.

use std::time::Duration;

/// Window for detecting 403 rate limit patterns.
pub const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(30);

/// Threshold of unique 403s in window to trigger rate limit detection.
pub const RATE_LIMIT_403_THRESHOLD: usize = 3;

/// Configuration for rate limiting behavior.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Base delay between requests to the same domain.
    pub base_delay: Duration,
    /// Minimum delay (floor).
    pub min_delay: Duration,
    /// Maximum delay (ceiling for backoff).
    pub max_delay: Duration,
    /// Multiplier for exponential backoff on rate limit.
    pub backoff_multiplier: f64,
    /// Multiplier for recovery on success (< 1.0 to decrease delay).
    pub recovery_multiplier: f64,
    /// Number of consecutive successes before reducing delay.
    pub recovery_threshold: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(500),
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            recovery_multiplier: 0.8,
            recovery_threshold: 5,
        }
    }
}

/// Statistics for a domain.
#[derive(Debug, Clone)]
pub struct DomainStats {
    pub current_delay: Duration,
    pub in_backoff: bool,
    pub total_requests: u64,
    pub rate_limit_hits: u64,
}
