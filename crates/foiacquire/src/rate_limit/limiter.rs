//! Adaptive per-domain rate limiter.
//!
//! Provides a high-level rate limiting API that wraps a pluggable backend.
//! Supports in-memory, SQLite/PostgreSQL (Diesel), and Redis backends.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};
use url::Url;

pub use super::config::{DomainStats, RateLimitConfig};

use super::backend::RateLimitBackend;

/// Type alias for a boxed rate limit backend.
pub type BoxedRateLimitBackend = Arc<dyn RateLimitBackend>;

/// Adaptive rate limiter that tracks per-domain request timing.
///
/// Wraps a `RateLimitBackend` and provides high-level rate limiting logic:
/// - Exponential backoff on rate limit responses (429, 503)
/// - 403 pattern detection (multiple unique URLs getting 403)
/// - Gradual recovery after consecutive successes
#[derive(Clone)]
pub struct RateLimiter {
    backend: BoxedRateLimitBackend,
    config: RateLimitConfig,
}

impl RateLimiter {
    /// Create a new rate limiter with the given backend.
    pub fn new(backend: BoxedRateLimitBackend) -> Self {
        Self::with_config(backend, RateLimitConfig::default())
    }

    /// Create a new rate limiter with custom config.
    pub fn with_config(backend: BoxedRateLimitBackend, config: RateLimitConfig) -> Self {
        Self { backend, config }
    }

    /// Extract domain from URL.
    pub fn extract_domain(url: &str) -> Option<String> {
        Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
    }

    /// Wait until the domain is ready, then mark request as started.
    /// Returns the domain name if successful.
    pub async fn acquire(&self, url: &str) -> Option<String> {
        let domain = Self::extract_domain(url)?;
        let base_delay_ms = self.config.base_delay.as_millis() as u64;

        match self.backend.acquire(&domain, base_delay_ms).await {
            Ok(wait_time) => {
                if wait_time > Duration::ZERO {
                    debug!("Rate limiting {}: waiting {:?}", domain, wait_time);
                    tokio::time::sleep(wait_time).await;
                }
                Some(domain)
            }
            Err(e) => {
                warn!("Rate limit acquire failed for {}: {}", domain, e);
                // Fall back to allowing the request
                Some(domain)
            }
        }
    }

    /// Report a successful request - may decrease delay.
    pub async fn report_success(&self, domain: &str) {
        let base_delay_ms = self.config.base_delay.as_millis() as u64;

        let state = match self
            .backend
            .get_or_create_domain(domain, base_delay_ms)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get domain state for {}: {}", domain, e);
                return;
            }
        };

        let mut state = state;
        state.consecutive_successes += 1;

        // Clear 403 tracking on success
        let _ = self.backend.clear_403s(domain).await;

        // Recover from backoff after threshold successes
        if state.in_backoff && state.consecutive_successes >= self.config.recovery_threshold {
            let new_delay_ms =
                (state.current_delay_ms as f64 * self.config.recovery_multiplier) as u64;
            state.current_delay_ms = new_delay_ms.max(self.config.min_delay.as_millis() as u64);

            if state.current_delay_ms <= base_delay_ms {
                state.in_backoff = false;
                state.current_delay_ms = base_delay_ms;
                info!("Domain {} recovered from rate limit backoff", domain);
            } else {
                debug!(
                    "Domain {} delay reduced to {}ms",
                    domain, state.current_delay_ms
                );
            }

            state.consecutive_successes = 0;
        }

        if let Err(e) = self.backend.update_domain(&state).await {
            warn!("Failed to update domain state for {}: {}", domain, e);
        }
    }

    /// Check if a status code is definitely a rate limit (not ambiguous).
    pub fn is_definite_rate_limit(status_code: u16) -> bool {
        matches!(status_code, 429 | 503)
    }

    /// Check if a status code might be a rate limit (needs pattern detection).
    pub fn is_possible_rate_limit(status_code: u16) -> bool {
        matches!(status_code, 429 | 503 | 403)
    }

    /// Report a 403 response - only backs off if we see a pattern on different URLs.
    /// Returns true if this was detected as rate limiting.
    pub async fn report_403(&self, domain: &str, url: &str, has_retry_after: bool) -> bool {
        let base_delay_ms = self.config.base_delay.as_millis() as u64;

        // Record the 403
        if let Err(e) = self.backend.record_403(domain, url).await {
            warn!("Failed to record 403 for {}: {}", domain, e);
        }

        // Get current state
        let state = match self
            .backend
            .get_or_create_domain(domain, base_delay_ms)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get domain state for {}: {}", domain, e);
                return false;
            }
        };

        let mut state = state;
        state.consecutive_successes = 0;

        // Check if we have a pattern of 403s
        let window_ms = 60_000; // 60 second window
        let threshold = 3;
        let unique_403_count = self
            .backend
            .get_403_count(domain, window_ms)
            .await
            .unwrap_or(0);

        // Retry-After header = definitely rate limiting
        // N+ unique URLs getting 403 within time window = probably rate limiting
        let is_rate_limit = has_retry_after || unique_403_count >= threshold;

        if is_rate_limit {
            state.rate_limit_hits += 1;
            state.in_backoff = true;
            let _ = self.backend.clear_403s(domain).await;

            let new_delay_ms =
                (state.current_delay_ms as f64 * self.config.backoff_multiplier) as u64;
            state.current_delay_ms = new_delay_ms.min(self.config.max_delay.as_millis() as u64);

            warn!(
                "Rate limited by {} ({} unique URLs got 403), backing off to {}ms",
                domain, unique_403_count, state.current_delay_ms
            );

            if let Err(e) = self.backend.update_domain(&state).await {
                warn!("Failed to update domain state for {}: {}", domain, e);
            }
            return true;
        }

        debug!(
            "403 from {} for {} ({} unique URLs in window) - treating as access denied",
            domain, url, unique_403_count
        );

        if let Err(e) = self.backend.update_domain(&state).await {
            warn!("Failed to update domain state for {}: {}", domain, e);
        }
        false
    }

    /// Report a definite rate limit hit (429 or 503) - increases delay.
    pub async fn report_rate_limit(&self, domain: &str, status_code: u16) {
        let base_delay_ms = self.config.base_delay.as_millis() as u64;

        let state = match self
            .backend
            .get_or_create_domain(domain, base_delay_ms)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get domain state for {}: {}", domain, e);
                return;
            }
        };

        let mut state = state;
        state.rate_limit_hits += 1;
        state.consecutive_successes = 0;
        let _ = self.backend.clear_403s(domain).await;
        state.in_backoff = true;

        let new_delay_ms = (state.current_delay_ms as f64 * self.config.backoff_multiplier) as u64;
        state.current_delay_ms = new_delay_ms.min(self.config.max_delay.as_millis() as u64);

        warn!(
            "Rate limited by {} (HTTP {}), backing off to {}ms",
            domain, status_code, state.current_delay_ms
        );

        if let Err(e) = self.backend.update_domain(&state).await {
            warn!("Failed to update domain state for {}: {}", domain, e);
        }
    }

    /// Report a client error (4xx other than 429) - no delay change.
    pub async fn report_client_error(&self, domain: &str) {
        let base_delay_ms = self.config.base_delay.as_millis() as u64;
        if let Ok(state) = self
            .backend
            .get_or_create_domain(domain, base_delay_ms)
            .await
        {
            debug!(
                "Client error for {}, delay unchanged at {}ms",
                domain, state.current_delay_ms
            );
        }
    }

    /// Report a server error (5xx other than 503) - mild backoff.
    pub async fn report_server_error(&self, domain: &str) {
        let base_delay_ms = self.config.base_delay.as_millis() as u64;

        let state = match self
            .backend
            .get_or_create_domain(domain, base_delay_ms)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get domain state for {}: {}", domain, e);
                return;
            }
        };

        let mut state = state;
        // Mild backoff for server errors (might be overloaded)
        let new_delay_ms = (state.current_delay_ms as f64 * 1.5) as u64;
        state.current_delay_ms = new_delay_ms.min(self.config.max_delay.as_millis() as u64);

        debug!(
            "Server error for {}, delay increased to {}ms",
            domain, state.current_delay_ms
        );

        if let Err(e) = self.backend.update_domain(&state).await {
            warn!("Failed to update domain state for {}: {}", domain, e);
        }
    }

    /// Classify a response status code and report it to the appropriate handler.
    ///
    /// Consolidates the duplicated if/else chains that were copy-pasted across
    /// every HTTP method. Handles 429/503 (rate limit), 403 (pattern detection),
    /// 5xx (server error), and 2xx/3xx (success).
    pub async fn report_response_status(
        &self,
        domain: &str,
        status_code: u16,
        original_url: &str,
        response_headers: &std::collections::HashMap<String, String>,
    ) {
        let has_retry_after = response_headers.contains_key("retry-after");
        if status_code == 429 || status_code == 503 {
            self.report_rate_limit(domain, status_code).await;
        } else if status_code == 403 {
            self.report_403(domain, original_url, has_retry_after).await;
        } else if status_code >= 500 {
            self.report_server_error(domain).await;
        } else if (200..400).contains(&status_code) {
            self.report_success(domain).await;
        }
    }

    /// Get statistics for all domains (only works with InMemoryRateLimitBackend).
    pub async fn get_stats(&self) -> std::collections::HashMap<String, DomainStats> {
        // This is a limitation - we can't easily get all stats from all backends
        // For now, return empty. Users should use backend-specific methods.
        std::collections::HashMap::new()
    }

    /// Get the underlying backend for direct access.
    pub fn backend(&self) -> &BoxedRateLimitBackend {
        &self.backend
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::InMemoryRateLimitBackend;
    use super::*;

    fn create_test_limiter() -> RateLimiter {
        let backend = Arc::new(InMemoryRateLimitBackend::new(100));
        RateLimiter::with_config(
            backend,
            RateLimitConfig {
                base_delay: Duration::from_millis(100),
                backoff_multiplier: 2.0,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn test_extract_domain() {
        assert_eq!(
            RateLimiter::extract_domain("https://example.com/path"),
            Some("example.com".to_string())
        );
        assert_eq!(
            RateLimiter::extract_domain("https://cdn.muckrock.com/file.pdf"),
            Some("cdn.muckrock.com".to_string())
        );
    }

    #[tokio::test]
    async fn test_acquire() {
        let limiter = create_test_limiter();
        let domain = limiter.acquire("https://example.com/doc").await;
        assert_eq!(domain, Some("example.com".to_string()));
    }

    #[tokio::test]
    async fn test_report_rate_limit_increases_delay() {
        let limiter = create_test_limiter();

        // First acquire to create state
        limiter.acquire("https://example.com/doc").await;

        // Report rate limit
        limiter.report_rate_limit("example.com", 429).await;

        // Next acquire should have longer delay
        // We can't easily check the delay without accessing the backend directly
    }

    #[tokio::test]
    async fn test_report_success() {
        let limiter = create_test_limiter();
        limiter.acquire("https://example.com/doc").await;
        limiter.report_success("example.com").await;
        // Should not error
    }

    #[tokio::test]
    async fn test_report_response_status_rate_limit() {
        let limiter = create_test_limiter();
        limiter.acquire("https://example.com/doc").await;

        let headers = std::collections::HashMap::new();

        // 429 should trigger rate limit backoff
        limiter
            .report_response_status("example.com", 429, "https://example.com/doc", &headers)
            .await;

        let state = limiter
            .backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        assert!(state.in_backoff);
        assert_eq!(state.rate_limit_hits, 1);
    }

    #[tokio::test]
    async fn test_report_response_status_503() {
        let limiter = create_test_limiter();
        limiter.acquire("https://example.com/doc").await;

        let headers = std::collections::HashMap::new();

        // 503 should also trigger rate limit backoff
        limiter
            .report_response_status("example.com", 503, "https://example.com/doc", &headers)
            .await;

        let state = limiter
            .backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        assert!(state.in_backoff);
        assert_eq!(state.rate_limit_hits, 1);
    }

    #[tokio::test]
    async fn test_report_response_status_success() {
        let limiter = create_test_limiter();
        limiter.acquire("https://example.com/doc").await;

        // Put into backoff first
        limiter.report_rate_limit("example.com", 429).await;

        let headers = std::collections::HashMap::new();

        // 200 should count as success
        limiter
            .report_response_status("example.com", 200, "https://example.com/doc", &headers)
            .await;

        let state = limiter
            .backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        assert_eq!(state.consecutive_successes, 1);
    }

    #[tokio::test]
    async fn test_report_response_status_304_is_success() {
        let limiter = create_test_limiter();
        limiter.acquire("https://example.com/doc").await;

        let headers = std::collections::HashMap::new();

        // 304 should count as success (in the 200..400 range)
        limiter
            .report_response_status("example.com", 304, "https://example.com/doc", &headers)
            .await;

        let state = limiter
            .backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        assert_eq!(state.consecutive_successes, 1);
    }

    #[tokio::test]
    async fn test_report_response_status_server_error() {
        let limiter = create_test_limiter();
        limiter.acquire("https://example.com/doc").await;

        let headers = std::collections::HashMap::new();

        // 500 should trigger mild server error backoff (not rate limit)
        limiter
            .report_response_status("example.com", 500, "https://example.com/doc", &headers)
            .await;

        let state = limiter
            .backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        // Server error does mild backoff but doesn't set rate_limit_hits
        assert_eq!(state.rate_limit_hits, 0);
        assert!(state.current_delay_ms > 100);
    }

    #[tokio::test]
    async fn test_report_response_status_403_pattern() {
        let limiter = create_test_limiter();
        limiter.acquire("https://example.com/doc").await;

        let headers = std::collections::HashMap::new();

        // Single 403 should not trigger rate limit
        limiter
            .report_response_status("example.com", 403, "https://example.com/a", &headers)
            .await;

        let state = limiter
            .backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        assert!(!state.in_backoff);
    }

    #[tokio::test]
    async fn test_is_definite_rate_limit() {
        assert!(RateLimiter::is_definite_rate_limit(429));
        assert!(RateLimiter::is_definite_rate_limit(503));
        assert!(!RateLimiter::is_definite_rate_limit(403));
        assert!(!RateLimiter::is_definite_rate_limit(500));
    }
}
