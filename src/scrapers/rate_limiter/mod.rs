//! Adaptive per-domain rate limiter.
//!
//! Tracks request timing per domain and adapts delays based on responses.
//! Backs off on 429/503, gradually recovers on success.

mod config;
mod domain_state;
mod persistence;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use url::Url;

pub use config::{DomainStats, RateLimitConfig};
use domain_state::DomainState;
pub use persistence::{load_rate_limit_state, save_rate_limit_state};

/// Adaptive rate limiter that tracks per-domain request timing.
#[derive(Debug)]
pub struct RateLimiter {
    pub(crate) config: RateLimitConfig,
    pub(crate) domains: Arc<RwLock<HashMap<String, DomainState>>>,
}

impl RateLimiter {
    /// Create a new rate limiter with default config.
    pub fn new() -> Self {
        Self::with_config(RateLimitConfig::default())
    }

    /// Create a new rate limiter with custom config.
    pub fn with_config(config: RateLimitConfig) -> Self {
        Self {
            config,
            domains: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Extract domain from URL.
    pub fn extract_domain(url: &str) -> Option<String> {
        Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
    }

    /// Wait until the domain is ready, then mark request as started.
    pub async fn acquire(&self, url: &str) -> Option<String> {
        let domain = Self::extract_domain(url)?;

        // Get or create domain state
        let wait_time = {
            let domains = self.domains.read().await;
            domains
                .get(&domain)
                .map(|s| s.time_until_ready())
                .unwrap_or(Duration::ZERO)
        };

        // Wait if needed
        if wait_time > Duration::ZERO {
            debug!("Rate limiting {}: waiting {:?}", domain, wait_time);
            tokio::time::sleep(wait_time).await;
        }

        // Mark request as started
        {
            let mut domains = self.domains.write().await;
            let state = domains
                .entry(domain.clone())
                .or_insert_with(|| DomainState::new(self.config.base_delay));
            state.last_request = Some(Instant::now());
            state.total_requests += 1;
        }

        Some(domain)
    }

    /// Report a successful request - may decrease delay.
    pub async fn report_success(&self, domain: &str) {
        let mut domains = self.domains.write().await;
        if let Some(state) = domains.get_mut(domain) {
            state.consecutive_successes += 1;
            state.clear_403_tracking(); // Reset 403 tracking on success

            // Recover from backoff after threshold successes
            if state.in_backoff && state.consecutive_successes >= self.config.recovery_threshold {
                let new_delay = Duration::from_secs_f64(
                    state.current_delay.as_secs_f64() * self.config.recovery_multiplier,
                );
                state.current_delay = new_delay.max(self.config.min_delay);

                if state.current_delay <= self.config.base_delay {
                    state.in_backoff = false;
                    state.current_delay = self.config.base_delay;
                    info!("Domain {} recovered from rate limit backoff", domain);
                } else {
                    debug!(
                        "Domain {} delay reduced to {:?}",
                        domain, state.current_delay
                    );
                }

                state.consecutive_successes = 0;
            }
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
        let mut domains = self.domains.write().await;
        if let Some(state) = domains.get_mut(domain) {
            let is_pattern_rate_limit = state.add_403(url);
            state.consecutive_successes = 0;

            // Retry-After header = definitely rate limiting
            // N+ unique URLs getting 403 within time window = probably rate limiting
            let is_rate_limit = has_retry_after || is_pattern_rate_limit;

            if is_rate_limit {
                let (count, window) = state.get_403_stats();
                state.rate_limit_hits += 1;
                state.in_backoff = true;
                state.clear_403_tracking(); // Clear after confirming rate limit

                let new_delay = Duration::from_secs_f64(
                    state.current_delay.as_secs_f64() * self.config.backoff_multiplier,
                );
                state.current_delay = new_delay.min(self.config.max_delay);

                warn!(
                    "Rate limited by {} ({} unique URLs got 403 in {:?}), backing off to {:?}",
                    domain, count, window, state.current_delay
                );
                return true;
            } else {
                let unique_count = state.unique_403_count();
                debug!(
                    "403 from {} for {} ({} unique URLs in window) - treating as access denied",
                    domain, url, unique_count
                );
            }
        }
        false
    }

    /// Report a definite rate limit hit (429 or 503) - increases delay.
    pub async fn report_rate_limit(&self, domain: &str, status_code: u16) {
        let mut domains = self.domains.write().await;
        if let Some(state) = domains.get_mut(domain) {
            state.rate_limit_hits += 1;
            state.consecutive_successes = 0;
            state.clear_403_tracking(); // Reset 403 tracking
            state.in_backoff = true;

            let new_delay = Duration::from_secs_f64(
                state.current_delay.as_secs_f64() * self.config.backoff_multiplier,
            );
            state.current_delay = new_delay.min(self.config.max_delay);

            warn!(
                "Rate limited by {} (HTTP {}), backing off to {:?}",
                domain, status_code, state.current_delay
            );
        }
    }

    /// Report a client error (4xx other than 429) - no delay change.
    pub async fn report_client_error(&self, domain: &str) {
        // Client errors don't affect rate limiting
        let domains = self.domains.read().await;
        if let Some(state) = domains.get(domain) {
            debug!(
                "Client error for {}, delay unchanged at {:?}",
                domain, state.current_delay
            );
        }
    }

    /// Report a server error (5xx other than 503) - mild backoff.
    pub async fn report_server_error(&self, domain: &str) {
        let mut domains = self.domains.write().await;
        if let Some(state) = domains.get_mut(domain) {
            // Mild backoff for server errors (might be overloaded)
            let new_delay = Duration::from_secs_f64(state.current_delay.as_secs_f64() * 1.5);
            state.current_delay = new_delay.min(self.config.max_delay);
            debug!(
                "Server error for {}, delay increased to {:?}",
                domain, state.current_delay
            );
        }
    }

    /// Check if a domain is currently ready for requests.
    pub async fn is_domain_ready(&self, url: &str) -> bool {
        let domain = match Self::extract_domain(url) {
            Some(d) => d,
            None => return true,
        };

        let domains = self.domains.read().await;
        domains.get(&domain).map(|s| s.is_ready()).unwrap_or(true)
    }

    /// Get time until domain is ready.
    pub async fn time_until_ready(&self, url: &str) -> Duration {
        let domain = match Self::extract_domain(url) {
            Some(d) => d,
            None => return Duration::ZERO,
        };

        let domains = self.domains.read().await;
        domains
            .get(&domain)
            .map(|s| s.time_until_ready())
            .unwrap_or(Duration::ZERO)
    }

    /// Get statistics for all domains.
    pub async fn get_stats(&self) -> HashMap<String, DomainStats> {
        let domains = self.domains.read().await;
        domains
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    DomainStats {
                        current_delay: v.current_delay,
                        in_backoff: v.in_backoff,
                        total_requests: v.total_requests,
                        rate_limit_hits: v.rate_limit_hits,
                    },
                )
            })
            .collect()
    }

    /// Find the domain that's ready soonest from a list of URLs.
    pub async fn find_ready_url<'a>(&self, urls: &'a [String]) -> Option<&'a String> {
        let domains = self.domains.read().await;

        let mut best_url: Option<&String> = None;
        let mut best_wait = Duration::MAX;

        for url in urls {
            let domain = match Self::extract_domain(url) {
                Some(d) => d,
                None => continue,
            };

            let wait = domains
                .get(&domain)
                .map(|s| s.time_until_ready())
                .unwrap_or(Duration::ZERO);

            if wait < best_wait {
                best_wait = wait;
                best_url = Some(url);

                // If we found one that's ready now, use it
                if wait == Duration::ZERO {
                    break;
                }
            }
        }

        best_url
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RateLimiter {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            domains: self.domains.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn test_backoff_on_rate_limit() {
        let limiter = RateLimiter::with_config(RateLimitConfig {
            base_delay: Duration::from_millis(100),
            backoff_multiplier: 2.0,
            ..Default::default()
        });

        // First request
        limiter.acquire("https://example.com/1").await;

        // Report rate limit
        limiter.report_rate_limit("example.com", 429).await;

        // Check delay increased
        let stats = limiter.get_stats().await;
        let domain_stats = stats.get("example.com").unwrap();
        assert!(domain_stats.current_delay >= Duration::from_millis(200));
        assert!(domain_stats.in_backoff);
    }
}
