//! In-memory rate limit backend for single-process operation.
//!
//! Fast, lock-based backend for rate limiting within a single process.
//! State is not persisted across restarts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::backend::{DomainRateState, RateLimitBackend, RateLimitResult};

/// Configuration for the in-memory backend.
#[derive(Debug, Clone)]
pub struct InMemoryConfig {
    /// Number of unique 403 URLs before triggering rate limit detection.
    pub forbidden_threshold: usize,
    /// Time window for 403 pattern detection.
    pub forbidden_window: Duration,
}

impl Default for InMemoryConfig {
    fn default() -> Self {
        Self {
            forbidden_threshold: 3,
            forbidden_window: Duration::from_secs(60),
        }
    }
}

/// Internal state for a domain.
#[derive(Debug)]
struct DomainEntry {
    current_delay_ms: u64,
    last_request: Option<Instant>,
    consecutive_successes: u32,
    in_backoff: bool,
    total_requests: u64,
    rate_limit_hits: u64,
    /// Recent 403s: (timestamp, url) for pattern detection.
    recent_403s: Vec<(Instant, String)>,
}

impl DomainEntry {
    fn new(base_delay_ms: u64) -> Self {
        Self {
            current_delay_ms: base_delay_ms,
            last_request: None,
            consecutive_successes: 0,
            in_backoff: false,
            total_requests: 0,
            rate_limit_hits: 0,
            recent_403s: Vec::new(),
        }
    }

    fn to_state(&self, domain: &str) -> DomainRateState {
        DomainRateState {
            domain: domain.to_string(),
            current_delay_ms: self.current_delay_ms,
            last_request_at: self.last_request.map(|t| {
                let elapsed = t.elapsed();
                chrono::Utc::now().timestamp_millis() - elapsed.as_millis() as i64
            }),
            consecutive_successes: self.consecutive_successes,
            in_backoff: self.in_backoff,
            total_requests: self.total_requests,
            rate_limit_hits: self.rate_limit_hits,
        }
    }

    fn time_until_ready(&self) -> Duration {
        match self.last_request {
            Some(last) => {
                let elapsed = last.elapsed();
                let delay = Duration::from_millis(self.current_delay_ms);
                if elapsed >= delay {
                    Duration::ZERO
                } else {
                    delay - elapsed
                }
            }
            None => Duration::ZERO,
        }
    }

    /// Count unique URLs in the 403 window.
    fn unique_403_count(&self, window: Duration) -> usize {
        let cutoff = Instant::now() - window;
        let mut urls: Vec<&str> = self
            .recent_403s
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, u)| u.as_str())
            .collect();
        urls.sort();
        urls.dedup();
        urls.len()
    }

    /// Add a 403 record.
    fn add_403(&mut self, url: &str, window: Duration) {
        let now = Instant::now();
        let cutoff = now - window;

        // Remove old entries
        self.recent_403s.retain(|(t, _)| *t >= cutoff);

        // Add new entry
        self.recent_403s.push((now, url.to_string()));
    }
}

/// In-memory rate limit backend.
#[derive(Clone)]
pub struct InMemoryRateLimitBackend {
    domains: Arc<RwLock<HashMap<String, DomainEntry>>>,
    config: InMemoryConfig,
    base_delay_ms: u64,
}

impl InMemoryRateLimitBackend {
    /// Create a new in-memory backend with default config.
    pub fn new(base_delay_ms: u64) -> Self {
        Self::with_config(base_delay_ms, InMemoryConfig::default())
    }

    /// Create a new in-memory backend with custom config.
    pub fn with_config(base_delay_ms: u64, config: InMemoryConfig) -> Self {
        Self {
            domains: Arc::new(RwLock::new(HashMap::new())),
            config,
            base_delay_ms,
        }
    }

    /// Get statistics for all tracked domains.
    pub async fn get_all_stats(&self) -> HashMap<String, DomainRateState> {
        let domains = self.domains.read().await;
        domains
            .iter()
            .map(|(k, v)| (k.clone(), v.to_state(k)))
            .collect()
    }
}

#[async_trait]
impl RateLimitBackend for InMemoryRateLimitBackend {
    async fn get_or_create_domain(
        &self,
        domain: &str,
        base_delay_ms: u64,
    ) -> RateLimitResult<DomainRateState> {
        let domains = self.domains.read().await;
        if let Some(entry) = domains.get(domain) {
            return Ok(entry.to_state(domain));
        }
        drop(domains);

        let mut domains = self.domains.write().await;
        let entry = domains
            .entry(domain.to_string())
            .or_insert_with(|| DomainEntry::new(base_delay_ms));
        Ok(entry.to_state(domain))
    }

    async fn update_domain(&self, state: &DomainRateState) -> RateLimitResult<()> {
        let mut domains = self.domains.write().await;
        if let Some(entry) = domains.get_mut(&state.domain) {
            entry.current_delay_ms = state.current_delay_ms;
            entry.consecutive_successes = state.consecutive_successes;
            entry.in_backoff = state.in_backoff;
            entry.total_requests = state.total_requests;
            entry.rate_limit_hits = state.rate_limit_hits;
        }
        Ok(())
    }

    async fn acquire(&self, domain: &str, base_delay_ms: u64) -> RateLimitResult<Duration> {
        // Get current wait time
        let wait_time = {
            let domains = self.domains.read().await;
            domains
                .get(domain)
                .map(|e| e.time_until_ready())
                .unwrap_or(Duration::ZERO)
        };

        // Update state
        {
            let mut domains = self.domains.write().await;
            let entry = domains
                .entry(domain.to_string())
                .or_insert_with(|| DomainEntry::new(base_delay_ms));
            entry.last_request = Some(Instant::now() + wait_time);
            entry.total_requests += 1;
        }

        Ok(wait_time)
    }

    async fn record_403(&self, domain: &str, url: &str) -> RateLimitResult<()> {
        let mut domains = self.domains.write().await;
        if let Some(entry) = domains.get_mut(domain) {
            entry.add_403(url, self.config.forbidden_window);
        } else {
            let mut entry = DomainEntry::new(self.base_delay_ms);
            entry.add_403(url, self.config.forbidden_window);
            domains.insert(domain.to_string(), entry);
        }
        Ok(())
    }

    async fn get_403_count(&self, domain: &str, window_ms: u64) -> RateLimitResult<usize> {
        let domains = self.domains.read().await;
        let window = Duration::from_millis(window_ms);
        Ok(domains
            .get(domain)
            .map(|e| e.unique_403_count(window))
            .unwrap_or(0))
    }

    async fn clear_403s(&self, domain: &str) -> RateLimitResult<()> {
        let mut domains = self.domains.write().await;
        if let Some(entry) = domains.get_mut(domain) {
            entry.recent_403s.clear();
        }
        Ok(())
    }

    async fn cleanup_expired_403s(&self, window_ms: u64) -> RateLimitResult<u64> {
        let mut domains = self.domains.write().await;
        let window = Duration::from_millis(window_ms);
        let cutoff = Instant::now() - window;
        let mut removed = 0u64;

        for entry in domains.values_mut() {
            let before = entry.recent_403s.len();
            entry.recent_403s.retain(|(t, _)| *t >= cutoff);
            removed += (before - entry.recent_403s.len()) as u64;
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_returns_zero_first_time() {
        let backend = InMemoryRateLimitBackend::new(100);
        let wait = backend.acquire("example.com", 100).await.unwrap();
        assert_eq!(wait, Duration::ZERO);
    }

    #[tokio::test]
    async fn test_acquire_returns_delay_after_request() {
        let backend = InMemoryRateLimitBackend::new(100);

        backend.acquire("example.com", 100).await.unwrap();
        let wait = backend.acquire("example.com", 100).await.unwrap();

        // Should be close to 100ms (minus tiny elapsed time)
        assert!(wait > Duration::from_millis(90));
        assert!(wait <= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_get_or_create_domain() {
        let backend = InMemoryRateLimitBackend::new(100);

        let state = backend
            .get_or_create_domain("example.com", 200)
            .await
            .unwrap();
        assert_eq!(state.domain, "example.com");
        assert_eq!(state.current_delay_ms, 200);
        assert!(!state.in_backoff);
    }

    #[tokio::test]
    async fn test_update_domain() {
        let backend = InMemoryRateLimitBackend::new(100);

        backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();

        let mut state = DomainRateState::new("example.com".to_string(), 100);
        state.current_delay_ms = 500;
        state.in_backoff = true;
        state.rate_limit_hits = 3;

        backend.update_domain(&state).await.unwrap();

        let loaded = backend
            .get_or_create_domain("example.com", 100)
            .await
            .unwrap();
        assert_eq!(loaded.current_delay_ms, 500);
        assert!(loaded.in_backoff);
        assert_eq!(loaded.rate_limit_hits, 3);
    }

    #[tokio::test]
    async fn test_403_tracking() {
        let backend = InMemoryRateLimitBackend::new(100);

        backend
            .record_403("example.com", "https://example.com/a")
            .await
            .unwrap();
        backend
            .record_403("example.com", "https://example.com/b")
            .await
            .unwrap();
        backend
            .record_403("example.com", "https://example.com/a")
            .await
            .unwrap(); // duplicate

        let count = backend.get_403_count("example.com", 60000).await.unwrap();
        assert_eq!(count, 2); // Only 2 unique URLs
    }

    #[tokio::test]
    async fn test_clear_403s() {
        let backend = InMemoryRateLimitBackend::new(100);

        backend
            .record_403("example.com", "https://example.com/a")
            .await
            .unwrap();
        backend
            .record_403("example.com", "https://example.com/b")
            .await
            .unwrap();

        backend.clear_403s("example.com").await.unwrap();

        let count = backend.get_403_count("example.com", 60000).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_get_all_stats() {
        let backend = InMemoryRateLimitBackend::new(100);

        backend.acquire("example.com", 100).await.unwrap();
        backend.acquire("test.org", 100).await.unwrap();

        let stats = backend.get_all_stats().await;
        assert_eq!(stats.len(), 2);
        assert!(stats.contains_key("example.com"));
        assert!(stats.contains_key("test.org"));
    }
}
