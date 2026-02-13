//! Browser selection strategies for multi-browser pools.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

pub use crate::config::browser::SelectionStrategyType;

/// Strategy for selecting which browser to use for a request.
pub trait BrowserSelectionStrategy: Send + Sync {
    /// Select a browser index for the given URL.
    ///
    /// # Arguments
    /// * `url` - The URL being fetched (used by per-domain strategy)
    /// * `count` - Total number of browsers in the pool
    /// * `healthy` - Slice indicating which browsers are currently healthy
    ///
    /// # Returns
    /// Some(index) if a healthy browser is available, None if all are unhealthy.
    fn select(&self, url: &str, count: usize, healthy: &[bool]) -> Option<usize>;
}

/// Round-robin selection - rotates through browsers consecutively.
pub struct RoundRobinStrategy {
    counter: AtomicUsize,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl BrowserSelectionStrategy for RoundRobinStrategy {
    fn select(&self, _url: &str, count: usize, healthy: &[bool]) -> Option<usize> {
        if count == 0 {
            return None;
        }

        let start = self.counter.fetch_add(1, Ordering::Relaxed) % count;

        // Find next healthy browser starting from round-robin position
        for i in 0..count {
            let idx = (start + i) % count;
            if healthy.get(idx).copied().unwrap_or(false) {
                return Some(idx);
            }
        }

        None
    }
}

/// Random selection - picks a random browser each time.
pub struct RandomStrategy {
    counter: AtomicUsize,
}

impl RandomStrategy {
    pub fn new() -> Self {
        // Use system time as a simple seed for pseudo-random starting point
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0);
        Self {
            counter: AtomicUsize::new(seed),
        }
    }

    /// Simple LCG-based pseudo-random number generation
    fn next_random(&self) -> usize {
        // Linear congruential generator constants (same as glibc)
        const A: usize = 1103515245;
        const C: usize = 12345;
        loop {
            let current = self.counter.load(Ordering::Relaxed);
            let next = current.wrapping_mul(A).wrapping_add(C);
            if self
                .counter
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
        }
    }
}

impl Default for RandomStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl BrowserSelectionStrategy for RandomStrategy {
    fn select(&self, _url: &str, count: usize, healthy: &[bool]) -> Option<usize> {
        if count == 0 {
            return None;
        }

        // Collect healthy indices
        let healthy_indices: Vec<usize> = healthy
            .iter()
            .enumerate()
            .filter_map(|(i, &h)| if h { Some(i) } else { None })
            .collect();

        if healthy_indices.is_empty() {
            return None;
        }

        // Random selection from healthy browsers
        let random_idx = self.next_random() % healthy_indices.len();
        Some(healthy_indices[random_idx])
    }
}

/// Per-domain selection - consistent hashing so same domain always uses same browser.
/// This maintains session/cookie state per domain across requests.
pub struct PerDomainStrategy;

impl PerDomainStrategy {
    fn extract_domain(url: &str) -> String {
        url::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
            .unwrap_or_default()
    }

    fn hash_domain(domain: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        domain.hash(&mut hasher);
        hasher.finish()
    }
}

impl BrowserSelectionStrategy for PerDomainStrategy {
    fn select(&self, url: &str, count: usize, healthy: &[bool]) -> Option<usize> {
        if count == 0 {
            return None;
        }

        let domain = Self::extract_domain(url);
        let hash = Self::hash_domain(&domain);
        let preferred = (hash as usize) % count;

        // Try preferred browser first if healthy
        if healthy.get(preferred).copied().unwrap_or(false) {
            return Some(preferred);
        }

        // Fall back to next healthy browser (consistent ordering from preferred)
        for i in 1..count {
            let idx = (preferred + i) % count;
            if healthy.get(idx).copied().unwrap_or(false) {
                return Some(idx);
            }
        }

        None
    }
}

impl SelectionStrategyType {
    /// Create a boxed strategy instance.
    pub fn create_strategy(&self) -> Box<dyn BrowserSelectionStrategy> {
        match self {
            Self::RoundRobin => Box::new(RoundRobinStrategy::new()),
            Self::Random => Box::new(RandomStrategy::new()),
            Self::PerDomain => Box::new(PerDomainStrategy),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_cycles_through_browsers() {
        let strategy = RoundRobinStrategy::new();
        let healthy = [true, true, true];

        let first = strategy.select("http://example.com", 3, &healthy);
        let second = strategy.select("http://example.com", 3, &healthy);
        let third = strategy.select("http://example.com", 3, &healthy);
        let fourth = strategy.select("http://example.com", 3, &healthy);

        assert_eq!(first, Some(0));
        assert_eq!(second, Some(1));
        assert_eq!(third, Some(2));
        assert_eq!(fourth, Some(0)); // Wraps around
    }

    #[test]
    fn round_robin_skips_unhealthy() {
        let strategy = RoundRobinStrategy::new();
        let healthy = [false, true, false];

        let first = strategy.select("http://example.com", 3, &healthy);
        let second = strategy.select("http://example.com", 3, &healthy);

        assert_eq!(first, Some(1));
        assert_eq!(second, Some(1)); // Only healthy option
    }

    #[test]
    fn round_robin_returns_none_when_all_unhealthy() {
        let strategy = RoundRobinStrategy::new();
        let healthy = [false, false, false];

        let result = strategy.select("http://example.com", 3, &healthy);
        assert_eq!(result, None);
    }

    #[test]
    fn random_only_selects_healthy() {
        let strategy = RandomStrategy::new();
        let healthy = [false, true, false];

        for _ in 0..100 {
            let result = strategy.select("http://example.com", 3, &healthy);
            assert_eq!(result, Some(1));
        }
    }

    #[test]
    fn per_domain_is_consistent() {
        let strategy = PerDomainStrategy;
        let healthy = [true, true, true];

        let url1 = "https://example.com/page1";
        let url2 = "https://example.com/page2";
        let url3 = "https://other.com/page";

        let result1a = strategy.select(url1, 3, &healthy);
        let result1b = strategy.select(url1, 3, &healthy);
        let result2 = strategy.select(url2, 3, &healthy);
        let result3 = strategy.select(url3, 3, &healthy);

        // Same domain should get same browser
        assert_eq!(result1a, result1b);
        assert_eq!(result1a, result2);

        // Different domain may get different browser (depends on hash)
        // Just verify it returns something
        assert!(result3.is_some());
    }

    #[test]
    fn per_domain_falls_back_when_preferred_unhealthy() {
        let strategy = PerDomainStrategy;
        let url = "https://example.com/page";

        // First, find which browser this domain prefers
        let all_healthy = [true, true, true];
        let preferred = strategy.select(url, 3, &all_healthy).unwrap();

        // Now mark that browser as unhealthy
        let mut healthy = [true, true, true];
        healthy[preferred] = false;

        let fallback = strategy.select(url, 3, &healthy);
        assert!(fallback.is_some());
        assert_ne!(fallback, Some(preferred));
    }

    #[test]
    fn all_strategies_return_none_for_empty_pool() {
        let healthy: [bool; 0] = [];

        assert_eq!(
            RoundRobinStrategy::new().select("http://example.com", 0, &healthy),
            None
        );
        assert_eq!(
            RandomStrategy::new().select("http://example.com", 0, &healthy),
            None
        );
        assert_eq!(
            PerDomainStrategy.select("http://example.com", 0, &healthy),
            None
        );
    }

    #[test]
    fn all_strategies_work_with_single_healthy_browser() {
        let healthy = [true];

        assert_eq!(
            RoundRobinStrategy::new().select("http://example.com", 1, &healthy),
            Some(0)
        );
        assert_eq!(
            RandomStrategy::new().select("http://example.com", 1, &healthy),
            Some(0)
        );
        assert_eq!(
            PerDomainStrategy.select("http://example.com", 1, &healthy),
            Some(0)
        );
    }

    #[test]
    fn all_strategies_return_none_for_single_unhealthy_browser() {
        let healthy = [false];

        assert_eq!(
            RoundRobinStrategy::new().select("http://example.com", 1, &healthy),
            None
        );
        assert_eq!(
            RandomStrategy::new().select("http://example.com", 1, &healthy),
            None
        );
        assert_eq!(
            PerDomainStrategy.select("http://example.com", 1, &healthy),
            None
        );
    }

    #[test]
    fn round_robin_with_alternating_health() {
        let strategy = RoundRobinStrategy::new();
        let healthy = [true, false, true, false, true];

        // Should cycle through only healthy indices: 0, 2, 4
        let results: Vec<_> = (0..6)
            .map(|_| strategy.select("http://example.com", 5, &healthy))
            .collect();

        // Verify all results are healthy indices
        for result in &results {
            let idx = result.unwrap();
            assert!(healthy[idx], "Selected unhealthy browser at index {}", idx);
        }
    }

    #[test]
    fn random_distributes_across_multiple_healthy() {
        let strategy = RandomStrategy::new();
        let healthy = [true, true, true, true];
        let mut seen = [false; 4];

        // With 1000 iterations, we should see all healthy browsers selected
        for _ in 0..1000 {
            if let Some(idx) = strategy.select("http://example.com", 4, &healthy) {
                seen[idx] = true;
            }
        }

        // All browsers should have been selected at least once
        assert!(
            seen.iter().all(|&s| s),
            "Random strategy didn't distribute across all browsers: {:?}",
            seen
        );
    }

    #[test]
    fn random_returns_none_when_all_unhealthy() {
        let strategy = RandomStrategy::new();
        let healthy = [false, false, false];

        assert_eq!(strategy.select("http://example.com", 3, &healthy), None);
    }

    #[test]
    fn per_domain_distributes_different_domains() {
        let strategy = PerDomainStrategy;
        let healthy = [true, true, true, true, true];

        // Use many different domains to increase chance of distribution
        let domains = [
            "https://a.com/page",
            "https://b.com/page",
            "https://c.com/page",
            "https://d.com/page",
            "https://e.com/page",
            "https://f.com/page",
            "https://g.com/page",
            "https://h.com/page",
            "https://i.com/page",
            "https://j.com/page",
        ];

        let mut seen = [false; 5];
        for domain in &domains {
            if let Some(idx) = strategy.select(domain, 5, &healthy) {
                seen[idx] = true;
            }
        }

        // With 10 different domains across 5 browsers, we should see multiple browsers used
        let used_count = seen.iter().filter(|&&s| s).count();
        assert!(
            used_count >= 2,
            "Per-domain strategy should distribute across browsers, only used {}",
            used_count
        );
    }

    #[test]
    fn per_domain_handles_malformed_urls() {
        let strategy = PerDomainStrategy;
        let healthy = [true, true, true];

        // Malformed URLs should still return a result (using empty domain hash)
        let result = strategy.select("not-a-valid-url", 3, &healthy);
        assert!(result.is_some());

        // Empty URL
        let result = strategy.select("", 3, &healthy);
        assert!(result.is_some());
    }

    #[test]
    fn per_domain_same_host_different_paths_same_browser() {
        let strategy = PerDomainStrategy;
        let healthy = [true, true, true, true, true];

        let urls = [
            "https://example.com/",
            "https://example.com/page1",
            "https://example.com/page2/subpage",
            "https://example.com/api/v1/users",
            "https://example.com:443/with-port",
        ];

        let first = strategy.select(urls[0], 5, &healthy);
        for url in &urls[1..] {
            assert_eq!(
                strategy.select(url, 5, &healthy),
                first,
                "URL {} should map to same browser as {}",
                url,
                urls[0]
            );
        }
    }

    #[test]
    fn per_domain_subdomains_are_different() {
        let strategy = PerDomainStrategy;
        let healthy = [true, true, true, true, true, true, true, true, true, true];

        // Subdomains should be treated as different domains
        let www = strategy.select("https://www.example.com/page", 10, &healthy);
        let api = strategy.select("https://api.example.com/page", 10, &healthy);
        let bare = strategy.select("https://example.com/page", 10, &healthy);

        // At least some should differ (hash collision possible but unlikely with 10 buckets)
        let all_same = www == api && api == bare;
        // Note: This could theoretically fail due to hash collisions, but very unlikely
        assert!(
            !all_same,
            "Subdomains should generally map to different browsers"
        );
    }

    #[test]
    fn strategy_type_creates_correct_strategy() {
        let healthy = [true, true, true];

        // Round-robin should increment
        let rr = SelectionStrategyType::RoundRobin.create_strategy();
        let first = rr.select("http://a.com", 3, &healthy);
        let second = rr.select("http://a.com", 3, &healthy);
        assert_ne!(first, second, "Round-robin should cycle");

        // Per-domain should be consistent
        let pd = SelectionStrategyType::PerDomain.create_strategy();
        let first = pd.select("http://a.com", 3, &healthy);
        let second = pd.select("http://a.com", 3, &healthy);
        assert_eq!(first, second, "Per-domain should be consistent");
    }

}
