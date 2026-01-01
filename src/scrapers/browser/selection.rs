//! Browser selection strategies for multi-browser pools.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::Rng;
use serde::{Deserialize, Serialize};

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
pub struct RandomStrategy;

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
        let random_idx = rand::rng().random_range(0..healthy_indices.len());
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

/// Selection strategy type enum for config/CLI.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelectionStrategyType {
    /// Rotate through browsers consecutively
    #[default]
    RoundRobin,
    /// Random selection each request
    Random,
    /// Consistent hash by domain (sticky)
    PerDomain,
}

impl SelectionStrategyType {
    /// Create a boxed strategy instance.
    pub fn create_strategy(&self) -> Box<dyn BrowserSelectionStrategy> {
        match self {
            Self::RoundRobin => Box::new(RoundRobinStrategy::new()),
            Self::Random => Box::new(RandomStrategy),
            Self::PerDomain => Box::new(PerDomainStrategy),
        }
    }

    /// Parse from string (for CLI/env var).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().replace('-', "").as_str() {
            "roundrobin" => Some(Self::RoundRobin),
            "random" => Some(Self::Random),
            "perdomain" => Some(Self::PerDomain),
            _ => None,
        }
    }
}

impl std::fmt::Display for SelectionStrategyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RoundRobin => write!(f, "round-robin"),
            Self::Random => write!(f, "random"),
            Self::PerDomain => write!(f, "per-domain"),
        }
    }
}

impl std::str::FromStr for SelectionStrategyType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s).ok_or_else(|| {
            format!(
                "Invalid selection strategy '{}'. Valid options: round-robin, random, per-domain",
                s
            )
        })
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
        let strategy = RandomStrategy;
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
    fn strategy_type_from_str() {
        assert_eq!(
            SelectionStrategyType::from_str("round-robin"),
            Some(SelectionStrategyType::RoundRobin)
        );
        assert_eq!(
            SelectionStrategyType::from_str("roundrobin"),
            Some(SelectionStrategyType::RoundRobin)
        );
        assert_eq!(
            SelectionStrategyType::from_str("random"),
            Some(SelectionStrategyType::Random)
        );
        assert_eq!(
            SelectionStrategyType::from_str("per-domain"),
            Some(SelectionStrategyType::PerDomain)
        );
        assert_eq!(
            SelectionStrategyType::from_str("perdomain"),
            Some(SelectionStrategyType::PerDomain)
        );
        assert_eq!(SelectionStrategyType::from_str("invalid"), None);
    }
}
