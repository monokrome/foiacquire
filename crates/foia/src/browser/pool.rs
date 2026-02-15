//! Browser pool with multiple connections and selection strategies.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::selection::{BrowserSelectionStrategy, SelectionStrategyType};
use super::types::{BinaryFetchResponse, BrowserFetchResponse};
use super::{BrowserEngineConfig, BrowserFetcher};

/// Health tracking for a single browser.
#[derive(Debug)]
struct BrowserHealth {
    /// Whether the browser is currently considered healthy.
    healthy: bool,
    /// Number of consecutive failures.
    consecutive_failures: u32,
    /// Time of last health check or status change.
    last_update: Instant,
}

impl Default for BrowserHealth {
    fn default() -> Self {
        Self {
            healthy: true,
            consecutive_failures: 0,
            last_update: Instant::now(),
        }
    }
}

/// Configuration for a browser pool.
#[derive(Debug, Clone)]
pub struct BrowserPoolConfig {
    /// List of browser WebSocket URLs.
    pub urls: Vec<String>,
    /// Selection strategy for choosing browsers.
    pub strategy: SelectionStrategyType,
    /// Base browser config (applied to all connections).
    pub base_config: BrowserEngineConfig,
    /// Number of consecutive failures before marking unhealthy.
    pub unhealthy_threshold: u32,
    /// Duration before attempting to restore an unhealthy browser.
    pub health_check_interval: Duration,
}

impl Default for BrowserPoolConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            strategy: SelectionStrategyType::default(),
            base_config: BrowserEngineConfig::default().with_env_overrides(),
            unhealthy_threshold: 3,
            health_check_interval: Duration::from_secs(60),
        }
    }
}

impl BrowserPoolConfig {
    /// Create config from a single URL (backward compatibility).
    pub fn single(url: String) -> Self {
        Self {
            urls: vec![url],
            ..Default::default()
        }
    }

    /// Parse from environment variables.
    ///
    /// - `BROWSER_URL` - Comma-separated list of browser URLs
    /// - `BROWSER_SELECTION` - Selection strategy (round-robin, random, per-domain)
    /// - `SOCKS_PROXY` - Proxy server for browser traffic
    pub fn from_env() -> Option<Self> {
        let url_str = std::env::var("BROWSER_URL").ok()?;
        if url_str.is_empty() {
            return None;
        }

        let urls: Vec<String> = url_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if urls.is_empty() {
            return None;
        }

        let strategy = std::env::var("BROWSER_SELECTION")
            .ok()
            .and_then(|s| SelectionStrategyType::from_str(&s))
            .unwrap_or_default();

        Some(Self {
            urls,
            strategy,
            base_config: BrowserEngineConfig::default().with_env_overrides(),
            ..Default::default()
        })
    }

    /// Check if this config has multiple browsers.
    pub fn is_pool(&self) -> bool {
        self.urls.len() > 1
    }
}

/// Pool of browser connections with selection strategy and health tracking.
pub struct BrowserPool {
    fetchers: Vec<Arc<Mutex<BrowserFetcher>>>,
    strategy: Box<dyn BrowserSelectionStrategy>,
    health: Arc<Mutex<Vec<BrowserHealth>>>,
    urls: Vec<String>,
    config: BrowserPoolConfig,
}

impl BrowserPool {
    /// Create a new browser pool with lazy connection.
    pub fn new(config: BrowserPoolConfig) -> Self {
        let strategy = config.strategy.create_strategy();
        let urls = config.urls.clone();

        // Create fetchers lazily - they connect on first use
        let fetchers: Vec<Arc<Mutex<BrowserFetcher>>> = config
            .urls
            .iter()
            .map(|url| {
                let mut cfg = config.base_config.clone();
                cfg.remote_url = Some(url.clone());
                Arc::new(Mutex::new(BrowserFetcher::new(cfg)))
            })
            .collect();

        let health = Arc::new(Mutex::new(
            (0..fetchers.len())
                .map(|_| BrowserHealth::default())
                .collect(),
        ));

        info!(
            "Created browser pool with {} browser(s) (strategy: {})",
            urls.len(),
            config.strategy
        );

        Self {
            fetchers,
            strategy,
            health,
            urls,
            config,
        }
    }

    /// Get the number of browsers in the pool.
    pub fn size(&self) -> usize {
        self.fetchers.len()
    }

    /// Get current health status snapshot.
    async fn get_health_snapshot(&self) -> Vec<bool> {
        let mut health = self.health.lock().await;
        let now = Instant::now();

        // Check if any unhealthy browsers should be retried
        for h in health.iter_mut() {
            if !h.healthy && now.duration_since(h.last_update) >= self.config.health_check_interval
            {
                debug!("Restoring browser to healthy status for retry");
                h.healthy = true;
                h.consecutive_failures = 0;
                h.last_update = now;
            }
        }

        health.iter().map(|h| h.healthy).collect()
    }

    /// Mark a browser as failed.
    async fn mark_failed(&self, idx: usize) {
        let mut health = self.health.lock().await;
        if let Some(h) = health.get_mut(idx) {
            h.consecutive_failures += 1;
            h.last_update = Instant::now();

            if h.consecutive_failures >= self.config.unhealthy_threshold {
                h.healthy = false;
                warn!(
                    "Browser {} ({}) marked unhealthy after {} consecutive failures",
                    idx, self.urls[idx], h.consecutive_failures
                );
            }
        }
    }

    /// Mark a browser as successful.
    async fn mark_success(&self, idx: usize) {
        let mut health = self.health.lock().await;
        if let Some(h) = health.get_mut(idx) {
            if h.consecutive_failures > 0 || !h.healthy {
                debug!("Browser {} recovered", idx);
            }
            h.healthy = true;
            h.consecutive_failures = 0;
            h.last_update = Instant::now();
        }
    }

    /// Fetch a URL, selecting browser based on strategy.
    /// On connection failure, tries next browser (up to pool size attempts).
    #[cfg(feature = "browser")]
    pub async fn fetch(&self, url: &str) -> Result<BrowserFetchResponse> {
        let count = self.fetchers.len();
        if count == 0 {
            return Err(anyhow::anyhow!("No browsers configured in pool"));
        }

        let healthy = self.get_health_snapshot().await;

        // Try strategy-selected browser first
        if let Some(start_idx) = self.strategy.select(url, count, &healthy) {
            debug!(
                "Strategy selected browser {} ({}) for {}",
                start_idx, self.urls[start_idx], url
            );

            // Try starting from selected browser, then fall through to others
            for attempt in 0..count {
                let idx = (start_idx + attempt) % count;
                let fetcher = &self.fetchers[idx];

                debug!("Attempting fetch from browser {} ({})", idx, self.urls[idx]);

                let mut guard = fetcher.lock().await;
                match guard.fetch(url).await {
                    Ok(response) => {
                        drop(guard);
                        self.mark_success(idx).await;
                        return Ok(response);
                    }
                    Err(e) => {
                        drop(guard);
                        warn!("Browser {} ({}) failed: {}", idx, self.urls[idx], e);
                        self.mark_failed(idx).await;
                    }
                }
            }
        } else {
            // All browsers unhealthy, try them anyway
            warn!("All browsers marked unhealthy, attempting recovery");
            for idx in 0..count {
                let fetcher = &self.fetchers[idx];
                let mut guard = fetcher.lock().await;
                match guard.fetch(url).await {
                    Ok(response) => {
                        drop(guard);
                        self.mark_success(idx).await;
                        return Ok(response);
                    }
                    Err(e) => {
                        drop(guard);
                        warn!("Browser {} ({}) failed: {}", idx, self.urls[idx], e);
                        self.mark_failed(idx).await;
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "All {} browser(s) failed to fetch {}",
            count,
            url
        ))
    }

    /// Fetch binary content (PDF, images).
    #[cfg(feature = "browser")]
    pub async fn fetch_binary(
        &self,
        url: &str,
        context_url: Option<&str>,
    ) -> Result<BinaryFetchResponse> {
        let count = self.fetchers.len();
        if count == 0 {
            return Err(anyhow::anyhow!("No browsers configured in pool"));
        }

        let healthy = self.get_health_snapshot().await;

        if let Some(start_idx) = self.strategy.select(url, count, &healthy) {
            for attempt in 0..count {
                let idx = (start_idx + attempt) % count;
                let fetcher = &self.fetchers[idx];

                let mut guard = fetcher.lock().await;
                match guard.fetch_binary(url, context_url).await {
                    Ok(response) => {
                        drop(guard);
                        self.mark_success(idx).await;
                        return Ok(response);
                    }
                    Err(e) => {
                        drop(guard);
                        warn!("Browser {} binary fetch failed: {}", idx, e);
                        self.mark_failed(idx).await;
                    }
                }
            }
        } else {
            // All browsers unhealthy, try them anyway
            for idx in 0..count {
                let fetcher = &self.fetchers[idx];
                let mut guard = fetcher.lock().await;
                match guard.fetch_binary(url, context_url).await {
                    Ok(response) => {
                        drop(guard);
                        self.mark_success(idx).await;
                        return Ok(response);
                    }
                    Err(e) => {
                        drop(guard);
                        self.mark_failed(idx).await;
                        warn!("Browser {} binary fetch failed: {}", idx, e);
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "All {} browser(s) failed binary fetch for {}",
            count,
            url
        ))
    }

    /// Stub fetch for when browser feature is disabled.
    #[cfg(not(feature = "browser"))]
    pub async fn fetch(&self, _url: &str) -> Result<BrowserFetchResponse> {
        Err(anyhow::anyhow!(
            "Browser support not compiled. Rebuild with: cargo build --features browser"
        ))
    }

    /// Stub binary fetch for when browser feature is disabled.
    #[cfg(not(feature = "browser"))]
    pub async fn fetch_binary(
        &self,
        _url: &str,
        _context_url: Option<&str>,
    ) -> Result<BinaryFetchResponse> {
        Err(anyhow::anyhow!(
            "Browser support not compiled. Rebuild with: cargo build --features browser"
        ))
    }

    /// Close all browser connections.
    pub async fn close(&self) {
        for (idx, fetcher) in self.fetchers.iter().enumerate() {
            debug!("Closing browser {}", idx);
            fetcher.lock().await.close().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Tests that modify environment variables must be serialized
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn pool_config_from_single_url() {
        let _guard = ENV_MUTEX.lock().unwrap();

        std::env::set_var("BROWSER_URL", "ws://localhost:9222");
        std::env::remove_var("BROWSER_SELECTION");

        let config = BrowserPoolConfig::from_env().unwrap();
        assert_eq!(config.urls, vec!["ws://localhost:9222"]);
        assert_eq!(config.strategy, SelectionStrategyType::RoundRobin);
        assert!(!config.is_pool());

        std::env::remove_var("BROWSER_URL");
    }

    #[test]
    fn pool_config_from_multiple_urls() {
        let _guard = ENV_MUTEX.lock().unwrap();

        std::env::set_var("BROWSER_URL", "ws://b1:9222, ws://b2:9222, ws://b3:9222");
        std::env::set_var("BROWSER_SELECTION", "per-domain");

        let config = BrowserPoolConfig::from_env().unwrap();
        assert_eq!(
            config.urls,
            vec!["ws://b1:9222", "ws://b2:9222", "ws://b3:9222"]
        );
        assert_eq!(config.strategy, SelectionStrategyType::PerDomain);
        assert!(config.is_pool());

        std::env::remove_var("BROWSER_URL");
        std::env::remove_var("BROWSER_SELECTION");
    }

    #[test]
    fn pool_config_empty_url_returns_none() {
        let _guard = ENV_MUTEX.lock().unwrap();

        std::env::set_var("BROWSER_URL", "");
        assert!(BrowserPoolConfig::from_env().is_none());

        std::env::remove_var("BROWSER_URL");
        assert!(BrowserPoolConfig::from_env().is_none());
    }

    #[test]
    fn pool_config_single_helper() {
        let config = BrowserPoolConfig::single("ws://test:9222".to_string());
        assert_eq!(config.urls, vec!["ws://test:9222"]);
        assert_eq!(config.strategy, SelectionStrategyType::RoundRobin);
        assert!(!config.is_pool());
    }

    #[test]
    fn pool_config_is_pool_requires_multiple() {
        let single = BrowserPoolConfig {
            urls: vec!["ws://a:9222".to_string()],
            ..Default::default()
        };
        assert!(!single.is_pool());

        let multi = BrowserPoolConfig {
            urls: vec!["ws://a:9222".to_string(), "ws://b:9222".to_string()],
            ..Default::default()
        };
        assert!(multi.is_pool());
    }

    #[test]
    fn pool_config_default_values() {
        let config = BrowserPoolConfig::default();
        assert!(config.urls.is_empty());
        assert_eq!(config.strategy, SelectionStrategyType::RoundRobin);
        assert_eq!(config.unhealthy_threshold, 3);
        assert_eq!(config.health_check_interval, Duration::from_secs(60));
    }
}
