//! Discovery source implementations.
//!
//! Each source provides a way to discover URLs for a target domain.

pub mod common_paths;
pub mod search;
pub mod sitemap;
pub mod wayback;

pub use common_paths::CommonPathsSource;
pub use sitemap::SitemapSource;
pub use wayback::WaybackSource;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::discovery::{DiscoveryError, DiscoverySource, DiscoverySourceConfig};
use crate::HttpClient;

/// Registry of all available discovery sources.
pub struct SourceRegistry {
    sources: HashMap<String, Arc<dyn DiscoverySource>>,
}

impl SourceRegistry {
    /// Create a new registry with all built-in sources.
    pub fn new() -> Self {
        let mut sources: HashMap<String, Arc<dyn DiscoverySource>> = HashMap::new();

        // Add built-in sources
        sources.insert("sitemap".to_string(), Arc::new(SitemapSource::new()));
        sources.insert("wayback".to_string(), Arc::new(WaybackSource::new()));
        sources.insert(
            "common_paths".to_string(),
            Arc::new(CommonPathsSource::new()),
        );

        Self { sources }
    }

    /// Get a source by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn DiscoverySource>> {
        self.sources.get(name).cloned()
    }

    /// List all available source names.
    pub fn list(&self) -> Vec<&str> {
        self.sources.keys().map(|s| s.as_str()).collect()
    }

    /// Register a custom source.
    pub fn register(&mut self, name: String, source: Arc<dyn DiscoverySource>) {
        self.sources.insert(name, source);
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (compatible; foia/1.0)";

/// Build an HttpClient configured for discovery with the given overrides.
pub fn create_discovery_client(
    service_name: &str,
    config: &DiscoverySourceConfig,
    timeout: Option<Duration>,
    user_agent: Option<&str>,
) -> Result<HttpClient, DiscoveryError> {
    HttpClient::builder(
        service_name,
        timeout.unwrap_or(DEFAULT_TIMEOUT),
        Duration::from_millis(config.rate_limit_ms),
    )
    .user_agent(user_agent.unwrap_or(DEFAULT_USER_AGENT))
    .privacy(&config.privacy)
    .build()
    .map_err(|e| DiscoveryError::Config(format!("Failed to create HTTP client: {}", e)))
}

/// Helper to create sources from config.
pub fn create_source(name: &str) -> Result<Arc<dyn DiscoverySource>, DiscoveryError> {
    match name.to_lowercase().as_str() {
        "sitemap" => Ok(Arc::new(SitemapSource::new())),
        "wayback" => Ok(Arc::new(WaybackSource::new())),
        "common_paths" | "paths" => Ok(Arc::new(CommonPathsSource::new())),
        _ => Err(DiscoveryError::Config(format!("Unknown source: {}", name))),
    }
}
