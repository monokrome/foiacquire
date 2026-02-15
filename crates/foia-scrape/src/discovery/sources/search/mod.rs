//! Search engine discovery sources.
//!
//! Implements discovery via external search engines using site: queries.

mod duckduckgo;
mod query;

pub use duckduckgo::DuckDuckGoSource;
pub use query::QueryBuilder;

use std::collections::HashMap;
use std::sync::Arc;

use crate::discovery::{DiscoveryError, DiscoverySource};

/// Registry of search engine sources.
pub struct SearchEngineRegistry {
    engines: HashMap<String, Arc<dyn DiscoverySource>>,
}

impl SearchEngineRegistry {
    /// Create a new registry with all built-in search engines.
    pub fn new() -> Self {
        let mut engines: HashMap<String, Arc<dyn DiscoverySource>> = HashMap::new();

        engines.insert("duckduckgo".to_string(), Arc::new(DuckDuckGoSource::new()));
        // TODO: Add Google, Bing, Brave when implemented

        Self { engines }
    }

    /// Get a search engine by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn DiscoverySource>> {
        self.engines.get(&name.to_lowercase()).cloned()
    }

    /// List all available engine names.
    pub fn list(&self) -> Vec<&str> {
        self.engines.keys().map(|s| s.as_str()).collect()
    }

    /// Check if an engine requires browser-based fetching.
    pub fn requires_browser(engine: &str) -> bool {
        matches!(engine.to_lowercase().as_str(), "google")
    }
}

impl Default for SearchEngineRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a search engine source by name.
pub fn create_search_engine(name: &str) -> Result<Arc<dyn DiscoverySource>, DiscoveryError> {
    match name.to_lowercase().as_str() {
        "duckduckgo" | "ddg" => Ok(Arc::new(DuckDuckGoSource::new())),
        "google" => Err(DiscoveryError::Config(
            "Google search requires browser support (not yet implemented)".to_string(),
        )),
        "bing" => Err(DiscoveryError::Config(
            "Bing search not yet implemented".to_string(),
        )),
        "brave" => Err(DiscoveryError::Config(
            "Brave search not yet implemented".to_string(),
        )),
        _ => Err(DiscoveryError::Config(format!(
            "Unknown search engine: {}. Available: duckduckgo",
            name
        ))),
    }
}
