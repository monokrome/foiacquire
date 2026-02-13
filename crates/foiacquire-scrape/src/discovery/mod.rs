//! Discovery system for finding document URLs via multiple sources.
//!
//! This module provides a pluggable architecture for discovering documents
//! through search engines, sitemaps, Wayback Machine, and intelligent term extraction.

#![allow(dead_code)] // Discovery module has extensibility APIs for future use

pub mod config;
mod result;

pub mod sources;
pub mod term_extraction;

// Re-export config types for public API
#[allow(unused_imports)]
pub use config::{
    DiscoverySourceConfig, ExternalDiscoveryConfig, SearchEngineSourceConfig, TermExtractionConfig,
};
pub use result::{is_listing_url, DiscoveredUrl};

use async_trait::async_trait;

use foiacquire::models::DiscoveryMethod;

/// Error type for discovery operations.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Failed to parse response: {0}")]
    Parse(String),

    #[error("Rate limited by source: {0}")]
    RateLimited(String),

    #[error("Source unavailable: {0}")]
    Unavailable(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Browser required but not available")]
    BrowserRequired,

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

/// Trait for pluggable discovery sources.
///
/// Each discovery source (search engines, sitemap, wayback, etc.) implements
/// this trait to provide a consistent interface for URL discovery.
#[async_trait]
pub trait DiscoverySource: Send + Sync {
    /// Unique identifier for this source (e.g., "duckduckgo", "sitemap").
    fn name(&self) -> &str;

    /// Discovery method enum variant for tracking in the database.
    fn method(&self) -> DiscoveryMethod;

    /// Whether this source requires browser-based fetching.
    ///
    /// Sources like Google Search need a browser to bypass bot detection.
    fn requires_browser(&self) -> bool {
        false
    }

    /// Discover URLs for a target domain using the given search terms.
    ///
    /// # Arguments
    /// * `target_domain` - The domain to discover URLs for (e.g., "oig.justice.gov")
    /// * `search_terms` - Terms to search for (empty for non-search sources)
    /// * `config` - Configuration for this discovery operation
    ///
    /// # Returns
    /// A list of discovered URLs with metadata.
    async fn discover(
        &self,
        target_domain: &str,
        search_terms: &[String],
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<DiscoveredUrl>, DiscoveryError>;

    /// Check if this source is currently available.
    ///
    /// Used to skip sources that are rate-limited or unavailable.
    async fn is_available(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_error_display() {
        let err = DiscoveryError::RateLimited("google".to_string());
        assert!(err.to_string().contains("Rate limited"));

        let err = DiscoveryError::BrowserRequired;
        assert!(err.to_string().contains("Browser required"));
    }
}
