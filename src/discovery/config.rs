//! Configuration types for the discovery system.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for a single discovery source operation.
#[derive(Debug, Clone)]
pub struct DiscoverySourceConfig {
    /// Whether this source is enabled.
    pub enabled: bool,

    /// Rate limit delay in milliseconds between requests.
    pub rate_limit_ms: u64,

    /// Maximum number of results to return.
    pub max_results: usize,

    /// Whether this source requires browser-based fetching.
    pub requires_browser: bool,

    /// Custom parameters for specific sources.
    pub custom_params: HashMap<String, serde_json::Value>,
}

impl Default for DiscoverySourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rate_limit_ms: 5000,
            max_results: 100,
            requires_browser: false,
            custom_params: HashMap::new(),
        }
    }
}

/// Configuration for external discovery sources.
///
/// This is added to the existing DiscoveryConfig to enable
/// search engines, sitemaps, and other external discovery methods.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ExternalDiscoveryConfig {
    /// Search engine sources to use.
    #[serde(default)]
    pub search_engines: Vec<SearchEngineSourceConfig>,

    /// Whether to parse sitemaps and robots.txt.
    #[serde(default)]
    pub enable_sitemap: bool,

    /// Whether to query Wayback Machine CDX API.
    #[serde(default)]
    pub enable_wayback: bool,

    /// Additional common paths to enumerate.
    ///
    /// These are appended to the built-in list of common
    /// government document paths.
    #[serde(default)]
    pub common_paths: Vec<String>,

    /// Term extraction configuration.
    #[serde(default)]
    pub term_extraction: TermExtractionConfig,
}

impl ExternalDiscoveryConfig {
    /// Check if any external discovery is enabled.
    pub fn is_enabled(&self) -> bool {
        !self.search_engines.is_empty()
            || self.enable_sitemap
            || self.enable_wayback
            || !self.common_paths.is_empty()
    }

    /// Get enabled search engines.
    pub fn enabled_search_engines(&self) -> impl Iterator<Item = &SearchEngineSourceConfig> {
        self.search_engines.iter().filter(|e| e.enabled)
    }
}

/// Configuration for a search engine source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchEngineSourceConfig {
    /// Engine identifier: "duckduckgo", "google", "bing", "brave".
    pub engine: String,

    /// Whether this engine is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Rate limit delay in milliseconds between requests.
    #[serde(default = "default_search_rate_limit")]
    pub rate_limit_ms: u64,

    /// Maximum number of results to fetch.
    #[serde(default = "default_max_results")]
    pub max_results: usize,
}

impl Default for SearchEngineSourceConfig {
    fn default() -> Self {
        Self {
            engine: "duckduckgo".to_string(),
            enabled: true,
            rate_limit_ms: default_search_rate_limit(),
            max_results: default_max_results(),
        }
    }
}

impl SearchEngineSourceConfig {
    /// Create a new search engine config.
    pub fn new(engine: &str) -> Self {
        let rate_limit_ms = match engine.to_lowercase().as_str() {
            "google" => 15000, // Google needs higher rate limit
            "bing" => 7000,
            _ => 5000,
        };

        Self {
            engine: engine.to_string(),
            enabled: true,
            rate_limit_ms,
            max_results: default_max_results(),
        }
    }

    /// Check if this engine requires browser-based fetching.
    pub fn requires_browser(&self) -> bool {
        matches!(self.engine.to_lowercase().as_str(), "google")
    }

    /// Convert to a DiscoverySourceConfig.
    pub fn to_source_config(&self) -> DiscoverySourceConfig {
        DiscoverySourceConfig {
            enabled: self.enabled,
            rate_limit_ms: self.rate_limit_ms,
            max_results: self.max_results,
            requires_browser: self.requires_browser(),
            custom_params: HashMap::new(),
        }
    }
}

/// Configuration for term extraction.
///
/// Both template detection and LLM expansion can be enabled
/// simultaneously - they're not mutually exclusive.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TermExtractionConfig {
    /// Extract terms from HTML patterns (titles, headings, navigation).
    ///
    /// This is fast and doesn't require an LLM.
    #[serde(default)]
    pub use_template_detection: bool,

    /// CSS selectors for template-based term extraction.
    ///
    /// If empty, uses default selectors for common patterns.
    #[serde(default)]
    pub template_selectors: Vec<String>,

    /// Use LLM to expand seed terms into related search terms.
    ///
    /// Uses the existing expand_search_terms() function.
    #[serde(default)]
    pub use_llm_expansion: bool,

    /// Domain description for LLM context.
    ///
    /// Helps the LLM generate more relevant terms.
    /// Example: "FBI declassified documents and FOIA reading room"
    #[serde(default)]
    pub domain_description: Option<String>,

    /// Maximum number of terms to generate.
    #[serde(default = "default_max_terms")]
    pub max_terms: usize,
}

impl TermExtractionConfig {
    /// Check if any term extraction is enabled.
    pub fn is_enabled(&self) -> bool {
        self.use_template_detection || self.use_llm_expansion
    }

    /// Get template selectors, using defaults if none specified.
    pub fn get_selectors(&self) -> Vec<&str> {
        if self.template_selectors.is_empty() {
            vec![
                "title",
                "h1",
                "h2",
                "h3",
                "nav a",
                ".breadcrumb a",
                ".sidebar a",
                "meta[name='keywords']",
                "meta[name='description']",
            ]
        } else {
            self.template_selectors.iter().map(|s| s.as_str()).collect()
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_search_rate_limit() -> u64 {
    5000
}

fn default_max_results() -> usize {
    100
}

fn default_max_terms() -> usize {
    50
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_discovery_config_default_disabled() {
        let config = ExternalDiscoveryConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn external_discovery_enabled_with_search() {
        let config = ExternalDiscoveryConfig {
            search_engines: vec![SearchEngineSourceConfig::new("duckduckgo")],
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn search_engine_rate_limits() {
        let ddg = SearchEngineSourceConfig::new("duckduckgo");
        assert_eq!(ddg.rate_limit_ms, 5000);

        let google = SearchEngineSourceConfig::new("google");
        assert_eq!(google.rate_limit_ms, 15000);
        assert!(google.requires_browser());
    }

    #[test]
    fn term_extraction_both_modes() {
        let config = TermExtractionConfig {
            use_template_detection: true,
            use_llm_expansion: true,
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn term_extraction_default_selectors() {
        let config = TermExtractionConfig::default();
        let selectors = config.get_selectors();
        assert!(selectors.contains(&"title"));
        assert!(selectors.contains(&"h1"));
        assert!(selectors.contains(&"nav a"));
    }

    #[test]
    fn term_extraction_custom_selectors() {
        let config = TermExtractionConfig {
            template_selectors: vec![".custom-nav a".to_string()],
            ..Default::default()
        };
        let selectors = config.get_selectors();
        assert_eq!(selectors, vec![".custom-nav a"]);
    }

    #[test]
    fn deserialize_config() {
        let json = r#"{
            "search_engines": [
                { "engine": "duckduckgo", "enabled": true }
            ],
            "enable_sitemap": true,
            "term_extraction": {
                "use_template_detection": true,
                "use_llm_expansion": true
            }
        }"#;

        let config: ExternalDiscoveryConfig = serde_json::from_str(json).unwrap();
        assert!(config.enable_sitemap);
        assert_eq!(config.search_engines.len(), 1);
        assert!(config.term_extraction.use_template_detection);
        assert!(config.term_extraction.use_llm_expansion);
    }
}
