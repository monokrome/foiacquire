//! Scraper configuration types.
//!
//! These structs define the JSON-configurable behavior for scrapers,
//! including discovery strategies, browser settings, fetch options,
//! and per-source privacy overrides.
//!
//! Moved from `scrapers/config.rs` to break circular dependencies
//! for workspace split (config is in core, scrapers is a domain crate).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::browser::BrowserEngineConfig;
use super::discovery::ExternalDiscoveryConfig;
use crate::privacy::SourcePrivacyConfig;

/// Via proxy mode - controls how URL rewriting through caching proxies works.
///
/// Via mappings rewrite URLs to fetch through CDN/caching proxies (e.g., Cloudflare).
/// This setting controls when those proxies are used.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ViaMode {
    /// Never send requests over via proxy. Via mappings are only used for
    /// URL normalization/detection (e.g., recognizing Google Drive URLs).
    #[default]
    Strict,
    /// Use via proxy as fallback when rate limited (429/503).
    /// Primary requests go to the original URL.
    Fallback,
    /// Use via proxy as primary, fall back to original URL on failure.
    Priority,
}

impl prefer::FromValue for ViaMode {
    fn from_value(value: &prefer::ConfigValue) -> prefer::Result<Self> {
        match value.as_str() {
            Some("strict") => Ok(ViaMode::Strict),
            Some("fallback") => Ok(ViaMode::Fallback),
            Some("priority") => Ok(ViaMode::Priority),
            Some(other) => Err(prefer::Error::ConversionError {
                key: String::new(),
                type_name: "ViaMode".to_string(),
                source: format!("unknown via mode: {}", other).into(),
            }),
            None => Err(prefer::Error::ConversionError {
                key: String::new(),
                type_name: "ViaMode".to_string(),
                source: "expected string".into(),
            }),
        }
    }
}

#[allow(dead_code)]
impl ViaMode {
    /// Check if this mode allows using via for requests (not just detection).
    pub fn allows_via_requests(&self) -> bool {
        !matches!(self, ViaMode::Strict)
    }

    /// Check if via should be tried first (priority mode).
    pub fn via_first(&self) -> bool {
        matches!(self, ViaMode::Priority)
    }
}

/// Scraper configuration from JSON.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct ScraperConfig {
    /// Name of the scraper (optional, can use source ID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Base URL for the scraper (optional, can be derived from discovery).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// User agent configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Refresh TTL in days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_ttl_days: Option<u64>,
    #[serde(default, skip_serializing_if = "DiscoveryConfig::is_default")]
    #[prefer(default)]
    pub discovery: DiscoveryConfig,
    #[serde(default, skip_serializing_if = "FetchConfig::is_default")]
    #[prefer(default)]
    pub fetch: FetchConfig,
    /// Browser configuration for anti-bot protected sites.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(skip)]
    pub browser: Option<BrowserEngineConfig>,
    /// Per-source privacy configuration.
    #[serde(default, skip_serializing_if = "SourcePrivacyConfig::is_default")]
    #[prefer(default)]
    pub privacy: SourcePrivacyConfig,
    /// Per-source request timeout in seconds (overrides global setting).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout: Option<u64>,
    /// Per-source request delay in milliseconds (overrides global setting).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_delay_ms: Option<u64>,
    /// Per-source URL rewriting for caching proxies (overrides global setting).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[prefer(default)]
    pub via: HashMap<String, String>,
    /// Per-source via proxy mode (overrides global setting).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via_mode: Option<ViaMode>,
}

impl ScraperConfig {
    /// Get the effective name, using the provided default if not set.
    pub fn name_or(&self, default: &str) -> String {
        self.name.clone().unwrap_or_else(|| default.to_string())
    }

    /// Get the effective base URL, falling back to discovery base_url.
    pub fn base_url_or(&self, default: &str) -> String {
        self.base_url
            .clone()
            .or_else(|| self.discovery.base_url.clone())
            .unwrap_or_else(|| default.to_string())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct DiscoveryConfig {
    #[serde(rename = "type", default = "default_discovery_type")]
    #[prefer(default, rename = "type")]
    pub discovery_type: String,
    #[serde(default)]
    #[prefer(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub start_paths: Vec<String>,
    #[serde(default)]
    #[prefer(default)]
    pub levels: Vec<LevelConfig>,
    #[serde(default)]
    #[prefer(default)]
    pub api: Option<ApiConfig>,
    /// Maximum recursion depth for BFS crawling (default: 10)
    #[serde(default)]
    #[prefer(default)]
    pub max_depth: Option<u32>,
    /// Direct document link selectors (simpler alternative to levels)
    #[serde(default)]
    #[prefer(default)]
    pub document_links: Vec<String>,
    /// Direct document URL patterns (simpler alternative to levels)
    #[serde(default)]
    #[prefer(default)]
    pub document_patterns: Vec<String>,
    /// Whether to use browser for fetching pages
    #[serde(default)]
    #[prefer(default)]
    pub use_browser: bool,
    /// Search queries to expand discovery (generates search URLs)
    #[serde(default)]
    #[prefer(default)]
    pub search_queries: Vec<String>,
    /// URL template for search queries, with {query} placeholder
    /// e.g., "/search?q={query}" or "/readingroom/search/site/?search_api_fulltext={query}"
    #[serde(default)]
    #[prefer(default)]
    pub search_url_template: Option<String>,
    /// Whether to expand search queries using LLM (generates related terms)
    #[serde(default)]
    #[prefer(default)]
    pub expand_search_terms: bool,

    /// External discovery configuration (search engines, sitemaps, Wayback, etc.)
    #[serde(default, skip_serializing_if = "ExternalDiscoveryConfig::is_default")]
    #[prefer(skip)]
    pub external: ExternalDiscoveryConfig,
}

impl ExternalDiscoveryConfig {
    /// Check if this config is all defaults (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl DiscoveryConfig {
    /// Check if the config equals the default (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

fn default_discovery_type() -> String {
    "html_crawl".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct LevelConfig {
    #[serde(default)]
    #[prefer(default)]
    pub link_selectors: Vec<String>,
    #[serde(default)]
    #[prefer(default)]
    pub link_pattern: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub document_patterns: Vec<String>,
    #[serde(default)]
    #[prefer(default)]
    pub pagination: Option<PaginationConfig>,
    #[serde(default)]
    #[prefer(default)]
    pub use_browser: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct PaginationConfig {
    #[serde(default)]
    #[prefer(default)]
    pub next_selectors: Vec<String>,
    #[serde(default)]
    #[prefer(default)]
    pub page_param: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct ApiConfig {
    #[serde(default)]
    #[prefer(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub endpoint: String,
    #[serde(default)]
    #[prefer(skip)]
    pub params: serde_json::Value,
    #[serde(default)]
    #[prefer(default)]
    pub pagination: ApiPaginationConfig,
    #[serde(default)]
    #[prefer(default)]
    pub url_extraction: UrlExtractionConfig,
    #[serde(default)]
    #[prefer(default)]
    pub queries: Vec<String>,
    #[serde(default)]
    #[prefer(default)]
    pub query_param: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub parent: Option<ApiParentConfig>,
    #[serde(default)]
    #[prefer(default)]
    pub child: Option<ApiChildConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct ApiPaginationConfig {
    #[serde(default = "default_page_param")]
    #[prefer(default)]
    pub page_param: String,
    #[serde(default)]
    #[prefer(default)]
    pub page_size_param: Option<String>,
    #[serde(default = "default_page_size")]
    #[prefer(default)]
    pub page_size: u32,
    #[serde(default = "default_results_path")]
    #[prefer(default)]
    pub results_path: String,
    #[serde(default)]
    #[prefer(default)]
    pub cursor_param: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub cursor_response_path: Option<String>,
}

fn default_page_param() -> String {
    "page".to_string()
}
fn default_page_size() -> u32 {
    100
}
fn default_results_path() -> String {
    "results".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct UrlExtractionConfig {
    #[serde(default = "default_url_field")]
    #[prefer(default)]
    pub url_field: String,
    #[serde(default)]
    #[prefer(default)]
    pub url_template: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub fallback_field: Option<String>,
    #[serde(default)]
    #[prefer(default)]
    pub items_path: Option<String>,
    /// Nested array paths to traverse (e.g., ["communications", "files"] for communications[*].files[*])
    #[serde(default)]
    #[prefer(default)]
    pub nested_arrays: Vec<String>,
}

fn default_url_field() -> String {
    "url".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct ApiParentConfig {
    #[serde(default)]
    #[prefer(default)]
    pub endpoint: String,
    #[serde(default)]
    #[prefer(skip)]
    pub params: serde_json::Value,
    #[serde(default)]
    #[prefer(default)]
    pub pagination: ApiPaginationConfig,
    #[serde(default = "default_results_path")]
    #[prefer(default)]
    pub results_path: String,
    #[serde(default = "default_id_path")]
    #[prefer(default)]
    pub id_path: String,
}

fn default_id_path() -> String {
    "id".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct ApiChildConfig {
    #[serde(default)]
    #[prefer(default)]
    pub endpoint_template: String,
    #[serde(default = "default_results_path")]
    #[prefer(default)]
    pub results_path: String,
    #[serde(default)]
    #[prefer(default)]
    pub url_extraction: UrlExtractionConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct FetchConfig {
    #[serde(default)]
    #[prefer(default)]
    pub use_browser: bool,
    /// Use binary fetch for PDFs (JavaScript fetch from within browser context).
    /// Required for sites with Akamai/Cloudflare protection on PDF endpoints.
    #[serde(default)]
    #[prefer(default)]
    pub binary_fetch: bool,
    #[serde(default)]
    #[prefer(default)]
    pub pdf_selectors: Vec<String>,
    #[serde(default)]
    #[prefer(default)]
    pub title_selectors: Vec<String>,
}

impl FetchConfig {
    /// Check if the config equals the default (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scraper_config_name_or() {
        let config = ScraperConfig {
            name: Some("custom".to_string()),
            ..Default::default()
        };
        assert_eq!(config.name_or("default"), "custom");

        let config_empty = ScraperConfig::default();
        assert_eq!(config_empty.name_or("default"), "default");
    }

    #[test]
    fn test_scraper_config_base_url_or() {
        // Test with explicit base_url
        let config = ScraperConfig {
            base_url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        assert_eq!(
            config.base_url_or("https://fallback.com"),
            "https://example.com"
        );

        // Test fallback to discovery base_url
        let mut config2 = ScraperConfig::default();
        config2.discovery.base_url = Some("https://discovery.com".to_string());
        assert_eq!(
            config2.base_url_or("https://fallback.com"),
            "https://discovery.com"
        );

        // Test final fallback
        let config3 = ScraperConfig::default();
        assert_eq!(
            config3.base_url_or("https://fallback.com"),
            "https://fallback.com"
        );
    }

    #[test]
    fn test_scraper_config_json_deserialization() {
        let json = r#"{
            "name": "test_scraper",
            "base_url": "https://example.com",
            "discovery": {
                "type": "api",
                "start_paths": ["/docs", "/files"]
            },
            "browser": {
                "enabled": true,
                "engine": "stealth"
            }
        }"#;

        let config: ScraperConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, Some("test_scraper".to_string()));
        assert_eq!(config.base_url, Some("https://example.com".to_string()));
        assert_eq!(config.discovery.discovery_type, "api");
        assert_eq!(config.discovery.start_paths, vec!["/docs", "/files"]);
        assert!(config.browser.is_some());
        assert!(config.browser.as_ref().unwrap().enabled);
    }

    #[test]
    fn test_discovery_config_defaults() {
        let config: DiscoveryConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.discovery_type, "html_crawl");
        assert!(config.start_paths.is_empty());
        assert!(config.levels.is_empty());
        assert!(!config.use_browser);
    }

    #[test]
    fn test_api_pagination_defaults() {
        let config: ApiPaginationConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.page_param, "page");
        assert_eq!(config.page_size, 100);
        assert_eq!(config.results_path, "results");
    }
}
