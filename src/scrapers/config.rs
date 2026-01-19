//! Scraper configuration types.
//!
//! These structs define the JSON-configurable behavior for scrapers,
//! including discovery strategies, browser settings, fetch options,
//! and per-source privacy overrides.

use serde::{Deserialize, Serialize};

use super::browser::{
    default_headless, default_timeout, BrowserEngineConfig, BrowserEngineType,
    SelectionStrategyType,
};
use crate::discovery::config::ExternalDiscoveryConfig;
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScraperConfig {
    /// Name of the scraper (optional, can use source ID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Base URL for the scraper (optional, can be derived from discovery).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// User agent configuration.
    /// - None: Use default FOIAcquire user agent
    /// - "impersonate": Randomly select from real browser user agents
    /// - Any other string: Use as custom user agent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Refresh TTL in days. URLs older than this will be re-checked.
    /// Overrides the global default_refresh_ttl_days if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_ttl_days: Option<u64>,
    #[serde(default, skip_serializing_if = "DiscoveryConfig::is_default")]
    pub discovery: DiscoveryConfig,
    #[serde(default, skip_serializing_if = "FetchConfig::is_default")]
    pub fetch: FetchConfig,
    /// Browser configuration for anti-bot protected sites.
    /// When set, the scraper will use a headless browser instead of HTTP requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser: Option<BrowserConfig>,
    /// Per-source privacy configuration.
    /// Allows overriding global privacy settings for this specific source.
    /// Example: `"privacy": { "direct": true }` to skip Tor for trusted internal sources.
    #[serde(default, skip_serializing_if = "SourcePrivacyConfig::is_default")]
    pub privacy: SourcePrivacyConfig,
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

/// Browser configuration for scraper.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BrowserConfig {
    /// Whether to use browser for fetching (enables browser mode).
    #[serde(default)]
    pub enabled: bool,

    /// Browser engine type.
    /// - "stealth": Chromium with stealth patches (bypasses most bot detection)
    /// - "cookies": Use saved cookies with regular HTTP (fastest)
    /// - "standard": Regular Chromium without stealth patches
    #[serde(default)]
    pub engine: String,

    /// Run in headless mode (default: true).
    /// Set to false for debugging or if headless detection is an issue.
    #[serde(default = "default_headless")]
    pub headless: bool,

    /// Proxy server URL (e.g., "socks5://127.0.0.1:1080").
    #[serde(default)]
    pub proxy: Option<String>,

    /// Path to cookies file for cookie injection mode.
    #[serde(default)]
    pub cookies_file: Option<String>,

    /// Page load timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Wait for this CSS selector before considering page loaded.
    #[serde(default)]
    pub wait_for_selector: Option<String>,

    /// Remote Chrome DevTools URL (e.g., "ws://localhost:9222").
    /// If set, connects to existing browser instead of launching one.
    /// For a single browser, use this field.
    #[serde(default)]
    pub remote_url: Option<String>,

    /// Multiple remote browser URLs for load balancing/failover.
    /// Overrides BROWSER_URL environment variable when set.
    #[serde(default)]
    pub urls: Vec<String>,

    /// Browser selection strategy when multiple URLs are configured.
    /// Options: "round-robin" (default), "random", "per-domain".
    #[serde(default)]
    pub selection: Option<String>,
}

impl BrowserConfig {
    /// Convert to BrowserEngineConfig.
    /// Per-scraper config overrides environment variables.
    pub fn to_engine_config(&self) -> BrowserEngineConfig {
        let engine = match self.engine.to_lowercase().as_str() {
            "stealth" => BrowserEngineType::Stealth,
            "cookies" => BrowserEngineType::Cookies,
            "standard" => BrowserEngineType::Standard,
            _ => BrowserEngineType::Stealth,
        };

        let selection = self
            .selection
            .as_ref()
            .and_then(|s| SelectionStrategyType::from_str(s))
            .unwrap_or_default();

        // Per-scraper URLs override environment
        let (remote_url, remote_urls) = if !self.urls.is_empty() {
            (None, self.urls.clone())
        } else if let Some(ref url) = self.remote_url {
            (Some(url.clone()), Vec::new())
        } else {
            // Fall back to environment variables
            let base = BrowserEngineConfig::default().with_env_overrides();
            (base.remote_url, base.remote_urls)
        };

        BrowserEngineConfig {
            engine,
            headless: self.headless,
            proxy: self.proxy.clone(),
            cookies_file: self.cookies_file.as_ref().map(std::path::PathBuf::from),
            timeout: self.timeout,
            wait_for_selector: self.wait_for_selector.clone(),
            chrome_args: Vec::new(),
            remote_url,
            remote_urls,
            selection,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    #[serde(rename = "type", default = "default_discovery_type")]
    pub discovery_type: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub start_paths: Vec<String>,
    #[serde(default)]
    pub levels: Vec<LevelConfig>,
    #[serde(default)]
    pub api: Option<ApiConfig>,
    /// Maximum recursion depth for BFS crawling (default: 10)
    #[serde(default)]
    pub max_depth: Option<u32>,
    /// Direct document link selectors (simpler alternative to levels)
    #[serde(default)]
    pub document_links: Vec<String>,
    /// Direct document URL patterns (simpler alternative to levels)
    #[serde(default)]
    pub document_patterns: Vec<String>,
    /// Whether to use browser for fetching pages
    #[serde(default)]
    pub use_browser: bool,
    /// Search queries to expand discovery (generates search URLs)
    #[serde(default)]
    pub search_queries: Vec<String>,
    /// URL template for search queries, with {query} placeholder
    /// e.g., "/search?q={query}" or "/readingroom/search/site/?search_api_fulltext={query}"
    #[serde(default)]
    pub search_url_template: Option<String>,
    /// Whether to expand search queries using LLM (generates related terms)
    #[serde(default)]
    pub expand_search_terms: bool,

    /// External discovery configuration (search engines, sitemaps, Wayback, etc.)
    #[serde(default, skip_serializing_if = "ExternalDiscoveryConfig::is_default")]
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LevelConfig {
    #[serde(default)]
    pub link_selectors: Vec<String>,
    #[serde(default)]
    pub link_pattern: Option<String>,
    #[serde(default)]
    pub document_patterns: Vec<String>,
    #[serde(default)]
    pub pagination: Option<PaginationConfig>,
    #[serde(default)]
    pub use_browser: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PaginationConfig {
    #[serde(default)]
    pub next_selectors: Vec<String>,
    #[serde(default)]
    pub page_param: Option<String>,
    #[serde(default)]
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub pagination: ApiPaginationConfig,
    #[serde(default)]
    pub url_extraction: UrlExtractionConfig,
    #[serde(default)]
    pub queries: Vec<String>,
    #[serde(default)]
    pub query_param: Option<String>,
    #[serde(default)]
    pub parent: Option<ApiParentConfig>,
    #[serde(default)]
    pub child: Option<ApiChildConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ApiPaginationConfig {
    #[serde(default = "default_page_param")]
    pub page_param: String,
    #[serde(default)]
    pub page_size_param: Option<String>,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_results_path")]
    pub results_path: String,
    #[serde(default)]
    pub cursor_param: Option<String>,
    #[serde(default)]
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UrlExtractionConfig {
    #[serde(default = "default_url_field")]
    pub url_field: String,
    #[serde(default)]
    pub url_template: Option<String>,
    #[serde(default)]
    pub fallback_field: Option<String>,
    #[serde(default)]
    pub items_path: Option<String>,
    /// Nested array paths to traverse (e.g., ["communications", "files"] for communications[*].files[*])
    #[serde(default)]
    pub nested_arrays: Vec<String>,
}

fn default_url_field() -> String {
    "url".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ApiParentConfig {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub pagination: ApiPaginationConfig,
    #[serde(default = "default_results_path")]
    pub results_path: String,
    #[serde(default = "default_id_path")]
    pub id_path: String,
}

fn default_id_path() -> String {
    "id".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ApiChildConfig {
    #[serde(default)]
    pub endpoint_template: String,
    #[serde(default = "default_results_path")]
    pub results_path: String,
    #[serde(default)]
    pub url_extraction: UrlExtractionConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FetchConfig {
    #[serde(default)]
    pub use_browser: bool,
    /// Use binary fetch for PDFs (JavaScript fetch from within browser context).
    /// Required for sites with Akamai/Cloudflare protection on PDF endpoints.
    #[serde(default)]
    pub binary_fetch: bool,
    #[serde(default)]
    pub pdf_selectors: Vec<String>,
    #[serde(default)]
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
    fn test_browser_config_to_engine_config() {
        let config = BrowserConfig {
            enabled: true,
            engine: "stealth".to_string(),
            headless: false,
            proxy: Some("socks5://127.0.0.1:1080".to_string()),
            timeout: 60,
            wait_for_selector: Some("#content".to_string()),
            ..Default::default()
        };

        let engine_config = config.to_engine_config();
        assert!(matches!(engine_config.engine, BrowserEngineType::Stealth));
        assert!(!engine_config.headless);
        assert_eq!(
            engine_config.proxy,
            Some("socks5://127.0.0.1:1080".to_string())
        );
        assert_eq!(engine_config.timeout, 60);
    }

    #[test]
    fn test_browser_engine_type_parsing() {
        let stealth = BrowserConfig {
            engine: "stealth".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            stealth.to_engine_config().engine,
            BrowserEngineType::Stealth
        ));

        let cookies = BrowserConfig {
            engine: "cookies".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            cookies.to_engine_config().engine,
            BrowserEngineType::Cookies
        ));

        let standard = BrowserConfig {
            engine: "standard".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            standard.to_engine_config().engine,
            BrowserEngineType::Standard
        ));

        // Unknown defaults to Stealth
        let unknown = BrowserConfig {
            engine: "unknown".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            unknown.to_engine_config().engine,
            BrowserEngineType::Stealth
        ));
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
