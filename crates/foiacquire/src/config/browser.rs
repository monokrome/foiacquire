//! Browser engine configuration types.
//!
//! These types define browser configuration for anti-bot protected sites.
//! They live here (always compiled) rather than behind `#[cfg(feature = "browser")]`
//! so that config parsing and serialization work without the browser feature.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Browser engine types.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrowserEngineType {
    /// Standard chromiumoxide with stealth patches (default).
    #[default]
    Stealth,

    /// Use saved cookies with regular HTTP requests (fastest, but cookies expire).
    Cookies,

    /// No stealth patches (for debugging).
    Standard,
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

/// Browser engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct BrowserEngineConfig {
    /// Whether to use browser for fetching (enables browser mode).
    #[serde(default)]
    pub enabled: bool,

    /// Browser engine type.
    #[serde(default)]
    pub engine: BrowserEngineType,

    /// Run in headless mode (default: true).
    /// Set to false for debugging or if headless detection is an issue.
    #[serde(default = "default_headless")]
    pub headless: bool,

    /// Proxy server URL (e.g., "socks5://127.0.0.1:1080").
    #[serde(default)]
    pub proxy: Option<String>,

    /// Path to cookies file for cookie injection mode.
    #[serde(default)]
    pub cookies_file: Option<PathBuf>,

    /// Page load timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Wait for this CSS selector before considering page loaded.
    #[serde(default)]
    pub wait_for_selector: Option<String>,

    /// Additional Chrome arguments.
    #[serde(default)]
    pub chrome_args: Vec<String>,

    /// Remote Chrome DevTools URL (e.g., "ws://localhost:9222").
    /// If set, connects to existing browser instead of launching one.
    /// Can also be set via BROWSER_URL environment variable.
    /// For multiple browsers, use comma-separated URLs or `urls` field.
    #[serde(default)]
    pub remote_url: Option<String>,

    /// Multiple remote browser URLs for load balancing/failover.
    /// Alternative to comma-separated `remote_url`.
    #[serde(default, alias = "remote_urls")]
    pub urls: Vec<String>,

    /// Browser selection strategy when multiple URLs are configured.
    /// Options: round-robin (default), random, per-domain.
    #[serde(default)]
    pub selection: SelectionStrategyType,
}

impl BrowserEngineConfig {
    /// Apply environment variable overrides.
    ///
    /// - `BROWSER_URL` - Remote Chrome DevTools URL(s), comma-separated for multiple
    /// - `BROWSER_SELECTION` - Selection strategy (round-robin, random, per-domain)
    /// - `SOCKS_PROXY` - SOCKS proxy for browser traffic (e.g., "socks5://127.0.0.1:9050")
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(val) = std::env::var("BROWSER_URL") {
            if !val.is_empty() {
                // Check if comma-separated (multiple browsers)
                if val.contains(',') {
                    self.urls = val
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    self.remote_url = None;
                } else {
                    self.remote_url = Some(val);
                }
            }
        }

        if let Ok(val) = std::env::var("BROWSER_SELECTION") {
            if let Some(strategy) = SelectionStrategyType::from_str(&val) {
                self.selection = strategy;
            }
        }

        // Set proxy from SOCKS_PROXY if not already configured
        if self.proxy.is_none() {
            if let Some(proxy) = crate::privacy::socks_proxy_from_env() {
                self.proxy = Some(proxy);
            }
        }

        self
    }

    /// Check if multiple browsers are configured.
    pub fn has_multiple_browsers(&self) -> bool {
        !self.urls.is_empty()
            || self
                .remote_url
                .as_ref()
                .map(|s| s.contains(','))
                .unwrap_or(false)
    }

    /// Get all browser URLs as a Vec.
    /// Returns empty Vec if no remote URLs configured (will use local browser).
    pub fn all_urls(&self) -> Vec<String> {
        if !self.urls.is_empty() {
            return self.urls.clone();
        }
        if let Some(ref url) = self.remote_url {
            if url.contains(',') {
                return url
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            return vec![url.clone()];
        }
        Vec::new()
    }
}

pub fn default_headless() -> bool {
    true
}

pub fn default_timeout() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_browser_engine_type_serde() {
        let stealth: BrowserEngineType = serde_json::from_str("\"stealth\"").unwrap();
        assert_eq!(stealth, BrowserEngineType::Stealth);

        let cookies: BrowserEngineType = serde_json::from_str("\"cookies\"").unwrap();
        assert_eq!(cookies, BrowserEngineType::Cookies);

        let standard: BrowserEngineType = serde_json::from_str("\"standard\"").unwrap();
        assert_eq!(standard, BrowserEngineType::Standard);
    }

    #[test]
    fn test_selection_strategy_type_from_str() {
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

    #[test]
    fn test_selection_strategy_type_from_str_case_insensitive() {
        assert_eq!(
            SelectionStrategyType::from_str("ROUND-ROBIN"),
            Some(SelectionStrategyType::RoundRobin)
        );
        assert_eq!(
            SelectionStrategyType::from_str("Random"),
            Some(SelectionStrategyType::Random)
        );
        assert_eq!(
            SelectionStrategyType::from_str("PER-DOMAIN"),
            Some(SelectionStrategyType::PerDomain)
        );
    }

    #[test]
    fn test_selection_strategy_type_display() {
        assert_eq!(
            format!("{}", SelectionStrategyType::RoundRobin),
            "round-robin"
        );
        assert_eq!(format!("{}", SelectionStrategyType::Random), "random");
        assert_eq!(
            format!("{}", SelectionStrategyType::PerDomain),
            "per-domain"
        );
    }

    #[test]
    fn test_selection_strategy_type_default_is_round_robin() {
        assert_eq!(
            SelectionStrategyType::default(),
            SelectionStrategyType::RoundRobin
        );
    }

    #[test]
    fn test_browser_engine_config_default() {
        let config = BrowserEngineConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.engine, BrowserEngineType::Stealth);
        assert!(!config.headless); // Default::default() for bool is false
        assert!(config.proxy.is_none());
        assert!(config.cookies_file.is_none());
        assert_eq!(config.timeout, 0); // Default::default() for u64 is 0
        assert!(config.wait_for_selector.is_none());
        assert!(config.chrome_args.is_empty());
        assert!(config.remote_url.is_none());
        assert!(config.urls.is_empty());
        assert_eq!(config.selection, SelectionStrategyType::RoundRobin);
    }

    #[test]
    fn test_browser_engine_config_serde_defaults() {
        let config: BrowserEngineConfig = serde_json::from_str("{}").unwrap();
        assert!(!config.enabled);
        assert!(config.headless); // serde default = true via default_headless
        assert_eq!(config.timeout, 30); // serde default via default_timeout
    }

    #[test]
    fn test_browser_engine_config_serde_with_values() {
        let json = r##"{
            "enabled": true,
            "engine": "stealth",
            "headless": false,
            "proxy": "socks5://127.0.0.1:1080",
            "timeout": 60,
            "wait_for_selector": "#content"
        }"##;

        let config: BrowserEngineConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.engine, BrowserEngineType::Stealth);
        assert!(!config.headless);
        assert_eq!(
            config.proxy,
            Some("socks5://127.0.0.1:1080".to_string())
        );
        assert_eq!(config.timeout, 60);
        assert_eq!(
            config.wait_for_selector,
            Some("#content".to_string())
        );
    }

    #[test]
    fn test_remote_urls_alias() {
        let json = r#"{"remote_urls": ["ws://a:9222", "ws://b:9222"]}"#;
        let config: BrowserEngineConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.urls, vec!["ws://a:9222", "ws://b:9222"]);
    }

    #[test]
    fn test_has_multiple_browsers() {
        let single = BrowserEngineConfig {
            remote_url: Some("ws://a:9222".into()),
            ..Default::default()
        };
        assert!(!single.has_multiple_browsers());

        let multi_urls = BrowserEngineConfig {
            urls: vec!["ws://a:9222".into(), "ws://b:9222".into()],
            ..Default::default()
        };
        assert!(multi_urls.has_multiple_browsers());

        let comma = BrowserEngineConfig {
            remote_url: Some("ws://a:9222,ws://b:9222".into()),
            ..Default::default()
        };
        assert!(comma.has_multiple_browsers());
    }

    #[test]
    fn test_all_urls() {
        let empty = BrowserEngineConfig::default();
        assert!(empty.all_urls().is_empty());

        let single = BrowserEngineConfig {
            remote_url: Some("ws://a:9222".into()),
            ..Default::default()
        };
        assert_eq!(single.all_urls(), vec!["ws://a:9222"]);

        let multi = BrowserEngineConfig {
            urls: vec!["ws://a:9222".into(), "ws://b:9222".into()],
            ..Default::default()
        };
        assert_eq!(multi.all_urls(), vec!["ws://a:9222", "ws://b:9222"]);
    }
}
