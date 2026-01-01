//! Browser engine configuration types.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::selection::SelectionStrategyType;

/// Browser engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BrowserEngineConfig {
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
    /// For multiple browsers, use comma-separated URLs or `remote_urls` field.
    #[serde(default)]
    pub remote_url: Option<String>,

    /// Multiple remote browser URLs for load balancing/failover.
    /// Alternative to comma-separated `remote_url`.
    #[serde(default)]
    pub remote_urls: Vec<String>,

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
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(val) = std::env::var("BROWSER_URL") {
            if !val.is_empty() {
                // Check if comma-separated (multiple browsers)
                if val.contains(',') {
                    self.remote_urls = val
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

        self
    }

    /// Check if multiple browsers are configured.
    pub fn has_multiple_browsers(&self) -> bool {
        !self.remote_urls.is_empty()
            || self
                .remote_url
                .as_ref()
                .map(|s| s.contains(','))
                .unwrap_or(false)
    }

    /// Get all browser URLs as a Vec.
    /// Returns empty Vec if no remote URLs configured (will use local browser).
    pub fn all_urls(&self) -> Vec<String> {
        if !self.remote_urls.is_empty() {
            return self.remote_urls.clone();
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

/// Browser engine types.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
