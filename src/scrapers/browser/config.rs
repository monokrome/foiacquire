//! Browser engine configuration types.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    #[serde(default)]
    pub remote_url: Option<String>,
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
