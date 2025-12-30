//! Browser-based fetcher for anti-bot protected sites.
//!
//! Uses chromiumoxide (CDP) with stealth evasion techniques to bypass
//! bot detection systems like Akamai, Cloudflare, etc.

#![allow(dead_code)]

mod binary;
mod config;
mod cookies;
mod fetch;
mod stealth;
mod types;

pub use config::{default_headless, default_timeout, BrowserEngineConfig, BrowserEngineType};
#[allow(unused_imports)]
pub use types::{BinaryFetchResponse, BrowserCookie, BrowserFetchResponse};

#[cfg(not(feature = "browser"))]
use std::path::PathBuf;
#[cfg(feature = "browser")]
use std::sync::Arc;
#[cfg(feature = "browser")]
use std::time::Duration;

#[cfg(feature = "browser")]
use anyhow::Context;
use anyhow::Result;
#[cfg(feature = "browser")]
use tokio::sync::Mutex;
#[cfg(feature = "browser")]
use tracing::debug;

#[cfg(feature = "browser")]
use chromiumoxide::{Browser, BrowserConfig};
#[cfg(feature = "browser")]
use futures::StreamExt;

/// Browser-based fetcher with stealth capabilities.
#[cfg(feature = "browser")]
pub struct BrowserFetcher {
    pub(crate) config: BrowserEngineConfig,
    pub(crate) browser: Option<Arc<Mutex<Browser>>>,
}

#[cfg(feature = "browser")]
impl BrowserFetcher {
    /// Common Chrome executable paths to check.
    const CHROME_PATHS: &'static [&'static str] = &[
        // Linux
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/snap/bin/chromium",
        // macOS
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        // Common install locations
        "/opt/google/chrome/google-chrome",
    ];

    /// Create a new browser fetcher.
    pub fn new(config: BrowserEngineConfig) -> Self {
        Self {
            config,
            browser: None,
        }
    }

    /// Find Chrome executable.
    async fn find_or_download_chrome() -> Result<std::path::PathBuf> {
        // First, check common paths
        for path in Self::CHROME_PATHS {
            let p = std::path::Path::new(path);
            if p.exists() {
                debug!("Found Chrome at: {}", path);
                return Ok(p.to_path_buf());
            }
        }

        // Check if in PATH via `which`
        for cmd in &[
            "google-chrome",
            "google-chrome-stable",
            "chromium",
            "chromium-browser",
        ] {
            if let Ok(output) = std::process::Command::new("which").arg(cmd).output() {
                if output.status.success() {
                    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if !path.is_empty() {
                        debug!("Found Chrome in PATH: {}", path);
                        return Ok(std::path::PathBuf::from(path));
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Chrome/Chromium not found. Please install it:\n\
             - Arch/Manjaro: sudo pacman -S chromium\n\
             - Ubuntu/Debian: sudo apt install chromium-browser\n\
             - Fedora: sudo dnf install chromium\n\
             - Or download from: https://www.google.com/chrome/"
        ))
    }

    /// Launch or connect to browser if not already running.
    pub async fn ensure_browser(&mut self) -> Result<()> {
        if self.browser.is_some() {
            return Ok(());
        }

        // If remote URL is configured, connect to existing browser
        if let Some(remote_url) = self.config.remote_url.clone() {
            return self.connect_remote(&remote_url).await;
        }

        debug!("Launching browser (headless={})", self.config.headless);

        // Try to find Chrome, or download it
        let chrome_path = Self::find_or_download_chrome().await?;

        let mut builder = BrowserConfig::builder().chrome_executable(chrome_path);

        // Set headless mode (with_head means NOT headless, confusingly)
        if !self.config.headless {
            builder = builder.with_head();
        }

        // Add proxy if configured
        if let Some(ref proxy) = self.config.proxy {
            builder = builder.arg(format!("--proxy-server={}", proxy));
        }

        // Add stealth-related Chrome args
        builder = builder
            .arg("--disable-blink-features=AutomationControlled")
            .arg("--disable-infobars")
            .arg("--disable-dev-shm-usage")
            .arg("--no-first-run")
            .arg("--no-default-browser-check")
            .arg("--disable-background-networking")
            .arg("--disable-sync")
            .arg("--disable-translate")
            .arg("--metrics-recording-only")
            .arg("--safebrowsing-disable-auto-update")
            .arg("--no-sandbox") // Often needed for headless in containers/restricted environments
            .arg("--disable-gpu") // Recommended for headless
            .arg("--disable-software-rasterizer");

        // Add custom args
        for arg in &self.config.chrome_args {
            builder = builder.arg(arg);
        }

        let config = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build browser config: {}", e))?;

        let (browser, mut handler) = Browser::launch(config)
            .await
            .context("Failed to launch browser")?;

        // Spawn handler task
        tokio::spawn(async move {
            while let Some(h) = handler.next().await {
                if h.is_err() {
                    break;
                }
            }
        });

        self.browser = Some(Arc::new(Mutex::new(browser)));

        Ok(())
    }

    /// Connect to a remote Chrome instance.
    async fn connect_remote(&mut self, url: &str) -> Result<()> {
        debug!(
            "Connecting to remote browser at {} (timeout: {}s)",
            url, self.config.timeout
        );

        // Chrome DevTools only accepts connections with IP addresses or localhost in the Host header.
        // When using Docker container names (e.g., ws://stealth:9222), we need to resolve the
        // hostname to an IP address first.
        let resolved_url = Self::resolve_hostname_to_ip(url).await.unwrap_or_else(|e| {
            debug!(
                "Could not resolve hostname to IP: {}, using original URL",
                e
            );
            url.to_string()
        });

        // Get WebSocket URL from the /json/version endpoint
        let http_url = resolved_url
            .replace("ws://", "http://")
            .replace("wss://", "https://");
        let version_url = format!("{}/json/version", http_url.trim_end_matches('/'));

        debug!("Fetching browser version from: {}", version_url);

        let client = reqwest::Client::new();
        let resp: serde_json::Value = client
            .get(&version_url)
            .send()
            .await
            .context("Failed to connect to remote browser")?
            .json()
            .await
            .context("Failed to parse browser version info")?;

        let ws_url = resp
            .get("webSocketDebuggerUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("No webSocketDebuggerUrl in response"))?;

        debug!("Connecting to WebSocket: {}", ws_url);

        // Configure browser with custom request timeout
        let handler_config = chromiumoxide::handler::HandlerConfig {
            request_timeout: Duration::from_secs(self.config.timeout),
            ..Default::default()
        };

        let (browser, mut handler) = Browser::connect_with_config(ws_url, handler_config)
            .await
            .context("Failed to connect to remote browser")?;

        // Spawn handler task
        tokio::spawn(async move {
            while let Some(h) = handler.next().await {
                if h.is_err() {
                    break;
                }
            }
        });

        self.browser = Some(Arc::new(Mutex::new(browser)));

        Ok(())
    }

    /// Resolve a hostname in a URL to an IP address.
    /// Chrome DevTools rejects connections with non-IP Host headers for security.
    async fn resolve_hostname_to_ip(url: &str) -> Result<String> {
        use std::net::ToSocketAddrs;

        // Parse the URL to extract host and port
        let url_obj = url::Url::parse(url).context("Invalid URL")?;
        let host = url_obj
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("No host in URL"))?;
        let port = url_obj.port().unwrap_or(9222);

        // Skip resolution for localhost and IP addresses
        if host == "localhost" || host.parse::<std::net::IpAddr>().is_ok() {
            return Ok(url.to_string());
        }

        // Resolve hostname to IP (blocking, but fast for local DNS)
        let addr_str = format!("{}:{}", host, port);
        let resolved = tokio::task::spawn_blocking(move || addr_str.to_socket_addrs().ok()?.next())
            .await
            .context("DNS resolution task failed")?
            .ok_or_else(|| anyhow::anyhow!("Could not resolve hostname: {}", host))?;

        // Rebuild URL with IP address
        let ip = resolved.ip();
        let mut new_url = url_obj.clone();
        new_url
            .set_host(Some(&ip.to_string()))
            .map_err(|_| anyhow::anyhow!("Failed to set host in URL"))?;

        debug!("Resolved {} -> {}", url, new_url);
        Ok(new_url.to_string())
    }

    /// Close the browser.
    pub async fn close(&mut self) {
        self.browser = None;
    }
}

// Stub for when browser feature is disabled
#[cfg(not(feature = "browser"))]
pub struct BrowserFetcher {
    config: BrowserEngineConfig,
}

#[cfg(not(feature = "browser"))]
impl BrowserFetcher {
    pub fn new(config: BrowserEngineConfig) -> Self {
        Self { config }
    }

    pub async fn fetch(&mut self, _url: &str) -> Result<BrowserFetchResponse> {
        Err(anyhow::anyhow!(
            "Browser support not compiled. Rebuild with: cargo build --features browser"
        ))
    }

    pub async fn save_cookies(&mut self, _path: &PathBuf) -> Result<()> {
        Err(anyhow::anyhow!(
            "Browser support not compiled. Rebuild with: cargo build --features browser"
        ))
    }

    pub async fn close(&mut self) {}
}
