//! Browser-based fetcher for anti-bot protected sites.
//!
//! Uses chromiumoxide (CDP) with stealth evasion techniques to bypass
//! bot detection systems like Akamai, Cloudflare, etc.

#![allow(dead_code)]

use std::path::PathBuf;
#[cfg(feature = "browser")]
use std::sync::Arc;
#[cfg(feature = "browser")]
use std::time::Duration;

#[cfg(feature = "browser")]
use anyhow::Context;
use anyhow::Result;
use serde::{Deserialize, Serialize};
#[cfg(feature = "browser")]
use tokio::sync::Mutex;
#[cfg(feature = "browser")]
use tracing::{debug, info, warn};

#[cfg(feature = "browser")]
use chromiumoxide::cdp::browser_protocol::network::{
    CookieParam, GetCookiesParams, SetUserAgentOverrideParams,
};
#[cfg(feature = "browser")]
use chromiumoxide::cdp::browser_protocol::page::NavigateParams;
#[cfg(feature = "browser")]
use chromiumoxide::{Browser, BrowserConfig, Page};
#[cfg(feature = "browser")]
use futures::StreamExt;

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

fn default_headless() -> bool {
    true
}

fn default_timeout() -> u64 {
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

/// Response from browser fetch.
#[derive(Debug, Clone)]
pub struct BrowserFetchResponse {
    pub url: String,
    pub final_url: String,
    pub status: u16,
    pub content: String,
    pub content_type: String,
    /// Cookies from the browser session (for subsequent HTTP requests).
    pub cookies: Vec<BrowserCookie>,
}

/// Cookie extracted from browser session.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BrowserCookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
}

/// Response from binary fetch (PDF, images, etc).
#[derive(Debug, Clone)]
pub struct BinaryFetchResponse {
    pub url: String,
    pub status: u16,
    pub content_type: String,
    pub data: Vec<u8>,
    pub size: usize,
}

/// Stealth evasion JavaScript to inject into pages.
/// Based on puppeteer-extra-plugin-stealth techniques.
const STEALTH_SCRIPTS: &[&str] = &[
    // Remove webdriver property
    r#"
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    "#,
    // Fix chrome object
    r#"
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {}
    };
    "#,
    // Fix permissions
    r#"
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
    );
    "#,
    // Fix plugins (make it look like regular Chrome)
    r#"
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
            { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
        ],
        configurable: true
    });
    "#,
    // Fix languages
    r#"
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });
    "#,
    // Fix platform (if on Linux, keep it; don't pretend to be Windows)
    r#"
    if (!navigator.platform.includes('Win')) {
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Linux x86_64',
            configurable: true
        });
    }
    "#,
    // Remove automation-related properties
    r#"
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
    "#,
    // Fix WebGL vendor/renderer (common detection vector)
    r#"
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) {
            return 'Intel Inc.';
        }
        if (parameter === 37446) {
            return 'Intel Iris OpenGL Engine';
        }
        return getParameter.call(this, parameter);
    };
    "#,
    // Fix hairline feature detection
    r#"
    Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
        get: function() {
            if (this.id === 'modernizr') return 1;
            return this.getBoundingClientRect().height;
        }
    });
    "#,
];

/// Browser-based fetcher with stealth capabilities.
#[cfg(feature = "browser")]
pub struct BrowserFetcher {
    config: BrowserEngineConfig,
    browser: Option<Arc<Mutex<Browser>>>,
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
                info!("Found Chrome at: {}", path);
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
                        info!("Found Chrome in PATH: {}", path);
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

        info!("Launching browser (headless={})", self.config.headless);

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
        info!("Connecting to remote browser at {}", url);

        // Get WebSocket URL from the /json/version endpoint
        let http_url = url
            .replace("ws://", "http://")
            .replace("wss://", "https://");
        let version_url = format!("{}/json/version", http_url.trim_end_matches('/'));

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

        info!("Connecting to WebSocket: {}", ws_url);

        let (browser, mut handler) = Browser::connect(ws_url)
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

    /// Fetch a URL using the browser.
    pub async fn fetch(&mut self, url: &str) -> Result<BrowserFetchResponse> {
        // For cookies-only mode, use regular HTTP
        if self.config.engine == BrowserEngineType::Cookies {
            return self.fetch_with_cookies(url).await;
        }

        self.ensure_browser().await?;

        let browser = self.browser.as_ref().unwrap().lock().await;
        let page = browser.new_page("about:blank").await?;

        // Set realistic user agent first (before any navigation)
        let user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
        page.execute(SetUserAgentOverrideParams::new(user_agent.to_string()))
            .await?;

        // Load cookies if configured (before navigation)
        if let Some(ref cookies_file) = self.config.cookies_file {
            if cookies_file.exists() {
                self.load_cookies(&page, cookies_file).await?;
            }
        }

        // Navigate to URL first
        info!("Navigating to {}", url);
        let nav_params = NavigateParams::builder()
            .url(url)
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;

        page.execute(nav_params).await?;

        // Wait for page to be ready before applying stealth scripts
        // This uses document.readyState instead of a fixed timeout
        let wait_for_ready_script = r#"
            new Promise((resolve) => {
                if (document.readyState === 'complete' || document.readyState === 'interactive') {
                    resolve(document.readyState);
                } else {
                    document.addEventListener('DOMContentLoaded', () => resolve(document.readyState));
                    // Fallback timeout in case event never fires
                    setTimeout(() => resolve('timeout'), 10000);
                }
            })
        "#;

        let ready_timeout = Duration::from_secs(self.config.timeout);
        match tokio::time::timeout(
            ready_timeout,
            page.evaluate(wait_for_ready_script.to_string()),
        )
        .await
        {
            Ok(Ok(result)) => {
                let state: String = result
                    .into_value()
                    .unwrap_or_else(|_| "unknown".to_string());
                debug!("Page ready state: {}", state);
            }
            Ok(Err(e)) => {
                // Script execution failed - might be a non-HTML page (PDF)
                debug!(
                    "Could not check ready state (possibly non-HTML page): {}",
                    e
                );
            }
            Err(_) => {
                warn!("Timeout waiting for page ready state");
            }
        }

        // Small additional delay for any late-loading scripts
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Apply stealth scripts AFTER page is ready (they need a real page context)
        if self.config.engine == BrowserEngineType::Stealth {
            self.apply_stealth(&page).await?;
        }

        // Additional wait for dynamic content to render
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Wait for specific selector if configured
        if let Some(ref selector) = self.config.wait_for_selector {
            debug!("Waiting for selector: {}", selector);
            let timeout = Duration::from_secs(self.config.timeout);
            match tokio::time::timeout(timeout, page.find_element(selector)).await {
                Ok(Ok(_)) => debug!("Selector found"),
                Ok(Err(e)) => warn!("Selector not found: {}", e),
                Err(_) => warn!("Timeout waiting for selector"),
            }
        }

        // Get final URL and content
        let final_url = page
            .url()
            .await?
            .map(|u| u.to_string())
            .unwrap_or_else(|| url.to_string());

        let content = page.content().await?;

        // Extract cookies from the session for use in subsequent HTTP requests
        // Use explicit URL to ensure we get all cookies for the domain
        let cookie_params = GetCookiesParams::builder()
            .urls(vec![final_url.clone()])
            .build();
        let browser_cookies = match page.execute(cookie_params).await {
            Ok(result) => result.result.cookies,
            Err(e) => {
                warn!(
                    "Failed to get cookies via CDP: {}, trying page.get_cookies()",
                    e
                );
                page.get_cookies().await.unwrap_or_default()
            }
        };
        debug!("Got {} cookies from browser", browser_cookies.len());
        let cookies: Vec<BrowserCookie> = browser_cookies
            .iter()
            .map(|c| BrowserCookie {
                name: c.name.clone(),
                value: c.value.clone(),
                domain: c.domain.clone(),
                path: c.path.clone(),
                secure: c.secure,
                http_only: c.http_only,
            })
            .collect();

        // Check for common block indicators
        if content.contains("Access Denied") || content.contains("blocked") {
            warn!(
                "Page may be blocked: {} (contains 'Access Denied' or 'blocked')",
                url
            );
        }

        // Close the page to prevent tab accumulation
        let _ = page.close().await;

        Ok(BrowserFetchResponse {
            url: url.to_string(),
            final_url,
            status: 200, // CDP doesn't give us status codes easily
            content,
            content_type: "text/html".to_string(),
            cookies,
        })
    }

    /// Apply stealth evasion scripts to a page.
    async fn apply_stealth(&self, page: &Page) -> Result<()> {
        debug!("Applying stealth scripts");

        for script in STEALTH_SCRIPTS {
            if let Err(e) = page.evaluate(script.to_string()).await {
                // This can fail on non-HTML pages (PDFs, etc.) or during page transitions
                // It's not critical - the scripts are best-effort evasion
                debug!("Stealth script injection skipped: {}", e);
            }
        }

        Ok(())
    }

    /// Load cookies from a JSON file.
    async fn load_cookies(&self, page: &Page, path: &PathBuf) -> Result<()> {
        debug!("Loading cookies from {:?}", path);

        let content = std::fs::read_to_string(path)?;
        let cookies: Vec<serde_json::Value> = serde_json::from_str(&content)?;

        for cookie in cookies {
            let name = cookie
                .get("name")
                .or_else(|| cookie.get("key"))
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let value = cookie
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let domain = cookie
                .get("domain")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            if name.is_empty() || domain.is_empty() {
                continue;
            }

            let cookie_param = CookieParam::builder()
                .name(name)
                .value(value)
                .domain(domain)
                .build();

            match cookie_param {
                Ok(param) => {
                    if let Err(e) = page.set_cookie(param).await {
                        warn!("Failed to set cookie {}: {}", name, e);
                    }
                }
                Err(e) => {
                    warn!("Failed to build cookie {}: {}", name, e);
                }
            }
        }

        Ok(())
    }

    /// Fetch using saved cookies with regular HTTP (fastest method).
    async fn fetch_with_cookies(&self, url: &str) -> Result<BrowserFetchResponse> {
        let cookies_file = self
            .config
            .cookies_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Cookies file required for cookies engine"))?;

        if !cookies_file.exists() {
            return Err(anyhow::anyhow!(
                "Cookies file not found: {:?}",
                cookies_file
            ));
        }

        debug!("Fetching {} with cookies", url);

        let content = std::fs::read_to_string(cookies_file)?;
        let cookies: Vec<serde_json::Value> = serde_json::from_str(&content)?;

        // Build reqwest client with cookies
        let jar = reqwest::cookie::Jar::default();
        for cookie in &cookies {
            let name = cookie
                .get("name")
                .or_else(|| cookie.get("key"))
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let value = cookie
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let domain = cookie
                .get("domain")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            if !name.is_empty() && !domain.is_empty() {
                let cookie_str = format!("{}={}; Domain={}", name, value, domain);
                if let Ok(url_parsed) = url.parse::<reqwest::Url>() {
                    jar.add_cookie_str(&cookie_str, &url_parsed);
                }
            }
        }

        let mut client_builder = reqwest::Client::builder()
            .cookie_provider(Arc::new(jar))
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .timeout(Duration::from_secs(self.config.timeout));

        if let Some(ref proxy) = self.config.proxy {
            client_builder = client_builder.proxy(reqwest::Proxy::all(proxy)?);
        }

        let client = client_builder.build()?;
        let response = client.get(url).send().await?;

        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("text/html")
            .to_string();
        let final_url = response.url().to_string();
        let content = response.text().await?;

        Ok(BrowserFetchResponse {
            url: url.to_string(),
            final_url,
            status,
            content,
            content_type,
            cookies: Vec::new(), // Cookies were loaded from file, not extracted
        })
    }

    /// Save cookies from the last fetch response to a file.
    pub async fn save_cookies_from_response(
        &self,
        cookies: &[BrowserCookie],
        path: &PathBuf,
    ) -> Result<()> {
        let json = serde_json::to_string_pretty(&cookies)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)?;

        info!("Saved {} cookies to {:?}", cookies.len(), path);

        Ok(())
    }

    /// Save current browser cookies to a file (deprecated - use save_cookies_from_response).
    pub async fn save_cookies(&mut self, path: &PathBuf) -> Result<()> {
        self.ensure_browser().await?;

        let browser = self.browser.as_ref().unwrap().lock().await;
        // Get all cookies from the browser storage (not a blank page)
        let cookies = browser.get_cookies().await?;

        let json = serde_json::to_string_pretty(&cookies)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)?;

        info!("Saved {} cookies to {:?}", cookies.len(), path);

        Ok(())
    }

    /// Fetch a binary file (like PDF) using JavaScript from within a page context.
    /// This is needed for sites like CIA Reading Room where Akamai blocks direct requests
    /// but allows JavaScript fetch() from within a valid browser session.
    pub async fn fetch_binary(
        &mut self,
        url: &str,
        context_url: Option<&str>,
    ) -> Result<BinaryFetchResponse> {
        self.ensure_browser().await?;

        let browser = self.browser.as_ref().unwrap().lock().await;

        // First, navigate to a context page if provided (to establish session)
        let page = if let Some(ctx_url) = context_url {
            info!("Establishing session at {}", ctx_url);
            let page = browser.new_page(ctx_url).await?;

            // Set realistic user agent
            let user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
            page.execute(SetUserAgentOverrideParams::new(user_agent.to_string()))
                .await?;

            // Wait for page to load and Akamai scripts to run
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Apply stealth if configured
            if self.config.engine == BrowserEngineType::Stealth {
                self.apply_stealth(&page).await?;
            }

            page
        } else {
            browser.new_page("about:blank").await?
        };

        info!("Fetching binary from {}", url);

        // Use JavaScript fetch to download the file
        // This uses the browser's cookies and session state
        let fetch_script = format!(
            r#"
            (async () => {{
                try {{
                    const response = await fetch('{}', {{
                        method: 'GET',
                        credentials: 'include',
                        headers: {{
                            'Accept': 'application/pdf, */*'
                        }}
                    }});

                    if (!response.ok) {{
                        return {{
                            error: `HTTP ${{response.status}}: ${{response.statusText}}`,
                            status: response.status,
                            headers: Object.fromEntries(response.headers.entries())
                        }};
                    }}

                    const contentType = response.headers.get('content-type') || 'application/octet-stream';
                    const blob = await response.blob();
                    const arrayBuffer = await blob.arrayBuffer();
                    const bytes = new Uint8Array(arrayBuffer);

                    // Convert to base64
                    let binary = '';
                    for (let i = 0; i < bytes.length; i++) {{
                        binary += String.fromCharCode(bytes[i]);
                    }}
                    const base64 = btoa(binary);

                    return {{
                        status: response.status,
                        contentType: contentType,
                        size: bytes.length,
                        data: base64,
                        headers: Object.fromEntries(response.headers.entries())
                    }};
                }} catch (e) {{
                    return {{ error: e.toString() }};
                }}
            }})()
            "#,
            url
        );

        let result: serde_json::Value = page
            .evaluate(fetch_script)
            .await?
            .into_value()
            .context("Failed to parse fetch result")?;

        debug!("Fetch result: {:?}", result);

        if let Some(error) = result.get("error").and_then(|e| e.as_str()) {
            return Err(anyhow::anyhow!("JavaScript fetch failed: {}", error));
        }

        let status = result.get("status").and_then(|s| s.as_u64()).unwrap_or(0) as u16;
        let content_type = result
            .get("contentType")
            .and_then(|c| c.as_str())
            .unwrap_or("application/octet-stream")
            .to_string();
        let size = result.get("size").and_then(|s| s.as_u64()).unwrap_or(0) as usize;
        let data_b64 = result.get("data").and_then(|d| d.as_str()).unwrap_or("");

        // Decode base64
        use base64::Engine;
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .context("Failed to decode base64 data")?;

        info!(
            "Downloaded {} bytes, content-type: {}",
            data.len(),
            content_type
        );

        // Close the page to prevent tab accumulation
        let _ = page.close().await;

        Ok(BinaryFetchResponse {
            url: url.to_string(),
            status,
            content_type,
            data,
            size,
        })
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
