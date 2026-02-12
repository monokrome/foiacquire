//! HTML page fetch methods for browser.

#![allow(dead_code)]

#[cfg(feature = "browser")]
use std::time::Duration;

#[cfg(feature = "browser")]
use anyhow::Result;
#[cfg(feature = "browser")]
use tracing::{debug, warn};

#[cfg(feature = "browser")]
use chromiumoxide::cdp::browser_protocol::network::{GetCookiesParams, SetUserAgentOverrideParams};
#[cfg(feature = "browser")]
use chromiumoxide::cdp::browser_protocol::page::NavigateParams;
#[cfg(feature = "browser")]
use chromiumoxide::Page;

#[cfg(feature = "browser")]
use super::config::BrowserEngineType;
#[cfg(feature = "browser")]
use super::stealth::STEALTH_SCRIPTS;
#[cfg(feature = "browser")]
use super::types::{BrowserCookie, BrowserFetchResponse};

#[cfg(feature = "browser")]
use super::BrowserFetcher;

/// Default user agent for browser requests.
#[cfg(feature = "browser")]
const BROWSER_USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/// JavaScript to wait for page ready state.
#[cfg(feature = "browser")]
const WAIT_FOR_READY_SCRIPT: &str = r#"
    new Promise((resolve) => {
        if (document.readyState === 'complete' || document.readyState === 'interactive') {
            resolve(document.readyState);
        } else {
            document.addEventListener('DOMContentLoaded', () => resolve(document.readyState));
            setTimeout(() => resolve('timeout'), 10000);
        }
    })
"#;

/// Wait for the page to reach a ready state.
#[cfg(feature = "browser")]
async fn wait_for_page_ready(page: &Page, timeout_secs: u64) {
    let ready_timeout = Duration::from_secs(timeout_secs);
    match tokio::time::timeout(
        ready_timeout,
        page.evaluate(WAIT_FOR_READY_SCRIPT.to_string()),
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
            debug!(
                "Could not check ready state (possibly non-HTML page): {}",
                e
            );
        }
        Err(_) => {
            warn!("Timeout waiting for page ready state");
        }
    }
}

/// Wait for a specific selector if configured.
#[cfg(feature = "browser")]
async fn wait_for_selector_if_configured(
    page: &Page,
    selector: Option<&String>,
    timeout_secs: u64,
) {
    if let Some(selector) = selector {
        debug!("Waiting for selector: {}", selector);
        let timeout = Duration::from_secs(timeout_secs);
        match tokio::time::timeout(timeout, page.find_element(selector)).await {
            Ok(Ok(_)) => debug!("Selector found"),
            Ok(Err(e)) => warn!("Selector not found: {}", e),
            Err(_) => warn!("Timeout waiting for selector"),
        }
    }
}

/// Extract cookies from the browser page.
#[cfg(feature = "browser")]
async fn extract_browser_cookies(page: &Page, url: &str) -> Vec<BrowserCookie> {
    let cookie_params = GetCookiesParams::builder()
        .urls(vec![url.to_string()])
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
    browser_cookies
        .iter()
        .map(|c| BrowserCookie {
            name: c.name.clone(),
            value: c.value.clone(),
            domain: c.domain.clone(),
            path: c.path.clone(),
            secure: c.secure,
            http_only: c.http_only,
        })
        .collect()
}

/// Check if page content indicates blocking.
#[cfg(feature = "browser")]
fn check_for_block_indicators(content: &str, url: &str) {
    if content.contains("Access Denied") || content.contains("blocked") {
        warn!(
            "Page may be blocked: {} (contains 'Access Denied' or 'blocked')",
            url
        );
    }
}

#[cfg(feature = "browser")]
impl BrowserFetcher {
    /// Fetch a URL using the browser.
    pub async fn fetch(&mut self, url: &str) -> Result<BrowserFetchResponse> {
        // For cookies-only mode, use regular HTTP
        if self.config.engine == BrowserEngineType::Cookies {
            return self.fetch_with_cookies(url).await;
        }

        self.ensure_browser().await?;

        let browser = self
            .browser
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("browser not initialized after ensure_browser"))?
            .lock()
            .await;
        let page = browser.new_page("about:blank").await?;

        // Use inner function to ensure page is always closed
        let result = self.fetch_inner(&page, url).await;
        let _ = page.close().await;
        result
    }

    /// Inner fetch logic - page cleanup handled by caller.
    pub(crate) async fn fetch_inner(&self, page: &Page, url: &str) -> Result<BrowserFetchResponse> {
        // Set realistic user agent first (before any navigation)
        page.execute(SetUserAgentOverrideParams::new(
            BROWSER_USER_AGENT.to_string(),
        ))
        .await?;

        // Load cookies if configured (before navigation)
        if let Some(ref cookies_file) = self.config.cookies_file {
            if cookies_file.exists() {
                self.load_cookies(page, cookies_file).await?;
            }
        }

        // Navigate to URL with timeout
        self.navigate_to_url(page, url).await?;

        // Wait for page ready state
        wait_for_page_ready(page, self.config.timeout).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Apply stealth scripts AFTER page is ready
        if self.config.engine == BrowserEngineType::Stealth {
            self.apply_stealth(page).await?;
        }

        // Additional wait for dynamic content
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Wait for specific selector if configured
        wait_for_selector_if_configured(
            page,
            self.config.wait_for_selector.as_ref(),
            self.config.timeout,
        )
        .await;

        // Get final URL and content
        let final_url = page
            .url()
            .await?
            .map(|u| u.to_string())
            .unwrap_or_else(|| url.to_string());
        let content = page.content().await?;

        // Extract cookies and check for blocking
        let cookies = extract_browser_cookies(page, &final_url).await;
        check_for_block_indicators(&content, url);

        Ok(BrowserFetchResponse {
            url: url.to_string(),
            final_url,
            status: 200,
            content,
            content_type: "text/html".to_string(),
            cookies,
        })
    }

    /// Navigate to a URL with timeout handling.
    async fn navigate_to_url(&self, page: &Page, url: &str) -> Result<()> {
        tracing::info!("Navigating to {}", url);
        let nav_params = NavigateParams::builder()
            .url(url)
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;

        let nav_timeout = Duration::from_secs(self.config.timeout);
        tokio::time::timeout(nav_timeout, page.execute(nav_params))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Navigation timed out after {}s for {}",
                    self.config.timeout,
                    url
                )
            })?
            .map_err(|e| anyhow::anyhow!("Navigation failed for {}: {}", url, e))?;

        Ok(())
    }

    /// Apply stealth evasion scripts to a page.
    pub(crate) async fn apply_stealth(&self, page: &Page) -> Result<()> {
        debug!("Applying stealth scripts");

        for script in STEALTH_SCRIPTS {
            if let Err(e) = page.evaluate(script.to_string()).await {
                debug!("Stealth script injection skipped: {}", e);
            }
        }

        Ok(())
    }
}
