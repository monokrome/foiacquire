//! Cookie loading and saving for browser sessions.

#![allow(dead_code)]

use std::path::PathBuf;

#[cfg(feature = "browser")]
use std::sync::Arc;
#[cfg(feature = "browser")]
use std::time::Duration;

use anyhow::Result;
#[cfg(feature = "browser")]
use tracing::{debug, info, warn};

#[cfg(feature = "browser")]
use chromiumoxide::cdp::browser_protocol::network::CookieParam;
#[cfg(feature = "browser")]
use chromiumoxide::Page;

use super::types::{BrowserCookie, BrowserFetchResponse};

#[cfg(feature = "browser")]
use super::BrowserFetcher;

#[cfg(feature = "browser")]
impl BrowserFetcher {
    /// Load cookies from a JSON file.
    pub(crate) async fn load_cookies(&self, page: &Page, path: &PathBuf) -> Result<()> {
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
    pub(crate) async fn fetch_with_cookies(&self, url: &str) -> Result<BrowserFetchResponse> {
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
}
