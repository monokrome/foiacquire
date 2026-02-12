//! Binary file fetch methods (PDF, images, etc).

#![allow(dead_code)]

#[cfg(feature = "browser")]
use std::time::Duration;

#[cfg(feature = "browser")]
use anyhow::Context;
#[cfg(feature = "browser")]
use anyhow::Result;
#[cfg(feature = "browser")]
use tracing::{debug, info};

#[cfg(feature = "browser")]
use chromiumoxide::cdp::browser_protocol::network::SetUserAgentOverrideParams;
#[cfg(feature = "browser")]
use chromiumoxide::Page;

#[cfg(feature = "browser")]
use super::config::BrowserEngineType;
#[cfg(feature = "browser")]
use super::types::BinaryFetchResponse;

#[cfg(feature = "browser")]
use super::BrowserFetcher;

#[cfg(feature = "browser")]
impl BrowserFetcher {
    /// Fetch a binary file (like PDF) using JavaScript from within a page context.
    /// This is needed for sites like CIA Reading Room where Akamai blocks direct requests
    /// but allows JavaScript fetch() from within a valid browser session.
    pub async fn fetch_binary(
        &mut self,
        url: &str,
        context_url: Option<&str>,
    ) -> Result<BinaryFetchResponse> {
        self.ensure_browser().await?;

        let browser = self
            .browser
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("browser not initialized after ensure_browser"))?
            .lock()
            .await;

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

        // Use inner function to ensure page is always closed
        let result = self.fetch_binary_inner(&page, url).await;
        let _ = page.close().await;
        result
    }

    /// Inner binary fetch logic - page cleanup handled by caller.
    async fn fetch_binary_inner(&self, page: &Page, url: &str) -> Result<BinaryFetchResponse> {
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

        Ok(BinaryFetchResponse {
            url: url.to_string(),
            status,
            content_type,
            data,
            size,
        })
    }
}
