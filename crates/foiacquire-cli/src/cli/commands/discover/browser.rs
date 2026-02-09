//! Browser-based fetch testing command.

use console::style;

/// Test browser-based fetching with stealth capabilities.
#[cfg(feature = "browser")]
#[allow(clippy::too_many_arguments)]
pub async fn cmd_browser_test(
    url: &str,
    headed: bool,
    engine: &str,
    proxy: Option<String>,
    browser_url: Option<String>,
    cookies_file: Option<std::path::PathBuf>,
    save_cookies: Option<std::path::PathBuf>,
    output: Option<std::path::PathBuf>,
    binary: bool,
    context_url: Option<String>,
) -> anyhow::Result<()> {
    use foiacquire::scrapers::{BrowserEngineConfig, BrowserEngineType, BrowserFetcher};

    println!("{} Testing browser fetch: {}", style("→").cyan(), url);
    println!("  Engine: {}", engine);
    println!("  Headless: {}", !headed);
    println!("  Binary mode: {}", binary);
    if let Some(ref p) = proxy {
        println!("  Proxy: {}", p);
    }
    if let Some(ref b) = browser_url {
        println!("  Remote browser: {}", b);
    }
    if let Some(ref c) = cookies_file {
        println!("  Cookies: {:?}", c);
    }
    if let Some(ref ctx) = context_url {
        println!("  Context URL: {}", ctx);
    }

    let engine_type = match engine.to_lowercase().as_str() {
        "stealth" => BrowserEngineType::Stealth,
        "cookies" => BrowserEngineType::Cookies,
        "standard" => BrowserEngineType::Standard,
        _ => {
            println!(
                "{} Unknown engine '{}', using stealth",
                style("!").yellow(),
                engine
            );
            BrowserEngineType::Stealth
        }
    };

    let config = BrowserEngineConfig {
        engine: engine_type,
        headless: !headed,
        proxy,
        cookies_file,
        timeout: 30,
        wait_for_selector: None,
        chrome_args: vec![],
        remote_url: browser_url,
        remote_urls: vec![],
        selection: Default::default(),
    }
    .with_env_overrides();

    let mut fetcher = BrowserFetcher::new(config);

    println!("{} Launching browser...", style("→").cyan());

    // Binary fetch mode (for PDFs, images, etc.)
    if binary {
        match fetcher.fetch_binary(url, context_url.as_deref()).await {
            Ok(response) => {
                println!("{} Binary fetch successful!", style("✓").green());
                println!("  Status: {}", response.status);
                println!("  Content-Type: {}", response.content_type);
                println!("  Size: {} bytes", response.data.len());

                // Save binary content
                if let Some(output_path) = output {
                    std::fs::write(&output_path, &response.data)?;
                    println!("{} Saved binary to {:?}", style("✓").green(), output_path);

                    // Verify PDF magic bytes
                    if response.data.len() >= 4 && &response.data[0..4] == b"%PDF" {
                        println!("{} Verified: File is a valid PDF", style("✓").green());
                    } else if response.data.len() >= 4 {
                        println!(
                            "{} Warning: File does not have PDF magic bytes (got: {:?})",
                            style("!").yellow(),
                            &response.data[0..std::cmp::min(4, response.data.len())]
                        );
                    }
                } else {
                    println!(
                        "{} Use --output to save binary content",
                        style("!").yellow()
                    );
                }
            }
            Err(e) => {
                println!("{} Binary fetch failed: {}", style("✗").red(), e);
                return Err(e);
            }
        }
    } else {
        // Regular HTML fetch
        match fetcher.fetch(url).await {
            Ok(response) => {
                println!("{} Fetch successful!", style("✓").green());
                println!("  Final URL: {}", response.final_url);
                println!("  Status: {}", response.status);
                println!("  Content-Type: {}", response.content_type);
                println!("  Content length: {} bytes", response.content.len());

                // Check for common block indicators
                if response.content.contains("Access Denied") {
                    println!(
                        "{} Warning: Page contains 'Access Denied' - may be blocked",
                        style("!").yellow()
                    );
                }
                if response.content.contains("blocked") || response.content.contains("captcha") {
                    println!(
                        "{} Warning: Page may contain block/captcha indicators",
                        style("!").yellow()
                    );
                }

                // Save or print content
                if let Some(output_path) = output {
                    std::fs::write(&output_path, &response.content)?;
                    println!("{} Saved content to {:?}", style("✓").green(), output_path);
                } else {
                    // Print first 500 chars as preview
                    let preview: String = response.content.chars().take(500).collect();
                    println!(
                        "\n--- Content Preview ---\n{}\n--- End Preview ---",
                        preview
                    );
                }

                // Save cookies if requested
                if let Some(save_path) = save_cookies {
                    fetcher.save_cookies(&save_path).await?;
                    println!("{} Saved cookies to {:?}", style("✓").green(), save_path);
                }
            }
            Err(e) => {
                println!("{} Fetch failed: {}", style("✗").red(), e);
                return Err(e);
            }
        }
    }

    fetcher.close().await;

    Ok(())
}
