//! Search engine discovery command.

use console::style;

use foia::config::Settings;
use foia_scrape::discovery::{DiscoveredUrl, DiscoverySourceConfig};

use super::{add_discovered_urls, get_source_base_url};

/// Discover URLs using external search engines.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_discover_search(
    settings: &Settings,
    source_id: &str,
    engines: &str,
    terms: Option<&str>,
    expand: bool,
    template: bool,
    max_results: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    use foia_scrape::discovery::sources::search::create_search_engine;
    use foia_scrape::discovery::term_extraction::{
        ExtractionContext, LlmTermExtractor, TemplateTermExtractor, TermExtractor,
    };

    let base_url = get_source_base_url(settings, source_id).await?;
    let domain = url::Url::parse(&base_url)?
        .host_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| base_url.clone());

    println!(
        "{} Search-based discovery for {}",
        style("🔍").cyan(),
        style(&domain).bold()
    );

    // Get search terms
    let mut search_terms: Vec<String> = if let Some(t) = terms {
        t.split(',').map(|s| s.trim().to_string()).collect()
    } else {
        // Try to get terms from scraper config in database
        let repos = settings.repositories()?;
        repos
            .scraper_configs
            .get(source_id)
            .await?
            .map(|s| s.discovery.search_queries.clone())
            .unwrap_or_default()
    };

    if search_terms.is_empty() {
        // Default terms for FOIA document discovery
        search_terms = vec![
            "FOIA".to_string(),
            "documents".to_string(),
            "reading room".to_string(),
            "reports".to_string(),
        ];
    }

    println!("  Initial terms: {}", search_terms.join(", "));

    // Template-based term extraction
    if template {
        println!(
            "\n{} Extracting terms from HTML templates...",
            style("📝").cyan()
        );
        let extractor = TemplateTermExtractor::with_defaults();
        let context = ExtractionContext::for_domain(&domain);

        // Fetch the homepage for template extraction
        // ALLOWED: One-off homepage fetch in CLI command for term extraction
        // This is a lightweight operation outside the main scraping pipeline
        // TODO: Consider passing privacy config through CLI arguments if needed
        #[allow(clippy::disallowed_methods)]
        let client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (compatible; foia/1.0)")
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        if let Ok(response) = client.get(&base_url).send().await {
            if let Ok(html) = response.text().await {
                let context = context.with_html(&html);
                if let Ok(extracted) = extractor.extract_terms(&search_terms, &context).await {
                    println!("  Extracted {} template terms", extracted.len());
                    for term in extracted.iter().take(10) {
                        if !search_terms.contains(term) {
                            search_terms.push(term.clone());
                        }
                    }
                }
            }
        }
    }

    // LLM term expansion
    if expand {
        println!("\n{} Expanding terms with LLM...", style("🤖").cyan());
        let extractor = LlmTermExtractor::new().max_terms(50);
        let context = ExtractionContext::for_domain(&domain)
            .with_description(&format!("Government documents from {}", domain));

        match extractor.extract_terms(&search_terms, &context).await {
            Ok(expanded) => {
                println!("  LLM expanded to {} terms", expanded.len());
                for term in expanded {
                    if !search_terms.contains(&term) {
                        search_terms.push(term);
                    }
                }
            }
            Err(e) => {
                println!("  {} LLM expansion failed: {}", style("!").yellow(), e);
            }
        }
    }

    println!("\n  Final terms: {} total", search_terms.len());

    // Run searches
    let engine_list: Vec<&str> = engines.split(',').map(|s| s.trim()).collect();
    let mut all_urls: Vec<DiscoveredUrl> = Vec::new();

    let config = DiscoverySourceConfig {
        max_results,
        ..Default::default()
    };

    for engine_name in engine_list {
        println!("\n{} Searching with {}...", style("→").cyan(), engine_name);

        match create_search_engine(engine_name) {
            Ok(engine) => match engine.discover(&domain, &search_terms, &config).await {
                Ok(urls) => {
                    println!("  Found {} URLs", urls.len());
                    all_urls.extend(urls);
                }
                Err(e) => {
                    println!("  {} Search failed: {}", style("!").yellow(), e);
                }
            },
            Err(e) => {
                println!("  {} {}", style("!").yellow(), e);
            }
        }
    }

    // Deduplicate
    all_urls.sort_by(|a, b| a.url.cmp(&b.url));
    all_urls.dedup_by(|a, b| a.url == b.url);

    println!(
        "\n{} Found {} unique URLs from search",
        style("📊").cyan(),
        all_urls.len()
    );

    // Add to queue
    let added = add_discovered_urls(settings, source_id, all_urls, dry_run).await?;

    if !dry_run {
        println!("{} Added {} URLs to crawl queue", style("✓").green(), added);
    }

    Ok(())
}
