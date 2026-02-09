//! URL pattern discovery command.

use std::collections::{HashMap, HashSet};

use console::style;

use foiacquire::config::Settings;
use foiacquire::models::{CrawlUrl, DiscoveryMethod};

/// Analyze URL patterns and discover new URLs.
pub async fn cmd_discover_pattern(
    settings: &Settings,
    source_id: &str,
    limit: usize,
    dry_run: bool,
    min_examples: usize,
) -> anyhow::Result<()> {
    use regex::Regex;

    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();
    let crawl_repo = ctx.crawl();

    println!(
        "{} Analyzing URL patterns for source: {}",
        style("🔍").cyan(),
        style(source_id).bold()
    );

    // Get just the URLs for this source (lightweight query)
    let urls = doc_repo.get_urls_by_source(source_id).await?;
    if urls.is_empty() {
        println!(
            "{} No documents found for source {}",
            style("!").yellow(),
            source_id
        );
        return Ok(());
    }

    println!("  Found {} existing document URLs", urls.len());

    // === PHASE 1: Parent Directory Discovery ===
    // Extract unique parent directories from URLs that might have directory listings
    println!(
        "\n{} Phase 1: Analyzing parent directories...",
        style("📁").cyan()
    );

    let mut parent_dirs: HashSet<String> = HashSet::new();

    // Sample URLs if there are too many (parent dirs converge quickly)
    let sample_size = 10000.min(urls.len());
    let sample_urls: Vec<_> = if urls.len() > sample_size {
        println!(
            "  Sampling {} of {} URLs for directory analysis",
            sample_size,
            urls.len()
        );
        urls.iter()
            .step_by(urls.len() / sample_size)
            .take(sample_size)
            .collect()
    } else {
        urls.iter().collect()
    };

    for url in sample_urls {
        if let Ok(parsed) = url::Url::parse(url) {
            let mut path = parsed.path().to_string();

            // Remove the filename to get the directory
            if let Some(last_slash) = path.rfind('/') {
                path = path[..=last_slash].to_string();
            }

            // Walk up the directory tree
            while path.len() > 1 {
                // Reconstruct the full URL with this path
                let mut parent_url = parsed.clone();
                parent_url.set_path(&path);
                parent_url.set_query(None);
                parent_url.set_fragment(None);

                let parent_str = parent_url.to_string();

                // Don't add if it matches an existing document URL
                if !urls.contains(&parent_str) {
                    parent_dirs.insert(parent_str);
                }

                // Move up one level
                if path.ends_with('/') {
                    path = path[..path.len() - 1].to_string();
                }
                if let Some(last_slash) = path.rfind('/') {
                    path = path[..=last_slash].to_string();
                } else {
                    break;
                }
            }
        }
    }

    println!("  Found {} unique parent directories", parent_dirs.len());

    // === PHASE 2: Numeric Pattern Enumeration ===
    println!(
        "\n{} Phase 2: Analyzing numeric patterns...",
        style("🔢").cyan()
    );

    // Find patterns with numeric sequences
    // Pattern: look for numbers in URLs and try to find ranges
    let num_regex = Regex::new(r"\d+").unwrap();

    // Group URLs by their "template" (URL with numbers replaced by placeholder)
    let mut templates: HashMap<String, Vec<(String, Vec<u64>)>> = HashMap::new();

    for url in &urls {
        // Find all numeric sequences in the URL
        let nums: Vec<u64> = num_regex
            .find_iter(url)
            .filter_map(|m| m.as_str().parse().ok())
            .collect();

        if nums.is_empty() {
            continue;
        }

        // Create template by replacing all numbers with {N} for grouping
        let template = num_regex.replace_all(url, "{N}").to_string();
        templates
            .entry(template)
            .or_default()
            .push((url.to_string(), nums));
    }

    // Filter to templates with enough examples
    let viable_templates: Vec<_> = templates
        .iter()
        .filter(|(_, examples)| examples.len() >= min_examples)
        .collect();

    if viable_templates.is_empty() {
        println!(
            "{} No URL patterns found with at least {} examples",
            style("!").yellow(),
            min_examples
        );
        return Ok(());
    }

    println!(
        "\n{} Found {} URL pattern(s) with {} or more examples:",
        style("📊").cyan(),
        viable_templates.len(),
        min_examples
    );

    let mut total_candidates = 0;
    let mut new_urls: Vec<String> = Vec::new();

    // Get existing URLs to avoid duplicates
    let existing_urls: HashSet<String> = urls.iter().cloned().collect();
    let queued_urls: HashSet<String> = crawl_repo
        .get_pending_urls(source_id, 0)
        .await?
        .into_iter()
        .map(|u| u.url)
        .collect();

    for (template, examples) in viable_templates {
        println!("\n  Template: {}", style(template).dim());
        println!("  Examples: {} URLs", examples.len());

        // For each position in the template, find the range of numbers
        if examples.is_empty() {
            continue;
        }

        // Get the number of numeric positions from first example
        let num_positions = examples[0].1.len();
        if num_positions == 0 {
            continue;
        }

        // Focus on the last numeric position (most likely to be the document ID)
        let last_pos = num_positions - 1;
        let mut seen_nums: Vec<u64> = examples.iter().map(|(_, nums)| nums[last_pos]).collect();
        seen_nums.sort();
        seen_nums.dedup();

        if seen_nums.len() < 2 {
            continue;
        }

        let min_num = *seen_nums.first().unwrap();
        let max_num = *seen_nums.last().unwrap();
        let gaps: Vec<u64> = (min_num..=max_num)
            .filter(|n| !seen_nums.contains(n))
            .collect();

        println!(
            "  Last numeric position: {} - {} ({} gaps)",
            min_num,
            max_num,
            gaps.len()
        );

        // Generate candidate URLs for gaps
        let base_url = &examples[0].0;
        let base_nums = &examples[0].1;

        for gap_num in &gaps {
            // Reconstruct URL with the gap number
            let mut candidate = base_url.clone();
            let mut offset = 0i64;

            for (idx, m) in num_regex.find_iter(base_url).enumerate() {
                let replacement = if idx == last_pos {
                    gap_num.to_string()
                } else {
                    base_nums[idx].to_string()
                };

                let start = (m.start() as i64 + offset) as usize;
                let end = (m.end() as i64 + offset) as usize;
                let old_len = end - start;
                let new_len = replacement.len();

                candidate = format!(
                    "{}{}{}",
                    &candidate[..start],
                    replacement,
                    &candidate[end..]
                );
                offset += new_len as i64 - old_len as i64;
            }

            if !existing_urls.contains(&candidate) && !queued_urls.contains(&candidate) {
                new_urls.push(candidate);
                total_candidates += 1;

                if limit > 0 && total_candidates >= limit {
                    break;
                }
            }
        }

        // Also try extending beyond the range
        let extend_count = 10.min(max_num - min_num + 1);
        for i in 1..=extend_count {
            let extended_num = max_num + i;

            // Reconstruct URL with the extended number
            let mut candidate = base_url.clone();
            let mut offset = 0i64;

            for (idx, m) in num_regex.find_iter(base_url).enumerate() {
                let replacement = if idx == last_pos {
                    extended_num.to_string()
                } else {
                    base_nums[idx].to_string()
                };

                let start = (m.start() as i64 + offset) as usize;
                let end = (m.end() as i64 + offset) as usize;
                let old_len = end - start;
                let new_len = replacement.len();

                candidate = format!(
                    "{}{}{}",
                    &candidate[..start],
                    replacement,
                    &candidate[end..]
                );
                offset += new_len as i64 - old_len as i64;
            }

            if !existing_urls.contains(&candidate) && !queued_urls.contains(&candidate) {
                new_urls.push(candidate);
                total_candidates += 1;

                if limit > 0 && total_candidates >= limit {
                    break;
                }
            }
        }

        if limit > 0 && total_candidates >= limit {
            break;
        }
    }

    // Filter parent directories to exclude already queued ones
    let new_parent_dirs: Vec<String> = parent_dirs
        .into_iter()
        .filter(|u| !existing_urls.contains(u) && !queued_urls.contains(u))
        .collect();

    println!("\n{} Summary:", style("📊").cyan());
    println!("  {} parent directories to explore", new_parent_dirs.len());
    println!("  {} candidate URLs from patterns", new_urls.len());

    let total_new = new_parent_dirs.len() + new_urls.len();
    if total_new == 0 {
        println!(
            "\n{} No new URLs to discover (all already queued or fetched)",
            style("!").yellow()
        );
        return Ok(());
    }

    if dry_run {
        println!("\n{} Dry run - would add these URLs:", style("ℹ").blue());

        println!("\n  Parent directories (for directory listing discovery):");
        for url in new_parent_dirs.iter().take(10) {
            println!("    {}", url);
        }
        if new_parent_dirs.len() > 10 {
            println!(
                "    ... and {} more directories",
                new_parent_dirs.len() - 10
            );
        }

        println!("\n  Pattern-enumerated URLs:");
        for url in new_urls.iter().take(10) {
            println!("    {}", url);
        }
        if new_urls.len() > 10 {
            println!("    ... and {} more pattern URLs", new_urls.len() - 10);
        }
    } else {
        println!("\n{} Adding URLs to crawl queue...", style("📥").cyan());

        let mut added = 0;

        // Add parent directories (these will be crawled for links, not as documents)
        for url in &new_parent_dirs {
            let crawl_url = CrawlUrl::new(
                url.clone(),
                source_id.to_string(),
                DiscoveryMethod::PatternEnumeration, // Use same method for now
                None,
                0,
            );

            match crawl_repo.add_url(&crawl_url).await {
                Ok(true) => added += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!("Failed to add directory URL {}: {}", url, e),
            }
        }

        // Add pattern-enumerated URLs
        for url in &new_urls {
            let crawl_url = CrawlUrl::new(
                url.clone(),
                source_id.to_string(),
                DiscoveryMethod::PatternEnumeration,
                None,
                0,
            );

            match crawl_repo.add_url(&crawl_url).await {
                Ok(true) => added += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!("Failed to add URL {}: {}", url, e),
            }
        }

        println!("{} Added {} URLs to crawl queue", style("✓").green(), added);
        println!(
            "  Run {} to crawl discovered URLs",
            style(format!("foiacquire crawl {}", source_id)).cyan()
        );
        println!(
            "  Run {} to download discovered documents",
            style(format!("foiacquire download {}", source_id)).cyan()
        );
    }

    Ok(())
}
