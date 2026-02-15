//! Source management commands.

use console::style;

use foia::config::Settings;

use super::helpers::truncate;

/// List configured sources.
pub async fn cmd_source_list(settings: &Settings) -> anyhow::Result<()> {
    let repos = settings.repositories()?;
    let source_repo = repos.sources;
    let sources = source_repo.get_all().await?;

    if sources.is_empty() {
        println!(
            "{} No sources configured. Run 'foia init' first.",
            style("!").yellow()
        );
        return Ok(());
    }

    println!("\n{}", style("FOIA Sources").bold());
    println!("{}", "-".repeat(60));
    println!("{:<15} {:<25} {:<10} Last Scraped", "ID", "Name", "Type");
    println!("{}", "-".repeat(60));

    for source in sources {
        let last_scraped = source
            .last_scraped
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "Never".to_string());

        println!(
            "{:<15} {:<25} {:<10} {}",
            source.id,
            truncate(&source.name, 24),
            source.source_type.as_str(),
            last_scraped
        );
    }

    Ok(())
}

/// Rename a source (updates all associated documents).
pub async fn cmd_source_rename(
    settings: &Settings,
    old_id: &str,
    new_id: &str,
    confirm: bool,
) -> anyhow::Result<()> {
    use std::io::{self, Write};

    let repos = settings.repositories()?;
    let source_repo = repos.sources;
    let doc_repo = repos.documents;
    let crawl_repo = repos.crawl;

    // Check old source exists
    let old_source = source_repo.get(old_id).await?;
    if old_source.is_none() {
        println!("{} Source '{}' not found", style("✗").red(), old_id);
        return Ok(());
    }

    // Check new source doesn't exist
    if source_repo.get(new_id).await?.is_some() {
        println!(
            "{} Source '{}' already exists. Use a different name or delete it first.",
            style("✗").red(),
            new_id
        );
        return Ok(());
    }

    // Count affected documents
    let doc_count = doc_repo.count_by_source(old_id).await?;
    let crawl_count = crawl_repo.count_by_source(old_id).await?;

    println!(
        "\n{} Rename source '{}' → '{}'",
        style("→").cyan(),
        style(old_id).yellow(),
        style(new_id).green()
    );
    println!("  Documents to update: {}", doc_count);
    println!("  Crawl URLs to update: {}", crawl_count);

    // Confirm
    if !confirm {
        print!("\nProceed? [y/N] ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("{} Cancelled", style("!").yellow());
            return Ok(());
        }
    }

    // Perform the rename using the repository (handles both SQLite and PostgreSQL)
    let (docs_updated, crawls_updated) = source_repo.rename(old_id, new_id).await?;

    println!(
        "\n{} Renamed '{}' → '{}'",
        style("✓").green(),
        old_id,
        new_id
    );
    println!("  Documents updated: {}", docs_updated);
    println!("  Crawl URLs updated: {}", crawls_updated);

    Ok(())
}
