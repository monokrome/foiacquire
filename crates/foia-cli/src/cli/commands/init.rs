//! Initialize command.

use console::style;

use foia::config::{Config, Settings};
use foia::models::{Source, SourceType};
use foia::repository::migrations;

/// Initialize the data directory and database.
pub async fn cmd_init(settings: &Settings) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Run database migrations
    println!("{} Running migrations...", style("→").cyan());
    migrations::run_migrations(&settings.database_url(), settings.no_tls).await?;

    let repos = settings.repositories()?;
    let source_repo = repos.sources;

    // Load sources from config file and transfer to database
    let config = Config::load().await;

    let mut sources_added = 0;
    for (source_id, scraper_config) in &config.scrapers {
        // Register source in sources table
        if !source_repo.exists(source_id).await? {
            let source = Source::new(
                source_id.clone(),
                SourceType::Custom,
                scraper_config.name_or(source_id),
                scraper_config.base_url_or(""),
            );
            source_repo.save(&source).await?;
            sources_added += 1;
            println!("  {} Added source: {}", style("✓").green(), source.name);
        }

        // Store scraper config in scraper_configs table
        repos
            .scraper_configs
            .upsert(source_id, scraper_config)
            .await?;
    }

    if sources_added == 0 && config.scrapers.is_empty() {
        println!(
            "{} No scrapers configured in foia.json",
            style("!").yellow()
        );
        println!("  Copy foia.example.json to foia.json to get started");
    }

    println!(
        "{} Initialized foia in {}",
        style("✓").green(),
        settings.data_dir.display()
    );

    Ok(())
}
