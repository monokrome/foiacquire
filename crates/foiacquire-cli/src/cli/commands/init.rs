//! Initialize command.

use console::style;

use foiacquire::config::{Config, Settings};
use foiacquire::models::{Source, SourceType};
use foiacquire::repository::migrations;

/// Initialize the data directory and database.
pub async fn cmd_init(settings: &Settings) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Run database migrations
    println!("{} Running migrations...", style("→").cyan());
    migrations::run_migrations(&settings.database_url(), settings.no_tls).await?;

    // Create database context for source management
    let ctx = settings.create_db_context()?;
    let source_repo = ctx.sources();

    // Load sources from config
    let config = Config::load().await;

    let mut sources_added = 0;
    for (source_id, scraper_config) in &config.scrapers {
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
    }

    if sources_added == 0 && config.scrapers.is_empty() {
        println!(
            "{} No scrapers configured in foiacquire.json",
            style("!").yellow()
        );
        println!("  Copy foiacquire.example.json to foiacquire.json to get started");
    }

    println!(
        "{} Initialized FOIAcquire in {}",
        style("✓").green(),
        settings.data_dir.display()
    );

    Ok(())
}
