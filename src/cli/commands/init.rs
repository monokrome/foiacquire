//! Initialize command.

use console::style;

use crate::config::{Config, Settings};
use crate::models::{Source, SourceType};
use crate::repository::{CrawlRepository, DocumentRepository, SourceRepository};

/// Initialize the data directory and database.
pub async fn cmd_init(settings: &Settings) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Initialize repositories
    let db_path = settings.database_path();
    let _doc_repo = DocumentRepository::new(&db_path, &settings.documents_dir)?;
    let source_repo = SourceRepository::new(&db_path)?;
    let _crawl_repo = CrawlRepository::new(&db_path)?;

    // Load sources from config
    let config = Config::load().await;

    let mut sources_added = 0;
    for (source_id, scraper_config) in &config.scrapers {
        if !source_repo.exists(source_id)? {
            let source = Source::new(
                source_id.clone(),
                SourceType::Custom,
                scraper_config.name_or(source_id),
                scraper_config.base_url_or(""),
            );
            source_repo.save(&source)?;
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
