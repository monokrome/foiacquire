//! Configuration management commands.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use console::style;

use crate::config::{Config, Settings};
use crate::repository::{ConfigHistoryRepository, SourceRepository};
use crate::scrapers::ScraperConfig;

/// Recover a skeleton config from an existing database.
pub async fn cmd_config_recover(database: &Path, output: Option<&Path>) -> anyhow::Result<()> {
    // Validate database exists
    if !database.exists() {
        anyhow::bail!("Database not found: {}", database.display());
    }

    // Derive target directory from database path
    let target = database
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Could not determine parent directory of database"))?;

    let database_filename = database
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("Could not determine database filename"))?;

    // Open database and query sources
    let source_repo = SourceRepository::new(database)?;
    let sources = source_repo.get_all()?;

    if sources.is_empty() {
        eprintln!(
            "{} No sources found in database. Generating minimal config.",
            style("!").yellow()
        );
    } else {
        eprintln!(
            "{} Found {} source(s) in database",
            style("✓").green(),
            sources.len()
        );
    }

    // Build scraper configs from sources
    let mut scrapers: HashMap<String, ScraperConfig> = HashMap::new();
    for source in &sources {
        let scraper_config = ScraperConfig {
            name: Some(source.name.clone()),
            base_url: Some(source.base_url.clone()),
            ..Default::default()
        };
        scrapers.insert(source.id.clone(), scraper_config);

        eprintln!("  {} {} ({})", style("→").dim(), source.id, source.base_url);
    }

    // Build the config
    let config = Config {
        target: Some(target.display().to_string()),
        database: Some(database_filename.to_string()),
        scrapers,
        ..Default::default()
    };

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&config)?;

    // Output - JSON to stdout (or file), status messages to stderr
    match output {
        Some(path) => {
            let mut file = std::fs::File::create(path)?;
            file.write_all(json.as_bytes())?;
            file.write_all(b"\n")?;
            eprintln!(
                "\n{} Config written to {}",
                style("✓").green(),
                path.display()
            );
        }
        None => {
            println!("{}", json);
        }
    }

    if !sources.is_empty() {
        eprintln!();
        eprintln!(
            "{} This is a skeleton config. Discovery/fetch rules must be added manually.",
            style("Note:").yellow().bold()
        );
        eprintln!("  See {} for examples.", style("etc/example.json").cyan());
    }

    Ok(())
}

/// Restore the most recent config from database history.
pub async fn cmd_config_restore(settings: &Settings, output: Option<&Path>) -> anyhow::Result<()> {
    let db_path = settings.database_path();
    if !db_path.exists() {
        anyhow::bail!("Database not found: {}", db_path.display());
    }

    let repo = ConfigHistoryRepository::new(&db_path)?;
    let entry = repo
        .get_latest()?
        .ok_or_else(|| anyhow::anyhow!("No configuration history found in database"))?;

    // Determine output path
    let output_path = output
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| settings.data_dir.join("foiacquire.json"));

    // Write the config
    let mut file = std::fs::File::create(&output_path)?;
    file.write_all(entry.data.as_bytes())?;
    file.write_all(b"\n")?;

    eprintln!(
        "{} Config restored to {}",
        style("✓").green(),
        output_path.display()
    );
    eprintln!(
        "  Format: {}, Created: {}",
        entry.format,
        entry.created_at.format("%Y-%m-%d %H:%M:%S UTC")
    );

    Ok(())
}

/// List configuration history entries.
pub async fn cmd_config_history(settings: &Settings, full: bool) -> anyhow::Result<()> {
    let db_path = settings.database_path();
    if !db_path.exists() {
        anyhow::bail!("Database not found: {}", db_path.display());
    }

    let repo = ConfigHistoryRepository::new(&db_path)?;
    let entries = repo.get_all()?;

    if entries.is_empty() {
        println!("No configuration history found.");
        return Ok(());
    }

    println!(
        "{} configuration history entries:\n",
        style(entries.len()).cyan()
    );

    for (i, entry) in entries.iter().enumerate() {
        let marker = if i == 0 { "(latest)" } else { "" };
        println!(
            "{} {} {} {}",
            style(&entry.uuid[..8]).dim(),
            style(entry.created_at.format("%Y-%m-%d %H:%M:%S")).cyan(),
            style(&entry.format).yellow(),
            style(marker).green()
        );

        if full {
            println!("{}\n", entry.data);
        }
    }

    if !full {
        eprintln!(
            "\n{} Use --full to see complete config data",
            style("Tip:").dim()
        );
    }

    Ok(())
}
