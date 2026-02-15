//! Database migration command.

use console::style;

use foia::config::{Settings, SourcesConfig};
use foia::repository::migrations;
use foia::repository::util::redact_url_password;
use foia::repository::Repositories;

/// Expected schema version (should match storage_meta.format_version).
const EXPECTED_SCHEMA_VERSION: &str = "15";

/// Run database migrations.
pub async fn cmd_migrate(settings: &Settings, check: bool, force: bool) -> anyhow::Result<()> {
    println!("{} Database migration", style("→").cyan());
    println!(
        "  Database: {}",
        redact_url_password(&settings.database_url())
    );

    let repos = settings.repositories()?;

    // Check current schema version
    let current_version = repos.schema_version().await.ok().flatten();

    match &current_version {
        Some(v) => println!("  Current schema version: {}", v),
        None => println!(
            "  Current schema version: {} (not initialized)",
            style("none").yellow()
        ),
    }
    println!("  Expected schema version: {}", EXPECTED_SCHEMA_VERSION);

    let needs_migration = current_version.as_deref() != Some(EXPECTED_SCHEMA_VERSION);
    let schema_exists = current_version.is_some();

    if check {
        // Just report status
        if needs_migration {
            if schema_exists {
                println!(
                    "\n{} Schema version mismatch. Run 'foia db migrate' to update.",
                    style("!").yellow()
                );
            } else {
                println!(
                    "\n{} Database not initialized. Run 'foia db migrate' to initialize.",
                    style("!").yellow()
                );
            }
        } else {
            println!("\n{} Schema is up to date.", style("✓").green());
        }
        return Ok(());
    }

    // Run migrations
    if !needs_migration && !force {
        println!(
            "\n{} Schema is already up to date. Use --force to re-run.",
            style("✓").green()
        );
        return Ok(());
    }

    if force && !needs_migration {
        println!("\n{} Forcing migration re-run...", style("!").yellow());
    }

    println!("\n{} Running migrations...", style("→").cyan());
    match migrations::run_migrations(&settings.database_url(), settings.no_tls).await {
        Ok(()) => {
            println!("{} Migration complete!", style("✓").green());
        }
        Err(e) => {
            eprintln!("{} Migration failed: {}", style("✗").red(), e);
            return Err(anyhow::anyhow!("Migration failed: {}", e));
        }
    }

    // Verify new version
    if let Ok(Some(new_version)) = repos.schema_version().await {
        println!("  Schema version is now: {}", new_version);
    }

    // Post-migration: seed scraper_configs from configuration_history
    migrate_config_history_to_scraper_configs(&repos).await;

    Ok(())
}

/// Migrate data from configuration_history into scraper_configs.
///
/// If scraper_configs is empty and configuration_history has data,
/// extract each entry from the scrapers HashMap and insert into
/// scraper_configs. Merges global-level fields (user_agent, request_timeout,
/// request_delay_ms, via, via_mode) into each source's ScraperConfig as
/// fallback values.
async fn migrate_config_history_to_scraper_configs(repos: &Repositories) {
    // Only migrate if scraper_configs is empty
    let is_empty = match repos.scraper_configs.is_empty().await {
        Ok(empty) => empty,
        Err(e) => {
            tracing::warn!("Failed to check scraper_configs: {}", e);
            return;
        }
    };
    if !is_empty {
        return;
    }

    // Load all config history entries (newest first) and find the first
    // one that actually contains scraper configurations. Many entries may
    // be empty `{}` blobs that parse as SourcesConfig with no scrapers.
    let entries = match repos.config_history.get_all().await {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("Failed to read configuration_history: {}", e);
            return;
        }
    };

    let mut scrapers = std::collections::HashMap::new();
    let mut global_user_agent = None;
    let mut global_timeout = None;
    let mut global_delay = None;
    let mut global_via = std::collections::HashMap::new();
    let mut global_via_mode = foia::config::ViaMode::default();

    for entry in &entries {
        if let Ok(sc) = serde_json::from_str::<SourcesConfig>(&entry.data) {
            if !sc.scrapers.is_empty() {
                scrapers = sc.scrapers;
                global_user_agent = sc.user_agent;
                global_timeout = sc.request_timeout;
                global_delay = sc.request_delay_ms;
                global_via = sc.via;
                global_via_mode = sc.via_mode;
                break;
            }
        }
    }

    if scrapers.is_empty() {
        return;
    }

    let mut migrated = 0usize;
    for (source_id, mut config) in scrapers {
        // Merge global fields as fallbacks into per-source config
        if config.user_agent.is_none() {
            config.user_agent = global_user_agent.clone();
        }
        if config.request_timeout.is_none() {
            config.request_timeout = global_timeout;
        }
        if config.request_delay_ms.is_none() {
            config.request_delay_ms = global_delay;
        }
        if config.via.is_empty() && !global_via.is_empty() {
            config.via = global_via.clone();
        }
        if config.via_mode.is_none() {
            let default_via_mode = foia::config::ViaMode::default();
            if global_via_mode != default_via_mode {
                config.via_mode = Some(global_via_mode);
            }
        }

        match repos.scraper_configs.upsert(&source_id, &config).await {
            Ok(()) => migrated += 1,
            Err(e) => {
                tracing::warn!(
                    "Failed to migrate scraper config for '{}': {}",
                    source_id,
                    e
                );
            }
        }
    }

    if migrated > 0 {
        println!(
            "{} Migrated {} scraper configs from configuration_history",
            style("→").cyan(),
            migrated
        );
    }
}
