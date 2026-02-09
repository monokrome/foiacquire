//! Database migration command.

use console::style;

use foiacquire::config::Settings;
use foiacquire::repository::migrations;
use foiacquire::repository::util::redact_url_password;

/// Expected schema version (should match storage_meta.format_version).
const EXPECTED_SCHEMA_VERSION: &str = "13";

/// Run database migrations.
pub async fn cmd_migrate(settings: &Settings, check: bool, force: bool) -> anyhow::Result<()> {
    println!("{} Database migration", style("→").cyan());
    println!(
        "  Database: {}",
        redact_url_password(&settings.database_url())
    );

    let ctx = settings.create_db_context()?;

    // Check current schema version
    let current_version = ctx.get_schema_version().await.ok().flatten();

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
                    "\n{} Schema version mismatch. Run 'foiacquire db migrate' to update.",
                    style("!").yellow()
                );
            } else {
                println!(
                    "\n{} Database not initialized. Run 'foiacquire db migrate' to initialize.",
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
    if let Ok(Some(new_version)) = ctx.get_schema_version().await {
        println!("  Schema version is now: {}", new_version);
    }

    Ok(())
}
