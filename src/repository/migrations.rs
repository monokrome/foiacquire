//! Database migrations using diesel_migrations.
//!
//! Embeds migrations at compile time and runs them via blocking tasks
//! to work with async connections.

use diesel::Connection;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use tracing::info;

use super::pool::DieselError;

// Embed SQLite migrations (uses diesel_migrations harness)
pub const SQLITE_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/sqlite");

/// Run pending migrations for a database URL.
///
/// Creates a sync connection and runs migrations in a blocking task.
pub async fn run_migrations(database_url: &str) -> Result<(), DieselError> {
    let url = database_url.to_string();

    if super::util::is_postgres_url(&url) {
        #[cfg(feature = "postgres")]
        {
            run_postgres_migrations_async(&url).await
        }
        #[cfg(not(feature = "postgres"))]
        {
            Err(DieselError::QueryBuilderError(
                "PostgreSQL support not compiled. Use --features postgres".into(),
            ))
        }
    } else {
        run_sqlite_migrations_async(&url).await
    }
}

/// Run SQLite migrations asynchronously.
async fn run_sqlite_migrations_async(database_url: &str) -> Result<(), DieselError> {
    // Strip sqlite: prefix if present - diesel expects just the file path
    let url = database_url
        .strip_prefix("sqlite:")
        .unwrap_or(database_url)
        .to_string();

    tokio::task::spawn_blocking(move || {
        let mut conn = diesel::SqliteConnection::establish(&url).map_err(|e| {
            DieselError::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                Box::new(e.to_string()),
            )
        })?;

        let migrations = conn
            .run_pending_migrations(SQLITE_MIGRATIONS)
            .map_err(DieselError::QueryBuilderError)?;

        for migration in &migrations {
            info!("Applied migration: {}", migration);
        }

        if migrations.is_empty() {
            info!("No pending migrations");
        }

        Ok(())
    })
    .await
    .map_err(|e| DieselError::QueryBuilderError(Box::new(e)))?
}

/// PostgreSQL migration definitions (embedded at compile time).
#[cfg(feature = "postgres")]
static POSTGRES_MIGRATION_FILES: &[(&str, &str)] = &[
    (
        "00000000000000",
        include_str!("../../migrations/postgres/00000000000000_diesel_initial_setup/up.sql"),
    ),
    (
        "2024-12-26-000000",
        include_str!("../../migrations/postgres/2024-12-26-000000_initial_schema/up.sql"),
    ),
    (
        "2024-12-30-000000",
        include_str!("../../migrations/postgres/2024-12-30-000000_service_status/up.sql"),
    ),
    (
        "2024-12-30-100000",
        include_str!("../../migrations/postgres/2024-12-30-100000_analysis_results/up.sql"),
    ),
    (
        "2025-01-01-000000",
        include_str!(
            "../../migrations/postgres/2025-01-01-000000_add_missing_unique_constraints/up.sql"
        ),
    ),
    (
        "2025-01-01-200000",
        include_str!("../../migrations/postgres/2025-01-01-200000_archive_history/up.sql"),
    ),
];

/// Run PostgreSQL migrations asynchronously.
///
/// Uses tokio-postgres directly since we don't have diesel/postgres (requires libpq).
#[cfg(feature = "postgres")]
async fn run_postgres_migrations_async(database_url: &str) -> Result<(), DieselError> {
    use tokio_postgres::NoTls;

    // Parse the URL and connect
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .map_err(|e| {
            DieselError::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                Box::new(e.to_string()),
            )
        })?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("PostgreSQL connection error: {}", e);
        }
    });

    // Create migrations table if it doesn't exist
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS __diesel_schema_migrations (
                version VARCHAR(50) PRIMARY KEY NOT NULL,
                run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
            &[],
        )
        .await
        .map_err(|e| DieselError::QueryBuilderError(Box::new(e)))?;

    // Get already-run migrations
    let rows = client
        .query("SELECT version FROM __diesel_schema_migrations", &[])
        .await
        .map_err(|e| DieselError::QueryBuilderError(Box::new(e)))?;

    let applied: std::collections::HashSet<String> =
        rows.iter().map(|row| row.get::<_, String>(0)).collect();

    // Run pending migrations
    let mut ran_count = 0;
    for (version, sql) in POSTGRES_MIGRATION_FILES {
        if applied.contains(*version) {
            continue;
        }

        info!("Applying migration: {}", version);

        // Split by semicolons and execute each statement
        // Handle $$ delimited function bodies specially
        let statements = split_sql_statements(sql);
        for stmt in statements {
            let stmt = stmt.trim();
            // Skip empty statements and comment-only statements
            let is_comment_only = stmt.lines().all(|line| {
                let line = line.trim();
                line.is_empty() || line.starts_with("--")
            });
            if !stmt.is_empty() && !is_comment_only {
                client.execute(stmt, &[]).await.map_err(|e| {
                    tracing::error!("Migration {} failed on:\n{}\nError: {}", version, stmt, e);
                    DieselError::QueryBuilderError(Box::new(e))
                })?;
            }
        }

        // Record migration
        client
            .execute(
                "INSERT INTO __diesel_schema_migrations (version) VALUES ($1)",
                &[version],
            )
            .await
            .map_err(|e| DieselError::QueryBuilderError(Box::new(e)))?;

        ran_count += 1;
    }

    if ran_count == 0 {
        info!("No pending migrations");
    } else {
        info!("Applied {} migration(s)", ran_count);
    }

    Ok(())
}

/// Split SQL statements, handling $$ delimited function bodies.
#[cfg(feature = "postgres")]
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut current_start = 0;
    let mut in_dollar_quote = false;

    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Check for $$ delimiter
        if i + 1 < bytes.len() && bytes[i] == b'$' && bytes[i + 1] == b'$' {
            in_dollar_quote = !in_dollar_quote;
            i += 2;
            continue;
        }

        // Check for semicolon outside of $$ blocks
        if bytes[i] == b';' && !in_dollar_quote {
            let stmt = &sql[current_start..i];
            if !stmt.trim().is_empty() {
                statements.push(stmt);
            }
            current_start = i + 1;
        }

        i += 1;
    }

    // Don't forget the last statement
    if current_start < sql.len() {
        let stmt = &sql[current_start..];
        if !stmt.trim().is_empty() {
            statements.push(stmt);
        }
    }

    statements
}
