//! Database migrations using cetane.
//!
//! Runs migrations via blocking tasks to work with async connections.

use cetane::migrator::MigrationStateStore;
use tracing::info;

use super::pool::DieselError;

/// Run pending migrations for a database URL.
pub async fn run_migrations(database_url: &str, no_tls: bool) -> Result<(), DieselError> {
    let url = database_url.to_string();

    if super::util::is_postgres_url(&url) {
        #[cfg(feature = "postgres")]
        {
            run_postgres_migrations_async(&url, no_tls).await
        }
        #[cfg(not(feature = "postgres"))]
        {
            let _ = no_tls;
            Err(DieselError::QueryBuilderError(
                "PostgreSQL support not compiled. Use --features postgres".into(),
            ))
        }
    } else {
        let _ = no_tls;
        run_sqlite_migrations_async(&url).await
    }
}

fn migration_error(msg: impl std::fmt::Display) -> DieselError {
    DieselError::QueryBuilderError(msg.to_string().into())
}

/// Run SQLite migrations asynchronously.
async fn run_sqlite_migrations_async(database_url: &str) -> Result<(), DieselError> {
    use cetane::backend::Sqlite;
    use cetane::migrator::Migrator;

    let url = database_url
        .strip_prefix("sqlite:")
        .unwrap_or(database_url)
        .to_string();

    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open(&url).map_err(migration_error)?;
        let backend = Sqlite;
        let registry = crate::migrations::registry();

        let mut state = SqliteState::new(&conn)?;

        // One-time transition: if this database was previously managed by Diesel
        // and cetane hasn't run yet, mark existing migrations as applied.
        // Only do this when the cetane table is empty (first transition).
        let already_applied = state.applied_migrations().map_err(migration_error)?;
        if already_applied.is_empty() && state.has_diesel_migrations(&conn)? {
            mark_existing_as_applied(&registry, &mut state)?;
        }

        let mut migrator = Migrator::new(&registry, &backend, state);
        let applied = migrator
            .migrate_forward(|sql| conn.execute_batch(sql).map_err(|e| e.to_string()))
            .map_err(migration_error)?;

        for name in &applied {
            info!("Applied migration: {}", name);
        }

        if applied.is_empty() {
            info!("No pending migrations");
        }

        Ok(())
    })
    .await
    .map_err(|e| DieselError::QueryBuilderError(Box::new(e)))?
}

/// Run PostgreSQL migrations asynchronously.
#[cfg(feature = "postgres")]
async fn run_postgres_migrations_async(
    database_url: &str,
    no_tls: bool,
) -> Result<(), DieselError> {
    use cetane::backend::Postgres;
    use cetane::migrator::Migrator;

    let client = super::pg_tls::connect_raw(database_url, no_tls)
        .await
        .map_err(migration_error)?;

    let backend = Postgres;
    let registry = crate::migrations::registry();

    let mut state = PostgresState::new(&client).await?;

    // One-time transition: only auto-mark if cetane has no entries yet
    let already_applied = state.applied_migrations().map_err(migration_error)?;
    if already_applied.is_empty() && state.has_diesel_migrations(&client).await? {
        mark_existing_as_applied(&registry, &mut state)?;
    }

    let mut migrator = Migrator::new(&registry, &backend, state);
    let applied = migrator
        .migrate_forward(|sql| {
            // Block on async execution for the sync migrator interface
            let rt = tokio::runtime::Handle::current();
            std::thread::scope(|s| {
                s.spawn(|| {
                    rt.block_on(async {
                        client.execute(sql, &[]).await.map_err(|e| e.to_string())?;
                        Ok::<(), String>(())
                    })
                })
                .join()
                .map_err(|_| "thread panicked".to_string())?
            })
        })
        .map_err(migration_error)?;

    for name in &applied {
        info!("Applied migration: {}", name);
    }

    if applied.is_empty() {
        info!("No pending migrations");
    }

    Ok(())
}

/// Mark all migrations as applied for an existing database.
fn mark_existing_as_applied<S: cetane::migrator::MigrationStateStore>(
    registry: &cetane::migration::MigrationRegistry,
    state: &mut S,
) -> Result<(), DieselError> {
    let order = registry.resolve_order().map_err(migration_error)?;
    let applied = state.applied_migrations().map_err(migration_error)?;

    for name in order {
        if !applied.contains(&name.to_string()) {
            info!("Marking existing migration as applied: {}", name);
            state.mark_applied(name).map_err(migration_error)?;
        }
    }

    Ok(())
}

// -- SQLite state store --

struct SqliteState<'a> {
    conn: &'a rusqlite::Connection,
}

impl<'a> SqliteState<'a> {
    fn new(conn: &'a rusqlite::Connection) -> Result<Self, DieselError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS __cetane_migrations (
                name TEXT PRIMARY KEY NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
        )
        .map_err(migration_error)?;

        Ok(Self { conn })
    }

    fn has_diesel_migrations(&self, conn: &rusqlite::Connection) -> Result<bool, DieselError> {
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='__diesel_schema_migrations'",
                [],
                |row| row.get(0),
            )
            .map_err(migration_error)?;

        Ok(exists)
    }
}

impl cetane::migrator::MigrationStateStore for SqliteState<'_> {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM __cetane_migrations ORDER BY name")
            .map_err(|e| e.to_string())?;

        let names = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<String>, _>>()
            .map_err(|e| e.to_string())?;

        Ok(names)
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO __cetane_migrations (name) VALUES (?1)",
                [name],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        self.conn
            .execute("DELETE FROM __cetane_migrations WHERE name = ?1", [name])
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

// -- PostgreSQL state store --

#[cfg(feature = "postgres")]
struct PostgresState<'a> {
    client: &'a tokio_postgres::Client,
    applied: Vec<String>,
}

#[cfg(feature = "postgres")]
impl<'a> PostgresState<'a> {
    async fn new(client: &'a tokio_postgres::Client) -> Result<Self, DieselError> {
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS __cetane_migrations (
                    name TEXT PRIMARY KEY NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )",
                &[],
            )
            .await
            .map_err(migration_error)?;

        let rows = client
            .query("SELECT name FROM __cetane_migrations ORDER BY name", &[])
            .await
            .map_err(migration_error)?;

        let applied = rows.iter().map(|r| r.get::<_, String>(0)).collect();

        Ok(Self { client, applied })
    }

    async fn has_diesel_migrations(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<bool, DieselError> {
        let row = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '__diesel_schema_migrations')",
                &[],
            )
            .await
            .map_err(migration_error)?;

        Ok(row.get(0))
    }
}

#[cfg(feature = "postgres")]
impl cetane::migrator::MigrationStateStore for PostgresState<'_> {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        Ok(self.applied.clone())
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        let rt = tokio::runtime::Handle::current();
        std::thread::scope(|s| {
            s.spawn(|| {
                rt.block_on(async {
                    self.client
                        .execute(
                            "INSERT INTO __cetane_migrations (name) VALUES ($1) ON CONFLICT DO NOTHING",
                            &[&name],
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<(), String>(())
                })
            })
            .join()
            .map_err(|_| "thread panicked".to_string())?
        })?;

        if !self.applied.contains(&name.to_string()) {
            self.applied.push(name.to_string());
        }
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        let rt = tokio::runtime::Handle::current();
        std::thread::scope(|s| {
            s.spawn(|| {
                rt.block_on(async {
                    self.client
                        .execute("DELETE FROM __cetane_migrations WHERE name = $1", &[&name])
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<(), String>(())
                })
            })
            .join()
            .map_err(|_| "thread panicked".to_string())?
        })?;

        self.applied.retain(|n| n != name);
        Ok(())
    }
}
