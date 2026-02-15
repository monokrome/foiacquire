use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0002_service_status")
        .depends_on(&["0001_initial_schema"])
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    // SQLite schema after fix_schema_drift
                    r#"CREATE TABLE service_status (
    id TEXT PRIMARY KEY NOT NULL,
    service_type TEXT NOT NULL,
    source_id TEXT,
    status TEXT NOT NULL DEFAULT 'starting',
    last_heartbeat TEXT NOT NULL,
    last_activity TEXT,
    current_task TEXT,
    stats TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL,
    host TEXT,
    version TEXT,
    last_error TEXT,
    last_error_at TEXT,
    error_count INTEGER NOT NULL DEFAULT 0
)"#,
                )
                .for_backend(
                    "postgres",
                    // Postgres original schema
                    r#"CREATE TABLE IF NOT EXISTS service_status (
    id SERIAL PRIMARY KEY,
    service_type TEXT NOT NULL,
    hostname TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'starting',
    current_source TEXT,
    started_at TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    stats TEXT,
    last_error TEXT,
    error_count INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT service_status_type_host UNIQUE (service_type, hostname)
)"#,
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "CREATE INDEX idx_service_status_type ON service_status(service_type)",
                )
                .for_backend(
                    "postgres",
                    "CREATE INDEX idx_service_status_type ON service_status(service_type)",
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "CREATE INDEX idx_service_status_heartbeat ON service_status(last_heartbeat)",
                )
                .for_backend(
                    "postgres",
                    "CREATE INDEX idx_service_status_heartbeat ON service_status(last_heartbeat)",
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "CREATE INDEX idx_service_status_source ON service_status(source_id) WHERE source_id IS NOT NULL",
                )
                .for_backend(
                    "postgres",
                    "CREATE INDEX idx_service_status_source ON service_status(current_source) WHERE current_source IS NOT NULL",
                ),
        )
}
