use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0005_archive_history")
        .depends_on(&["0001_initial_schema"])
        // archive_snapshots
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE archive_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service TEXT NOT NULL,
    original_url TEXT NOT NULL,
    archive_url TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    http_status INTEGER,
    mimetype TEXT,
    content_length INTEGER,
    digest TEXT,
    metadata TEXT NOT NULL DEFAULT '{}'
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE archive_snapshots (
    id SERIAL PRIMARY KEY,
    service TEXT NOT NULL,
    original_url TEXT NOT NULL,
    archive_url TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    http_status INTEGER,
    mimetype TEXT,
    content_length BIGINT,
    digest TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'
)"#,
                ),
        )
        // archive_checks
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE archive_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_version_id INTEGER NOT NULL,
    archive_source TEXT NOT NULL,
    url_checked TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    snapshots_found INTEGER NOT NULL DEFAULT 0,
    matching_snapshots INTEGER NOT NULL DEFAULT 0,
    result TEXT NOT NULL,
    error_message TEXT,
    FOREIGN KEY (document_version_id) REFERENCES document_versions(id)
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE archive_checks (
    id SERIAL PRIMARY KEY,
    document_version_id INTEGER NOT NULL REFERENCES document_versions(id),
    archive_source TEXT NOT NULL,
    url_checked TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    snapshots_found INTEGER NOT NULL DEFAULT 0,
    matching_snapshots INTEGER NOT NULL DEFAULT 0,
    result TEXT NOT NULL,
    error_message TEXT
)"#,
                ),
        )
        // Add columns to document_versions
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"ALTER TABLE document_versions ADD COLUMN archive_snapshot_id INTEGER REFERENCES archive_snapshots(id);
ALTER TABLE document_versions ADD COLUMN earliest_archived_at TEXT"#,
                )
                .for_backend(
                    "postgres",
                    r#"ALTER TABLE document_versions ADD COLUMN archive_snapshot_id INTEGER REFERENCES archive_snapshots(id);
ALTER TABLE document_versions ADD COLUMN earliest_archived_at TIMESTAMPTZ"#,
                ),
        )
        // Indexes
        .operation(AddIndex::new(
            "archive_snapshots",
            Index::new("idx_archive_snapshots_service").column("service"),
        ))
        .operation(AddIndex::new(
            "archive_snapshots",
            Index::new("idx_archive_snapshots_original_url").column("original_url"),
        ))
        .operation(AddIndex::new(
            "archive_snapshots",
            Index::new("idx_archive_snapshots_captured_at").column("captured_at"),
        ))
        .operation(AddIndex::new(
            "archive_snapshots",
            Index::new("idx_archive_snapshots_service_url")
                .column("service")
                .column("original_url"),
        ))
        .operation(AddIndex::new(
            "archive_checks",
            Index::new("idx_archive_checks_version").column("document_version_id"),
        ))
        .operation(AddIndex::new(
            "archive_checks",
            Index::new("idx_archive_checks_source").column("archive_source"),
        ))
        .operation(AddIndex::new(
            "archive_checks",
            Index::new("idx_archive_checks_checked_at").column("checked_at"),
        ))
        .operation(AddIndex::new(
            "archive_checks",
            Index::new("idx_archive_checks_result").column("result"),
        ))
        .operation(AddIndex::new(
            "archive_checks",
            Index::new("idx_archive_checks_version_source")
                .column("document_version_id")
                .column("archive_source"),
        ))
        // Partial indexes
        .operation(AddIndex::new(
            "document_versions",
            Index::new("idx_document_versions_archive_snapshot")
                .column("archive_snapshot_id")
                .filter("archive_snapshot_id IS NOT NULL"),
        ))
        .operation(AddIndex::new(
            "document_versions",
            Index::new("idx_document_versions_earliest_archived")
                .column("earliest_archived_at")
                .filter("earliest_archived_at IS NOT NULL"),
        ))
}
