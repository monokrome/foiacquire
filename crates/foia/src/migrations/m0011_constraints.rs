use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0011_constraints")
        .depends_on(&["0010_deterministic_paths"])
        .operation(AddIndex::new(
            "document_versions",
            Index::new("idx_versions_content_hash_dedup")
                .column("content_hash")
                .column("dedup_index")
                .unique()
                .filter("dedup_index IS NOT NULL"),
        ))
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "CREATE TRIGGER IF NOT EXISTS tr_archive_checks_cascade_delete \
                     AFTER DELETE ON document_versions \
                     BEGIN \
                         DELETE FROM archive_checks WHERE document_version_id = OLD.id; \
                     END",
                )
                .for_backend(
                    "postgres",
                    "ALTER TABLE archive_checks \
                     DROP CONSTRAINT IF EXISTS archive_checks_document_version_id_fkey, \
                     ADD CONSTRAINT archive_checks_document_version_id_fkey \
                     FOREIGN KEY (document_version_id) REFERENCES document_versions(id) ON DELETE CASCADE",
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('format_version', '14')",
                )
                .for_backend(
                    "postgres",
                    "INSERT INTO storage_meta (key, value) VALUES ('format_version', '14') \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                ),
        )
}
