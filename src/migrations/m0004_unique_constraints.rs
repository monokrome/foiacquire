use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0004_unique_constraints")
        .depends_on(&["0001_initial_schema"])
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_document_pages_unique ON document_pages(document_id, version_id, page_number)",
                )
                .for_backend(
                    "postgres",
                    r#"DROP INDEX IF EXISTS idx_pages_doc_version_page;
CREATE UNIQUE INDEX IF NOT EXISTS idx_document_pages_unique ON document_pages(document_id, version_id, page_number)"#,
                ),
        )
}
