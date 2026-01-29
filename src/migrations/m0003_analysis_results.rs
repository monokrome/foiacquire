use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0003_analysis_results")
        .depends_on(&["0001_initial_schema"])
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS document_analysis_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id INTEGER,
    document_id TEXT NOT NULL,
    version_id INTEGER NOT NULL,
    analysis_type TEXT NOT NULL,
    backend TEXT NOT NULL,
    result_text TEXT,
    confidence REAL,
    processing_time_ms INTEGER,
    error TEXT,
    status TEXT NOT NULL DEFAULT 'complete',
    created_at TEXT NOT NULL,
    metadata TEXT,
    FOREIGN KEY (page_id) REFERENCES document_pages(id),
    FOREIGN KEY (document_id) REFERENCES documents(id),
    FOREIGN KEY (version_id) REFERENCES document_versions(id)
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS document_analysis_results (
    id SERIAL PRIMARY KEY,
    page_id INTEGER REFERENCES document_pages(id),
    document_id TEXT NOT NULL REFERENCES documents(id),
    version_id INTEGER NOT NULL,
    analysis_type TEXT NOT NULL,
    backend TEXT NOT NULL,
    result_text TEXT,
    confidence REAL,
    processing_time_ms INTEGER,
    error TEXT,
    status TEXT NOT NULL DEFAULT 'complete',
    created_at TEXT NOT NULL,
    metadata TEXT
)"#,
                ),
        )
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_document").column("document_id"),
        ))
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_page").column("page_id"),
        ))
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_type").column("analysis_type"),
        ))
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_status").column("status"),
        ))
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_type_backend")
                .column("analysis_type")
                .column("backend"),
        ))
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_page_unique")
                .column("page_id")
                .column("analysis_type")
                .column("backend")
                .unique()
                .filter("page_id IS NOT NULL"),
        ))
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_doc_unique")
                .column("document_id")
                .column("version_id")
                .column("analysis_type")
                .column("backend")
                .unique()
                .filter("page_id IS NULL"),
        ))
        // Migrate existing OCR data
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"INSERT INTO document_analysis_results (
    page_id, document_id, version_id, analysis_type, backend,
    result_text, confidence, processing_time_ms, status, created_at
)
SELECT
    por.page_id,
    dp.document_id,
    dp.version_id,
    'ocr' AS analysis_type,
    por.backend,
    por.ocr_text,
    por.confidence,
    por.processing_time_ms,
    CASE WHEN por.ocr_text IS NOT NULL THEN 'complete' ELSE 'failed' END AS status,
    por.created_at
FROM page_ocr_results por
JOIN document_pages dp ON dp.id = por.page_id"#,
                )
                .for_backend(
                    "postgres",
                    r#"DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'page_ocr_results') THEN
        INSERT INTO document_analysis_results (
            page_id, document_id, version_id, analysis_type, backend,
            result_text, confidence, processing_time_ms, status, created_at
        )
        SELECT
            por.page_id,
            dp.document_id,
            dp.version_id,
            'ocr' AS analysis_type,
            por.backend,
            por.ocr_text,
            por.confidence,
            por.processing_time_ms,
            CASE WHEN por.ocr_text IS NOT NULL THEN 'complete' ELSE 'failed' END AS status,
            por.created_at
        FROM page_ocr_results por
        JOIN document_pages dp ON dp.id = por.page_id;
    END IF;
END $$"#,
                ),
        )
        // Drop old OCR table
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"DROP INDEX IF EXISTS idx_page_ocr_results_page;
DROP INDEX IF EXISTS idx_page_ocr_results_backend;
DROP TABLE IF EXISTS page_ocr_results"#,
                )
                .for_backend(
                    "postgres",
                    r#"DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'page_ocr_results') THEN
        DROP INDEX IF EXISTS idx_page_ocr_results_page;
        DROP INDEX IF EXISTS idx_page_ocr_results_backend;
        DROP TABLE page_ocr_results;
    END IF;
END $$"#,
                ),
        )
}
