use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0006_page_ocr_results")
        .depends_on(&["0003_analysis_results"])
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE page_ocr_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id INTEGER NOT NULL,
    backend TEXT NOT NULL,
    text TEXT,
    confidence REAL,
    quality_score REAL,
    char_count INTEGER,
    word_count INTEGER,
    processing_time_ms INTEGER,
    error_message TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (page_id) REFERENCES document_pages(id) ON DELETE CASCADE,
    UNIQUE(page_id, backend)
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE page_ocr_results (
    id SERIAL PRIMARY KEY,
    page_id INTEGER NOT NULL REFERENCES document_pages(id) ON DELETE CASCADE,
    backend TEXT NOT NULL,
    text TEXT,
    confidence REAL,
    quality_score REAL,
    char_count INTEGER,
    word_count INTEGER,
    processing_time_ms INTEGER,
    error_message TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(page_id, backend)
)"#,
                ),
        )
        .operation(AddIndex::new(
            "page_ocr_results",
            Index::new("idx_page_ocr_results_page").column("page_id"),
        ))
        .operation(AddIndex::new(
            "page_ocr_results",
            Index::new("idx_page_ocr_results_backend").column("backend"),
        ))
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE INDEX idx_page_ocr_results_quality ON page_ocr_results(quality_score DESC);
CREATE INDEX idx_page_ocr_results_page_quality ON page_ocr_results(page_id, quality_score DESC)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE INDEX idx_page_ocr_results_quality ON page_ocr_results(quality_score DESC NULLS LAST);
CREATE INDEX idx_page_ocr_results_page_quality ON page_ocr_results(page_id, quality_score DESC NULLS LAST)"#,
                ),
        )
        // Migrate existing OCR data
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'tesseract',
    ocr_text,
    LENGTH(ocr_text),
    LENGTH(ocr_text) - LENGTH(REPLACE(ocr_text, ' ', '')) + 1,
    COALESCE(updated_at, created_at)
FROM document_pages
WHERE ocr_text IS NOT NULL AND ocr_text != ''"#,
                )
                .for_backend(
                    "postgres",
                    r#"INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'tesseract',
    ocr_text,
    LENGTH(ocr_text),
    array_length(regexp_split_to_array(ocr_text, '\s+'), 1),
    COALESCE(updated_at, created_at)
FROM document_pages
WHERE ocr_text IS NOT NULL AND ocr_text != ''"#,
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'pdftotext',
    pdf_text,
    LENGTH(pdf_text),
    LENGTH(pdf_text) - LENGTH(REPLACE(pdf_text, ' ', '')) + 1,
    COALESCE(updated_at, created_at)
FROM document_pages
WHERE pdf_text IS NOT NULL AND pdf_text != ''"#,
                )
                .for_backend(
                    "postgres",
                    r#"INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'pdftotext',
    pdf_text,
    LENGTH(pdf_text),
    array_length(regexp_split_to_array(pdf_text, '\s+'), 1),
    COALESCE(updated_at, created_at)
FROM document_pages
WHERE pdf_text IS NOT NULL AND pdf_text != ''"#,
                ),
        )
}
