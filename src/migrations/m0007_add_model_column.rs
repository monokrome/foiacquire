use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0007_add_model_column")
        .depends_on(&["0006_page_ocr_results"])
        // Add model columns
        .operation(AddField::new(
            "page_ocr_results",
            Field::new("model", FieldType::Text),
        ))
        .operation(AddField::new(
            "document_analysis_results",
            Field::new("model", FieldType::Text),
        ))
        // Recreate unique indexes with model
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"DROP INDEX IF EXISTS idx_page_ocr_results_unique;
CREATE UNIQUE INDEX idx_page_ocr_results_unique ON page_ocr_results(page_id, backend, COALESCE(model, ''));
DROP INDEX IF EXISTS idx_analysis_results_page_unique;
DROP INDEX IF EXISTS idx_analysis_results_doc_unique;
CREATE UNIQUE INDEX idx_analysis_results_page_unique ON document_analysis_results(page_id, analysis_type, backend, COALESCE(model, '')) WHERE page_id IS NOT NULL;
CREATE UNIQUE INDEX idx_analysis_results_doc_unique ON document_analysis_results(document_id, version_id, analysis_type, backend, COALESCE(model, '')) WHERE page_id IS NULL"#,
                )
                .for_backend(
                    "postgres",
                    r#"ALTER TABLE page_ocr_results DROP CONSTRAINT IF EXISTS page_ocr_results_page_id_backend_key;
CREATE UNIQUE INDEX idx_page_ocr_results_unique ON page_ocr_results(page_id, backend, COALESCE(model, ''));
DROP INDEX IF EXISTS idx_analysis_results_page_unique;
DROP INDEX IF EXISTS idx_analysis_results_doc_unique;
CREATE UNIQUE INDEX idx_analysis_results_page_unique ON document_analysis_results(page_id, analysis_type, backend, COALESCE(model, '')) WHERE page_id IS NOT NULL;
CREATE UNIQUE INDEX idx_analysis_results_doc_unique ON document_analysis_results(document_id, version_id, analysis_type, backend, COALESCE(model, '')) WHERE page_id IS NULL"#,
                ),
        )
        // Add model indexes
        .operation(AddIndex::new(
            "page_ocr_results",
            Index::new("idx_page_ocr_results_model").column("model"),
        ))
        .operation(AddIndex::new(
            "document_analysis_results",
            Index::new("idx_analysis_results_model").column("model"),
        ))
}
