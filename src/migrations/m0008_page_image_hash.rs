use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0008_page_image_hash")
        .depends_on(&["0007_add_model_column"])
        .operation(AddField::new(
            "page_ocr_results",
            Field::new("image_hash", FieldType::Text),
        ))
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE INDEX idx_page_ocr_results_image_hash ON page_ocr_results(image_hash);
CREATE INDEX idx_page_ocr_results_hash_backend ON page_ocr_results(image_hash, backend)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE INDEX idx_page_ocr_results_image_hash ON page_ocr_results(image_hash) WHERE image_hash IS NOT NULL;
CREATE INDEX idx_page_ocr_results_hash_backend ON page_ocr_results(image_hash, backend) WHERE image_hash IS NOT NULL"#,
                ),
        )
}
