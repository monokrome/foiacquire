use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0012_scraper_configs")
        .depends_on(&["0011_constraints"])
        .operation(
            CreateTable::new("scraper_configs")
                .add_field(Field::new("source_id", FieldType::Text).primary_key())
                .add_field(Field::new("config", FieldType::Text).not_null())
                .add_field(Field::new("created_at", FieldType::Text).not_null())
                .add_field(Field::new("updated_at", FieldType::Text).not_null()),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('format_version', '15')",
                )
                .for_backend(
                    "postgres",
                    "INSERT INTO storage_meta (key, value) VALUES ('format_version', '15') \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                ),
        )
}
