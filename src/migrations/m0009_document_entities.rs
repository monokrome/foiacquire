use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0009_document_entities")
        .depends_on(&["0008_page_image_hash"])
        // Create document_entities table (both backends)
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS document_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_text TEXT NOT NULL,
    normalized_text TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    created_at TEXT NOT NULL
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS document_entities (
    id SERIAL PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_text TEXT NOT NULL,
    normalized_text TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TEXT NOT NULL
)"#,
                ),
        )
        // Index on document_id for fast joins/deletes
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "CREATE INDEX IF NOT EXISTS idx_document_entities_doc_id ON document_entities(document_id)",
                )
                .for_backend(
                    "postgres",
                    "CREATE INDEX IF NOT EXISTS idx_document_entities_doc_id ON document_entities(document_id)",
                ),
        )
        // Unique index for entity search + dedup
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_document_entities_type_text_doc ON document_entities(entity_type, normalized_text, document_id)",
                )
                .for_backend(
                    "postgres",
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_document_entities_type_text_doc ON document_entities(entity_type, normalized_text, document_id)",
                ),
        )
        // PostgreSQL-only: regions table and spatial indexes (wrapped in PostGIS check)
        .operation(
            RunSql::portable()
                .for_backend("sqlite", "SELECT 1")
                .for_backend(
                    "postgres",
                    r#"DO $$ BEGIN
IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
    CREATE TABLE IF NOT EXISTS regions (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        region_type TEXT NOT NULL,
        iso_code TEXT,
        geom GEOGRAPHY(MultiPolygon, 4326) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_regions_geom ON regions USING GIST (geom);
    CREATE INDEX IF NOT EXISTS idx_regions_name ON regions (lower(name));
    CREATE INDEX IF NOT EXISTS idx_document_entities_spatial
        ON document_entities USING GIST (
            ST_MakePoint(longitude, latitude)::geography
        ) WHERE latitude IS NOT NULL;
END IF;
END $$"#,
                ),
        )
}
