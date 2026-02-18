use cetane::prelude::*;
use sea_query::{ConditionalStatement, Expr, Index as SeaIndex, PostgresQueryBuilder, SqliteQueryBuilder};

use crate::repository::sea_tables::{DocumentEntities, DocumentPages};

pub fn migration() -> Migration {
    // GIN index for full-text search (Postgres only, no SQLite equivalent)
    let fts_pg = SeaIndex::create()
        .if_not_exists()
        .name("idx_pages_fts")
        .table(DocumentPages::Table)
        .full_text()
        .col(Expr::cust(
            "to_tsvector('english', COALESCE(final_text, ocr_text, pdf_text, ''))",
        ))
        .to_string(PostgresQueryBuilder);

    // Entity type index for top_entities() GROUP BY queries
    let entity_type_pg = SeaIndex::create()
        .if_not_exists()
        .name("idx_document_entities_entity_type")
        .table(DocumentEntities::Table)
        .col(DocumentEntities::EntityType)
        .to_string(PostgresQueryBuilder);

    let entity_type_sqlite = SeaIndex::create()
        .if_not_exists()
        .name("idx_document_entities_entity_type")
        .table(DocumentEntities::Table)
        .col(DocumentEntities::EntityType)
        .to_string(SqliteQueryBuilder);

    // Partial index for geocoded entity lookups
    let geocoded_pg = SeaIndex::create()
        .if_not_exists()
        .name("idx_document_entities_geocoded")
        .table(DocumentEntities::Table)
        .col(DocumentEntities::Latitude)
        .col(DocumentEntities::Longitude)
        .and_where(Expr::col(DocumentEntities::Latitude).is_not_null())
        .to_string(PostgresQueryBuilder);

    let geocoded_sqlite = SeaIndex::create()
        .if_not_exists()
        .name("idx_document_entities_geocoded")
        .table(DocumentEntities::Table)
        .col(DocumentEntities::Latitude)
        .col(DocumentEntities::Longitude)
        .and_where(Expr::col(DocumentEntities::Latitude).is_not_null())
        .to_string(SqliteQueryBuilder);

    Migration::new("0014_search_indexes")
        .depends_on(&["0009_document_entities", "0006_page_ocr_results"])
        .operation(
            RunSql::portable()
                .for_backend("sqlite", "SELECT 1")
                .for_backend("postgres", fts_pg),
        )
        .operation(
            RunSql::portable()
                .for_backend("sqlite", entity_type_sqlite)
                .for_backend("postgres", entity_type_pg),
        )
        .operation(
            RunSql::portable()
                .for_backend("sqlite", geocoded_sqlite)
                .for_backend("postgres", geocoded_pg),
        )
}
