//! Entity CRUD, search, and spatial query methods.

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

#[allow(unused_imports)]
use super::{CountRow, DieselDocumentRepository, DocIdRow};
use crate::repository::diesel_models::{DocumentEntityRecord, NewDocumentEntity};
use crate::repository::pool::DieselError;
use crate::schema::document_entities;
use crate::{with_conn, with_conn_split};

/// Filter for entity-based document search.
#[derive(Debug, Clone)]
pub struct EntityFilter {
    pub entity_type: Option<String>,
    pub text: String,
    pub exact: bool,
}

/// Entity type + count pair for statistics.
#[derive(diesel::QueryableByName, Debug)]
pub struct EntityTypeCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub entity_type: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub count: i64,
}

/// Entity text + count pair for top-N queries.
#[derive(diesel::QueryableByName, Debug)]
pub struct EntityTextCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub entity_text: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub count: i64,
}

impl DieselDocumentRepository {
    /// Save document entities.
    /// Uses INSERT OR IGNORE (SQLite, one at a time) / ON CONFLICT DO NOTHING (Postgres, batch).
    pub async fn save_document_entities(
        &self,
        entities: &[NewDocumentEntity<'_>],
    ) -> Result<(), DieselError> {
        if entities.is_empty() {
            return Ok(());
        }

        with_conn_split!(self.pool,
            sqlite: conn => {
                // SQLite doesn't support batch insert_or_ignore, insert one at a time
                for entity in entities {
                    diesel::insert_or_ignore_into(document_entities::table)
                        .values(entity)
                        .execute(&mut conn)
                        .await?;
                }
                Ok::<_, DieselError>(())
            },
            postgres: conn => {
                for chunk in entities.chunks(50) {
                    diesel::insert_into(document_entities::table)
                        .values(chunk)
                        .on_conflict_do_nothing()
                        .execute(&mut conn)
                        .await?;
                }
                Ok::<_, DieselError>(())
            }
        )?;

        Ok(())
    }

    /// Delete all entities for a document (before re-extraction).
    pub async fn delete_document_entities(&self, doc_id: &str) -> Result<usize, DieselError> {
        with_conn!(self.pool, conn, {
            diesel::delete(
                document_entities::table.filter(document_entities::document_id.eq(doc_id)),
            )
            .execute(&mut conn)
            .await
        })
    }

    /// Get all entities for a specific document.
    pub async fn get_document_entities(
        &self,
        doc_id: &str,
    ) -> Result<Vec<DocumentEntityRecord>, DieselError> {
        with_conn!(self.pool, conn, {
            document_entities::table
                .filter(document_entities::document_id.eq(doc_id))
                .order(document_entities::entity_type.asc())
                .load(&mut conn)
                .await
        })
    }

    /// Get entities for multiple documents in a single query.
    pub async fn get_entities_batch(
        &self,
        doc_ids: &[String],
    ) -> Result<std::collections::HashMap<String, Vec<DocumentEntityRecord>>, DieselError> {
        if doc_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let records: Vec<DocumentEntityRecord> = with_conn!(self.pool, conn, {
            document_entities::table
                .filter(document_entities::document_id.eq_any(doc_ids))
                .order(document_entities::document_id.asc())
                .load(&mut conn)
                .await
        })?;

        let mut map: std::collections::HashMap<String, Vec<DocumentEntityRecord>> =
            std::collections::HashMap::new();
        for record in records {
            map.entry(record.document_id.clone())
                .or_default()
                .push(record);
        }
        Ok(map)
    }

    /// Search for document IDs matching ALL entity filters.
    pub async fn search_by_entities(
        &self,
        filters: &[EntityFilter],
        source_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<String>, DieselError> {
        let all_ids = self.entity_filter_intersection(filters, source_id).await?;
        Ok(all_ids.into_iter().skip(offset).take(limit).collect())
    }

    /// Count documents matching ALL entity filters.
    pub async fn count_by_entities(
        &self,
        filters: &[EntityFilter],
        source_id: Option<&str>,
    ) -> Result<u64, DieselError> {
        let all_ids = self.entity_filter_intersection(filters, source_id).await?;
        Ok(all_ids.len() as u64)
    }

    /// Get all document IDs matching ALL entity filters (AND semantics).
    async fn entity_filter_intersection(
        &self,
        filters: &[EntityFilter],
        source_id: Option<&str>,
    ) -> Result<Vec<String>, DieselError> {
        if filters.is_empty() {
            return Ok(vec![]);
        }

        if filters.len() == 1 {
            return self
                .search_single_entity_filter(&filters[0], source_id)
                .await;
        }

        let mut result_sets: Vec<std::collections::HashSet<String>> = Vec::new();
        for filter in filters {
            let ids = self.search_single_entity_filter(filter, source_id).await?;
            result_sets.push(ids.into_iter().collect());
        }

        let mut intersection = result_sets.remove(0);
        for set in &result_sets {
            intersection.retain(|id| set.contains(id));
        }

        let mut sorted: Vec<String> = intersection.into_iter().collect();
        sorted.sort();
        Ok(sorted)
    }

    /// Execute a single entity filter using Diesel query builder.
    async fn search_single_entity_filter(
        &self,
        filter: &EntityFilter,
        source_id: Option<&str>,
    ) -> Result<Vec<String>, DieselError> {
        let lower_text = filter.text.to_lowercase();

        with_conn!(self.pool, conn, {
            let mut query = document_entities::table
                .select(document_entities::document_id)
                .distinct()
                .into_boxed();

            if filter.exact {
                query = query.filter(document_entities::normalized_text.eq(&lower_text));
            } else {
                let pattern = format!("%{}%", lower_text);
                query = query.filter(document_entities::normalized_text.like(pattern));
            }

            if let Some(ref entity_type) = filter.entity_type {
                query = query.filter(document_entities::entity_type.eq(entity_type));
            }

            if let Some(sid) = source_id {
                use crate::schema::documents;
                let source_doc_ids = documents::table
                    .filter(documents::source_id.eq(sid))
                    .select(documents::id);
                query = query.filter(document_entities::document_id.eq_any(source_doc_ids));
            }

            query
                .order(document_entities::document_id.asc())
                .load::<String>(&mut conn)
                .await
        })
    }

    /// Search for documents near a lat/lng point within a radius (km).
    /// Only works on PostgreSQL with PostGIS. Returns an error on SQLite.
    #[allow(unused_variables)]
    pub async fn search_near_location(
        &self,
        lat: f64,
        lon: f64,
        radius_km: f64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<String>, DieselError> {
        let radius_meters = radius_km * 1000.0;

        with_conn_split!(self.pool,
            sqlite: _conn => {
                Err(diesel::result::Error::QueryBuilderError(
                    "Geospatial queries (near locations, latitude/longitude, etc) are not supported on this database backend.".into()
                ))
            },
            postgres: conn => {
                let query = format!(
                    r#"SELECT DISTINCT de.document_id as id
                    FROM document_entities de
                    WHERE de.latitude IS NOT NULL
                    AND ST_DWithin(
                        ST_MakePoint(de.longitude, de.latitude)::geography,
                        ST_MakePoint({}, {})::geography,
                        {}
                    )
                    ORDER BY de.document_id
                    LIMIT {} OFFSET {}"#,
                    lon, lat, radius_meters, limit, offset
                );
                let rows: Vec<DocIdRow> =
                    diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;
                Ok(rows.into_iter().map(|r| r.id).collect())
            }
        )
    }

    /// Count documents near a lat/lng point within a radius (km).
    /// Only works on PostgreSQL with PostGIS. Returns an error on SQLite.
    #[allow(unused_variables)]
    pub async fn count_near_location(
        &self,
        lat: f64,
        lon: f64,
        radius_km: f64,
    ) -> Result<u64, DieselError> {
        let radius_meters = radius_km * 1000.0;

        with_conn_split!(self.pool,
            sqlite: _conn => {
                Err(diesel::result::Error::QueryBuilderError(
                    "Geospatial queries (near locations, latitude/longitude, etc) are not supported on this database backend.".into()
                ))
            },
            postgres: conn => {
                let query = format!(
                    r#"SELECT COUNT(DISTINCT de.document_id) as count
                    FROM document_entities de
                    WHERE de.latitude IS NOT NULL
                    AND ST_DWithin(
                        ST_MakePoint(de.longitude, de.latitude)::geography,
                        ST_MakePoint({}, {})::geography,
                        {}
                    )"#,
                    lon, lat, radius_meters
                );
                let rows: Vec<CountRow> =
                    diesel_async::RunQueryDsl::load(diesel::sql_query(&query), &mut conn).await?;
                #[allow(clippy::get_first)]
                Ok(rows.get(0).map(|r| r.count as u64).unwrap_or(0))
            }
        )
    }

    /// Search for documents within a named region's polygon boundaries.
    /// Only works on PostgreSQL with PostGIS and populated regions table.
    #[allow(dead_code, unused_variables)]
    pub async fn search_in_region(
        &self,
        region_name: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<String>, DieselError> {
        with_conn_split!(self.pool,
            sqlite: _conn => {
                Err(diesel::result::Error::QueryBuilderError(
                    "Geospatial queries (near locations, latitude/longitude, etc) are not supported on this database backend.".into()
                ))
            },
            postgres: conn => {
                let query = format!(
                    r#"SELECT DISTINCT de.document_id as id
                    FROM document_entities de
                    JOIN regions r ON ST_Covers(r.geom, ST_MakePoint(de.longitude, de.latitude)::geography)
                    WHERE de.latitude IS NOT NULL AND lower(r.name) = lower($1)
                    ORDER BY de.document_id
                    LIMIT {} OFFSET {}"#,
                    limit, offset
                );
                let rows: Vec<DocIdRow> = diesel_async::RunQueryDsl::load(
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(region_name),
                    &mut conn,
                )
                .await?;
                Ok(rows.into_iter().map(|r| r.id).collect())
            }
        )
    }

    /// Search for documents near a named region within a radius (km).
    /// Only works on PostgreSQL with PostGIS and populated regions table.
    #[allow(dead_code, unused_variables)]
    pub async fn search_near_region(
        &self,
        region_name: &str,
        radius_km: f64,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<String>, DieselError> {
        let radius_meters = radius_km * 1000.0;

        with_conn_split!(self.pool,
            sqlite: _conn => {
                Err(diesel::result::Error::QueryBuilderError(
                    "Geospatial queries (near locations, latitude/longitude, etc) are not supported on this database backend.".into()
                ))
            },
            postgres: conn => {
                let query = format!(
                    r#"SELECT DISTINCT de.document_id as id
                    FROM document_entities de
                    JOIN regions r ON ST_DWithin(r.geom, ST_MakePoint(de.longitude, de.latitude)::geography, {})
                    WHERE de.latitude IS NOT NULL AND lower(r.name) = lower($1)
                    ORDER BY de.document_id
                    LIMIT {} OFFSET {}"#,
                    radius_meters, limit, offset
                );
                let rows: Vec<DocIdRow> = diesel_async::RunQueryDsl::load(
                    diesel::sql_query(&query)
                        .bind::<diesel::sql_types::Text, _>(region_name),
                    &mut conn,
                )
                .await?;
                Ok(rows.into_iter().map(|r| r.id).collect())
            }
        )
    }

    /// Get entity type breakdown with counts.
    pub async fn get_entity_type_counts(&self) -> Result<Vec<(String, u64)>, DieselError> {
        let query = "SELECT entity_type, COUNT(*) as count FROM document_entities GROUP BY entity_type ORDER BY count DESC";

        with_conn!(self.pool, conn, {
            let rows: Vec<EntityTypeCount> =
                diesel_async::RunQueryDsl::load(diesel::sql_query(query), &mut conn).await?;
            Ok(rows
                .into_iter()
                .map(|r| (r.entity_type, r.count as u64))
                .collect())
        })
    }

    /// Get the most frequent entities of a given type.
    pub async fn get_top_entities(
        &self,
        entity_type: &str,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, DieselError> {
        let query = format!(
            "SELECT entity_text, COUNT(DISTINCT document_id) as count \
             FROM document_entities WHERE entity_type = $1 \
             GROUP BY entity_text ORDER BY count DESC LIMIT {}",
            limit
        );

        with_conn!(self.pool, conn, {
            let rows: Vec<EntityTextCount> = diesel_async::RunQueryDsl::load(
                diesel::sql_query(&query).bind::<diesel::sql_types::Text, _>(entity_type),
                &mut conn,
            )
            .await?;
            Ok(rows
                .into_iter()
                .map(|r| (r.entity_text, r.count as u64))
                .collect())
        })
    }

    /// Get all entities with coordinates (for map views).
    pub async fn get_geocoded_entities(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DocumentEntityRecord>, DieselError> {
        with_conn!(self.pool, conn, {
            document_entities::table
                .filter(document_entities::latitude.is_not_null())
                .order(document_entities::entity_text.asc())
                .limit(limit as i64)
                .offset(offset as i64)
                .load(&mut conn)
                .await
        })
    }

    /// Count all entities with coordinates.
    pub async fn count_geocoded_entities(&self) -> Result<u64, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = document_entities::table
                .filter(document_entities::latitude.is_not_null())
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count as u64)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Document, DocumentStatus};
    use crate::repository::diesel_document::tests::setup_test_db;
    use chrono::Utc;

    async fn create_entity_table(repo: &DieselDocumentRepository) -> Result<(), DieselError> {
        use diesel_async::SimpleAsyncConnection;
        with_conn!(repo.pool, conn, {
            conn.batch_execute(
                r#"CREATE TABLE IF NOT EXISTS document_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_text TEXT NOT NULL,
                    normalized_text TEXT NOT NULL,
                    latitude REAL,
                    longitude REAL,
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_de_type_text_doc
                    ON document_entities(entity_type, normalized_text, document_id)"#,
            )
            .await
            .unwrap();
            Ok::<_, DieselError>(())
        })
    }

    #[tokio::test]
    async fn test_entity_crud() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);
        create_entity_table(&repo).await.unwrap();

        let doc = Document {
            id: "doc-entity-1".to_string(),
            source_id: "test-source".to_string(),
            title: "Entity Test".to_string(),
            source_url: "https://example.com/entity.pdf".to_string(),
            extracted_text: None,
            synopsis: None,
            tags: vec![],
            status: DocumentStatus::Pending,
            metadata: serde_json::Value::Object(Default::default()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            discovery_method: "seed".to_string(),
            versions: vec![],
        };
        repo.save(&doc).await.unwrap();

        let now = Utc::now().to_rfc3339();
        let entities = vec![
            NewDocumentEntity {
                document_id: "doc-entity-1",
                entity_type: "organization",
                entity_text: "CIA",
                normalized_text: "cia",
                latitude: None,
                longitude: None,
                created_at: &now,
            },
            NewDocumentEntity {
                document_id: "doc-entity-1",
                entity_type: "location",
                entity_text: "Langley",
                normalized_text: "langley",
                latitude: Some(38.9338),
                longitude: Some(-77.1771),
                created_at: &now,
            },
        ];

        repo.save_document_entities(&entities).await.unwrap();

        let fetched = repo.get_document_entities("doc-entity-1").await.unwrap();
        assert_eq!(fetched.len(), 2);

        let loc = fetched
            .iter()
            .find(|e| e.entity_type == "location")
            .unwrap();
        assert_eq!(loc.entity_text, "Langley");
        assert!(loc.latitude.is_some());

        let deleted = repo.delete_document_entities("doc-entity-1").await.unwrap();
        assert_eq!(deleted, 2);

        let empty = repo.get_document_entities("doc-entity-1").await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_entity_search() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);
        create_entity_table(&repo).await.unwrap();

        // Create two documents
        for i in 1..=2 {
            let doc = Document {
                id: format!("doc-search-{}", i),
                source_id: "test-source".to_string(),
                title: format!("Search Test {}", i),
                source_url: format!("https://example.com/{}.pdf", i),
                extracted_text: None,
                synopsis: None,
                tags: vec![],
                status: DocumentStatus::Pending,
                metadata: serde_json::Value::Object(Default::default()),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                discovery_method: "seed".to_string(),
                versions: vec![],
            };
            repo.save(&doc).await.unwrap();
        }

        let now = Utc::now().to_rfc3339();
        let entities = vec![
            NewDocumentEntity {
                document_id: "doc-search-1",
                entity_type: "organization",
                entity_text: "CIA",
                normalized_text: "cia",
                latitude: None,
                longitude: None,
                created_at: &now,
            },
            NewDocumentEntity {
                document_id: "doc-search-1",
                entity_type: "person",
                entity_text: "John Smith",
                normalized_text: "john smith",
                latitude: None,
                longitude: None,
                created_at: &now,
            },
            NewDocumentEntity {
                document_id: "doc-search-2",
                entity_type: "organization",
                entity_text: "CIA",
                normalized_text: "cia",
                latitude: None,
                longitude: None,
                created_at: &now,
            },
        ];
        repo.save_document_entities(&entities).await.unwrap();

        // Search for CIA - should match both docs
        let filters = vec![EntityFilter {
            entity_type: Some("organization".to_string()),
            text: "cia".to_string(),
            exact: true,
        }];
        let results = repo
            .search_by_entities(&filters, None, 100, 0)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        // Search for CIA + John Smith - only doc-search-1 has both
        let filters = vec![
            EntityFilter {
                entity_type: Some("organization".to_string()),
                text: "cia".to_string(),
                exact: true,
            },
            EntityFilter {
                entity_type: Some("person".to_string()),
                text: "john smith".to_string(),
                exact: true,
            },
        ];
        let results = repo
            .search_by_entities(&filters, None, 100, 0)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "doc-search-1");

        // Count
        let count = repo.count_by_entities(&filters, None).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_entity_type_counts() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);
        create_entity_table(&repo).await.unwrap();

        let doc = Document {
            id: "doc-counts-1".to_string(),
            source_id: "test-source".to_string(),
            title: "Counts Test".to_string(),
            source_url: "https://example.com/counts.pdf".to_string(),
            extracted_text: None,
            synopsis: None,
            tags: vec![],
            status: DocumentStatus::Pending,
            metadata: serde_json::Value::Object(Default::default()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            discovery_method: "seed".to_string(),
            versions: vec![],
        };
        repo.save(&doc).await.unwrap();

        let now = Utc::now().to_rfc3339();
        let entities = vec![
            NewDocumentEntity {
                document_id: "doc-counts-1",
                entity_type: "organization",
                entity_text: "CIA",
                normalized_text: "cia",
                latitude: None,
                longitude: None,
                created_at: &now,
            },
            NewDocumentEntity {
                document_id: "doc-counts-1",
                entity_type: "organization",
                entity_text: "FBI",
                normalized_text: "fbi",
                latitude: None,
                longitude: None,
                created_at: &now,
            },
            NewDocumentEntity {
                document_id: "doc-counts-1",
                entity_type: "person",
                entity_text: "John Smith",
                normalized_text: "john smith",
                latitude: None,
                longitude: None,
                created_at: &now,
            },
        ];
        repo.save_document_entities(&entities).await.unwrap();

        let counts = repo.get_entity_type_counts().await.unwrap();
        assert!(!counts.is_empty());

        let org_count = counts.iter().find(|(t, _)| t == "organization");
        assert_eq!(org_count.map(|(_, c)| *c), Some(2));

        let top_orgs = repo.get_top_entities("organization", 10).await.unwrap();
        assert_eq!(top_orgs.len(), 2);
    }

    #[tokio::test]
    async fn test_spatial_query_unsupported_on_sqlite() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);

        let result = repo.search_near_location(38.9, -77.0, 100.0, 10, 0).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not supported"));
    }

    #[tokio::test]
    async fn test_entity_search_with_sql_metacharacters() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselDocumentRepository::new(pool);
        create_entity_table(&repo).await.unwrap();

        let filters = vec![EntityFilter {
            entity_type: Some("'; DROP TABLE documents; --".to_string()),
            text: "' OR '1'='1".to_string(),
            exact: false,
        }];
        let result = repo.search_by_entities(&filters, None, 100, 0).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
