//! Diesel-based source repository.
//!
//! Uses diesel-async to provide an async interface while maintaining
//! Diesel's compile-time query checking. Supports both SQLite and PostgreSQL.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::models::SourceRecord;
use super::pool::{DbPool, DieselError};
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{Source, SourceType};
use crate::schema::sources;
use crate::with_conn;

/// Convert a database record to a domain model.
impl TryFrom<SourceRecord> for Source {
    type Error = diesel::result::Error;

    fn try_from(record: SourceRecord) -> Result<Self, Self::Error> {
        let metadata = serde_json::from_str(&record.metadata)
            .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;

        Ok(Source {
            id: record.id,
            source_type: SourceType::from_str(&record.source_type).unwrap_or(SourceType::Custom),
            name: record.name,
            base_url: record.base_url,
            metadata,
            created_at: parse_datetime(&record.created_at),
            last_scraped: parse_datetime_opt(record.last_scraped),
        })
    }
}

/// Diesel-based source repository with compile-time query checking.
#[derive(Clone)]
pub struct DieselSourceRepository {
    pool: DbPool,
}

impl DieselSourceRepository {
    /// Create a new Diesel source repository with an existing pool.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Get a source by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Source>, DieselError> {
        with_conn!(self.pool, conn, {
            sources::table
                .find(id)
                .first::<SourceRecord>(&mut conn)
                .await
                .optional()
                .and_then(|opt| opt.map(Source::try_from).transpose())
        })
    }

    /// Get all sources.
    pub async fn get_all(&self) -> Result<Vec<Source>, DieselError> {
        with_conn!(self.pool, conn, {
            sources::table
                .load::<SourceRecord>(&mut conn)
                .await
                .and_then(|records| records.into_iter().map(Source::try_from).collect())
        })
    }

    /// Save a source (insert or update).
    pub async fn save(&self, source: &Source) -> Result<(), DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::Sources;
        use sea_query::{OnConflict, Query};

        let metadata_json =
            serde_json::to_string(&source.metadata).unwrap_or_else(|_| "{}".to_string());
        let created_at = source.created_at.to_rfc3339();
        let last_scraped = source.last_scraped.map(|dt| dt.to_rfc3339());
        let source_type = source.source_type.as_str().to_string();

        let stmt = Query::insert()
            .into_table(Sources::Table)
            .columns([
                Sources::Id,
                Sources::SourceType,
                Sources::Name,
                Sources::BaseUrl,
                Sources::Metadata,
                Sources::CreatedAt,
                Sources::LastScraped,
            ])
            .values_panic([
                source.id.clone().into(),
                source_type.clone().into(),
                source.name.clone().into(),
                source.base_url.clone().into(),
                metadata_json.clone().into(),
                created_at.clone().into(),
                last_scraped.clone().into(),
            ])
            .on_conflict(
                OnConflict::column(Sources::Id)
                    .update_columns([
                        Sources::SourceType,
                        Sources::Name,
                        Sources::BaseUrl,
                        Sources::Metadata,
                        Sources::LastScraped,
                    ])
                    .to_owned(),
            )
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Text, _>(&source.id)
                .bind::<diesel::sql_types::Text, _>(&source_type)
                .bind::<diesel::sql_types::Text, _>(&source.name)
                .bind::<diesel::sql_types::Text, _>(&source.base_url)
                .bind::<diesel::sql_types::Text, _>(&metadata_json)
                .bind::<diesel::sql_types::Text, _>(&created_at)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    last_scraped.as_deref(),
                )
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }

    /// Delete a source.
    #[allow(dead_code)]
    pub async fn delete(&self, id: &str) -> Result<bool, DieselError> {
        with_conn!(self.pool, conn, {
            let rows = diesel::delete(sources::table.find(id))
                .execute(&mut conn)
                .await?;
            Ok(rows > 0)
        })
    }

    /// Check if a source exists.
    pub async fn exists(&self, id: &str) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = sources::table
                .filter(sources::id.eq(id))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count > 0)
        })
    }

    /// Update last scraped timestamp.
    #[allow(dead_code)]
    pub async fn update_last_scraped(
        &self,
        id: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), DieselError> {
        let ts = timestamp.to_rfc3339();
        with_conn!(self.pool, conn, {
            diesel::update(sources::table.find(id))
                .set(sources::last_scraped.eq(Some(&ts)))
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }

    /// Rename a source ID, updating all related tables.
    /// Returns the number of documents and crawl URLs updated.
    pub async fn rename(&self, old_id: &str, new_id: &str) -> Result<(usize, usize), DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::{CrawlConfig, CrawlUrls, Documents, Sources};
        use sea_query::{Expr, Query};

        let update_docs = Query::update()
            .table(Documents::Table)
            .value(Documents::SourceId, new_id)
            .and_where(Expr::col(Documents::SourceId).eq(old_id))
            .to_owned();
        let update_crawl_urls = Query::update()
            .table(CrawlUrls::Table)
            .value(CrawlUrls::SourceId, new_id)
            .and_where(Expr::col(CrawlUrls::SourceId).eq(old_id))
            .to_owned();
        let update_crawl_config = Query::update()
            .table(CrawlConfig::Table)
            .value(CrawlConfig::SourceId, new_id)
            .and_where(Expr::col(CrawlConfig::SourceId).eq(old_id))
            .to_owned();
        let update_sources = Query::update()
            .table(Sources::Table)
            .value(Sources::Id, new_id)
            .and_where(Expr::col(Sources::Id).eq(old_id))
            .to_owned();

        let sql_docs = build_sql(&self.pool, &update_docs);
        let sql_crawl_urls = build_sql(&self.pool, &update_crawl_urls);
        let sql_crawl_config = build_sql(&self.pool, &update_crawl_config);
        let sql_sources = build_sql(&self.pool, &update_sources);

        with_conn!(self.pool, conn, {
            let docs_updated = diesel::sql_query(&sql_docs).execute(&mut conn).await?;
            let crawls_updated = diesel::sql_query(&sql_crawl_urls)
                .execute(&mut conn)
                .await?;
            diesel::sql_query(&sql_crawl_config)
                .execute(&mut conn)
                .await?;
            diesel::sql_query(&sql_sources).execute(&mut conn).await?;
            Ok((docs_updated, crawls_updated))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::pool::SqlitePool;
    use super::*;
    use diesel_async::SimpleAsyncConnection;
    use tempfile::tempdir;

    async fn setup_test_db() -> (DbPool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let sqlite_pool = SqlitePool::from_path(&db_path);
        let mut conn = sqlite_pool.get().await.unwrap();

        // Create tables
        conn.batch_execute(
            r#"CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                last_scraped TEXT
            )"#,
        )
        .await
        .unwrap();

        (DbPool::Sqlite(sqlite_pool), dir)
    }

    #[tokio::test]
    async fn test_source_crud() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselSourceRepository::new(pool);

        // Create a source
        let source = Source::new(
            "test-source".to_string(),
            SourceType::Custom,
            "Test Source".to_string(),
            "https://example.com".to_string(),
        );

        // Save
        repo.save(&source).await.unwrap();

        // Check exists
        assert!(repo.exists("test-source").await.unwrap());

        // Get
        let fetched = repo.get("test-source").await.unwrap().unwrap();
        assert_eq!(fetched.name, "Test Source");
        assert_eq!(fetched.base_url, "https://example.com");

        // Get all
        let all = repo.get_all().await.unwrap();
        assert_eq!(all.len(), 1);

        // Delete
        let deleted = repo.delete("test-source").await.unwrap();
        assert!(deleted);

        // Verify deleted
        assert!(!repo.exists("test-source").await.unwrap());
    }

    async fn insert_raw_source(pool: &DbPool, sql: &str) {
        match pool {
            DbPool::Sqlite(ref sqlite_pool) => {
                let mut conn = sqlite_pool.get().await.unwrap();
                conn.batch_execute(sql).await.unwrap();
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(_) => unreachable!("test uses sqlite"),
        }
    }

    #[tokio::test]
    async fn test_invalid_metadata_json_returns_error() {
        let (pool, _dir) = setup_test_db().await;

        insert_raw_source(
            &pool,
            "INSERT INTO sources (id, source_type, name, base_url, metadata, created_at) \
             VALUES ('bad', 'custom', 'Bad Source', 'https://example.com', 'not json', '2024-01-01T00:00:00Z')",
        )
        .await;

        let repo = DieselSourceRepository::new(pool);
        let result = repo.get("bad").await;
        assert!(result.is_err());
        let err = format!("{:?}", result.unwrap_err());
        assert!(
            err.contains("Deserialization"),
            "Expected DeserializationError, got: {}",
            err,
        );
    }

    #[tokio::test]
    async fn test_get_all_fails_on_invalid_json_row() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselSourceRepository::new(pool.clone());

        // Insert a valid row
        let source = Source::new(
            "valid-source".to_string(),
            SourceType::Custom,
            "Valid Source".to_string(),
            "https://example.com".to_string(),
        );
        repo.save(&source).await.unwrap();

        // Insert a row with invalid JSON metadata
        insert_raw_source(
            &pool,
            "INSERT INTO sources (id, source_type, name, base_url, metadata, created_at) \
             VALUES ('bad', 'custom', 'Bad Source', 'https://example.com', 'not json', '2024-01-01T00:00:00Z')",
        )
        .await;

        let result = repo.get_all().await;
        assert!(result.is_err());
    }
}
