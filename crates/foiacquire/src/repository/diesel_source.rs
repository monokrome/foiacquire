//! Diesel-based source repository.
//!
//! Uses diesel-async to provide an async interface while maintaining
//! Diesel's compile-time query checking. Supports both SQLite and PostgreSQL.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::diesel_models::SourceRecord;
use super::pool::{DbPool, DieselError};
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{Source, SourceType};
use crate::schema::sources;
use crate::{with_conn, with_conn_split};

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
        let metadata_json =
            serde_json::to_string(&source.metadata).unwrap_or_else(|_| "{}".to_string());
        let created_at = source.created_at.to_rfc3339();
        let last_scraped = source.last_scraped.map(|dt| dt.to_rfc3339());
        let source_type = source.source_type.as_str().to_string();

        with_conn_split!(self.pool,
            sqlite: conn => {
                diesel::replace_into(sources::table)
                    .values((
                        sources::id.eq(&source.id),
                        sources::source_type.eq(&source_type),
                        sources::name.eq(&source.name),
                        sources::base_url.eq(&source.base_url),
                        sources::metadata.eq(&metadata_json),
                        sources::created_at.eq(&created_at),
                        sources::last_scraped.eq(&last_scraped),
                    ))
                    .execute(&mut conn)
                    .await?;
                Ok(())
            },
            postgres: conn => {
                use diesel::upsert::excluded;
                diesel::insert_into(sources::table)
                    .values((
                        sources::id.eq(&source.id),
                        sources::source_type.eq(&source_type),
                        sources::name.eq(&source.name),
                        sources::base_url.eq(&source.base_url),
                        sources::metadata.eq(&metadata_json),
                        sources::created_at.eq(&created_at),
                        sources::last_scraped.eq(&last_scraped),
                    ))
                    .on_conflict(sources::id)
                    .do_update()
                    .set((
                        sources::source_type.eq(excluded(sources::source_type)),
                        sources::name.eq(excluded(sources::name)),
                        sources::base_url.eq(excluded(sources::base_url)),
                        sources::metadata.eq(excluded(sources::metadata)),
                        sources::last_scraped.eq(excluded(sources::last_scraped)),
                    ))
                    .execute(&mut conn)
                    .await?;
                Ok(())
            }
        )
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
        with_conn_split!(self.pool,
            sqlite: conn => {
                let docs_updated =
                    diesel::sql_query("UPDATE documents SET source_id = ?1 WHERE source_id = ?2")
                        .bind::<diesel::sql_types::Text, _>(new_id)
                        .bind::<diesel::sql_types::Text, _>(old_id)
                        .execute(&mut conn)
                        .await?;

                let crawls_updated =
                    diesel::sql_query("UPDATE crawl_urls SET source_id = ?1 WHERE source_id = ?2")
                        .bind::<diesel::sql_types::Text, _>(new_id)
                        .bind::<diesel::sql_types::Text, _>(old_id)
                        .execute(&mut conn)
                        .await?;

                diesel::sql_query("UPDATE crawl_config SET source_id = ?1 WHERE source_id = ?2")
                    .bind::<diesel::sql_types::Text, _>(new_id)
                    .bind::<diesel::sql_types::Text, _>(old_id)
                    .execute(&mut conn)
                    .await?;

                diesel::sql_query("UPDATE sources SET id = ?1 WHERE id = ?2")
                    .bind::<diesel::sql_types::Text, _>(new_id)
                    .bind::<diesel::sql_types::Text, _>(old_id)
                    .execute(&mut conn)
                    .await?;

                Ok((docs_updated, crawls_updated))
            },
            postgres: conn => {
                let docs_updated =
                    diesel::sql_query("UPDATE documents SET source_id = $1 WHERE source_id = $2")
                        .bind::<diesel::sql_types::Text, _>(new_id)
                        .bind::<diesel::sql_types::Text, _>(old_id)
                        .execute(&mut conn)
                        .await?;

                let crawls_updated =
                    diesel::sql_query("UPDATE crawl_urls SET source_id = $1 WHERE source_id = $2")
                        .bind::<diesel::sql_types::Text, _>(new_id)
                        .bind::<diesel::sql_types::Text, _>(old_id)
                        .execute(&mut conn)
                        .await?;

                diesel::sql_query("UPDATE crawl_config SET source_id = $1 WHERE source_id = $2")
                    .bind::<diesel::sql_types::Text, _>(new_id)
                    .bind::<diesel::sql_types::Text, _>(old_id)
                    .execute(&mut conn)
                    .await?;

                diesel::sql_query("UPDATE sources SET id = $1 WHERE id = $2")
                    .bind::<diesel::sql_types::Text, _>(new_id)
                    .bind::<diesel::sql_types::Text, _>(old_id)
                    .execute(&mut conn)
                    .await?;

                Ok((docs_updated, crawls_updated))
            }
        )
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
