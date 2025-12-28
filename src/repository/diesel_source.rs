//! Diesel-based source repository.
//!
//! Uses diesel-async to provide an async interface while maintaining
//! Diesel's compile-time query checking. Supports both SQLite and PostgreSQL.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::diesel_context::DbPool;
use super::diesel_models::SourceRecord;
use super::diesel_pool::DieselError;
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{Source, SourceType};
use crate::schema::sources;
use crate::with_diesel_conn;

/// Convert a database record to a domain model.
impl From<SourceRecord> for Source {
    fn from(record: SourceRecord) -> Self {
        Source {
            id: record.id,
            source_type: SourceType::from_str(&record.source_type).unwrap_or(SourceType::Custom),
            name: record.name,
            base_url: record.base_url,
            metadata: serde_json::from_str(&record.metadata).unwrap_or_default(),
            created_at: parse_datetime(&record.created_at),
            last_scraped: parse_datetime_opt(record.last_scraped),
        }
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
        with_diesel_conn!(self.pool, conn, {
            sources::table
                .find(id)
                .first::<SourceRecord>(&mut conn)
                .await
                .optional()
                .map(|opt| opt.map(Source::from))
        })
    }

    /// Get all sources.
    pub async fn get_all(&self) -> Result<Vec<Source>, DieselError> {
        with_diesel_conn!(self.pool, conn, {
            sources::table
                .load::<SourceRecord>(&mut conn)
                .await
                .map(|records| records.into_iter().map(Source::from).collect())
        })
    }

    /// Save a source (insert or update).
    pub async fn save(&self, source: &Source) -> Result<(), DieselError> {
        let metadata_json =
            serde_json::to_string(&source.metadata).unwrap_or_else(|_| "{}".to_string());
        let created_at = source.created_at.to_rfc3339();
        let last_scraped = source.last_scraped.map(|dt| dt.to_rfc3339());
        let source_type = source.source_type.as_str().to_string();

        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
                // Use replace_into for SQLite upsert
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
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                use diesel::upsert::excluded;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
                // Use ON CONFLICT for PostgreSQL upsert
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
            }
        }
        Ok(())
    }

    /// Delete a source.
    #[allow(dead_code)]
    pub async fn delete(&self, id: &str) -> Result<bool, DieselError> {
        with_diesel_conn!(self.pool, conn, {
            let rows = diesel::delete(sources::table.find(id))
                .execute(&mut conn)
                .await?;
            Ok(rows > 0)
        })
    }

    /// Check if a source exists.
    pub async fn exists(&self, id: &str) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_diesel_conn!(self.pool, conn, {
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
        with_diesel_conn!(self.pool, conn, {
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
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut conn = pool.get().await?;
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
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                use super::util::to_diesel_error;
                let mut conn = pool.get().await.map_err(to_diesel_error)?;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::diesel_pool::AsyncSqlitePool;
    use super::*;
    use diesel_async::{AsyncConnection, SimpleAsyncConnection};
    use tempfile::tempdir;

    async fn setup_test_db() -> (DbPool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_url = db_path.display().to_string();

        let sqlite_pool = AsyncSqlitePool::new(&db_url, 5);
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
}
