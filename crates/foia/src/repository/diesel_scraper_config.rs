//! Diesel-based scraper config repository.
//!
//! Stores per-source scraper configurations in the `scraper_configs` table.
//! Uses diesel-async for async database support. Works with both SQLite and PostgreSQL.

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::models::{NewScraperConfig, ScraperConfigRecord};
use super::pool::{DbPool, DieselError};
use crate::config::ScraperConfig;
use crate::schema::scraper_configs;
use crate::{with_conn, with_conn_split};

/// Diesel-based scraper config repository with compile-time query checking.
#[derive(Clone)]
pub struct DieselScraperConfigRepository {
    pool: DbPool,
}

impl DieselScraperConfigRepository {
    /// Create a new scraper config repository.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Get a scraper config by source ID.
    pub async fn get(&self, source_id: &str) -> Result<Option<ScraperConfig>, DieselError> {
        let record: Option<ScraperConfigRecord> = with_conn!(self.pool, conn, {
            scraper_configs::table
                .find(source_id)
                .first::<ScraperConfigRecord>(&mut conn)
                .await
                .optional()?
        });

        match record {
            Some(r) => {
                let config: ScraperConfig = serde_json::from_str(&r.config)
                    .map_err(|e| DieselError::DeserializationError(Box::new(e)))?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    /// Get all scraper configs as (source_id, config) pairs.
    pub async fn get_all(&self) -> Result<Vec<(String, ScraperConfig)>, DieselError> {
        let records: Vec<ScraperConfigRecord> = with_conn!(self.pool, conn, {
            scraper_configs::table
                .load::<ScraperConfigRecord>(&mut conn)
                .await?
        });

        let mut results = Vec::with_capacity(records.len());
        for r in records {
            let config: ScraperConfig = serde_json::from_str(&r.config)
                .map_err(|e| DieselError::DeserializationError(Box::new(e)))?;
            results.push((r.source_id, config));
        }
        Ok(results)
    }

    /// List all source IDs that have scraper configs.
    pub async fn list_source_ids(&self) -> Result<Vec<String>, DieselError> {
        with_conn!(self.pool, conn, {
            scraper_configs::table
                .select(scraper_configs::source_id)
                .load::<String>(&mut conn)
                .await
        })
    }

    /// Upsert a scraper config for a source.
    pub async fn upsert(&self, source_id: &str, config: &ScraperConfig) -> Result<(), DieselError> {
        let config_json = serde_json::to_string(config)
            .map_err(|e| DieselError::SerializationError(Box::new(e)))?;
        let now = Utc::now().to_rfc3339();

        with_conn_split!(self.pool,
            sqlite: conn => {
                let new = NewScraperConfig {
                    source_id,
                    config: &config_json,
                    created_at: &now,
                    updated_at: &now,
                };
                diesel::replace_into(scraper_configs::table)
                    .values(&new)
                    .execute(&mut conn)
                    .await?;
                Ok(())
            },
            postgres: conn => {
                let new = NewScraperConfig {
                    source_id,
                    config: &config_json,
                    created_at: &now,
                    updated_at: &now,
                };
                diesel::insert_into(scraper_configs::table)
                    .values(&new)
                    .on_conflict(scraper_configs::source_id)
                    .do_update()
                    .set((
                        scraper_configs::config.eq(&config_json),
                        scraper_configs::updated_at.eq(&now),
                    ))
                    .execute(&mut conn)
                    .await?;
                Ok(())
            }
        )
    }

    /// Delete a scraper config by source ID.
    pub async fn delete(&self, source_id: &str) -> Result<bool, DieselError> {
        let rows = with_conn!(self.pool, conn, {
            diesel::delete(scraper_configs::table.find(source_id))
                .execute(&mut conn)
                .await?
        });
        Ok(rows > 0)
    }

    /// Check if the scraper_configs table has any entries.
    pub async fn is_empty(&self) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let n: i64 = scraper_configs::table
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(n == 0)
        })
    }

    /// Get the maximum updated_at timestamp across all configs.
    pub async fn max_updated_at(&self) -> Result<Option<String>, DieselError> {
        with_conn!(self.pool, conn, {
            scraper_configs::table
                .select(diesel::dsl::max(scraper_configs::updated_at))
                .first::<Option<String>>(&mut conn)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::pool::SqlitePool;
    use diesel_async::SimpleAsyncConnection;
    use tempfile::tempdir;

    async fn setup_test_db() -> (DbPool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let sqlite_pool = SqlitePool::from_path(&db_path);
        let mut conn = sqlite_pool.get().await.unwrap();

        conn.batch_execute(
            r#"CREATE TABLE IF NOT EXISTS scraper_configs (
                source_id TEXT PRIMARY KEY,
                config TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
        )
        .await
        .unwrap();

        (DbPool::Sqlite(sqlite_pool), dir)
    }

    #[tokio::test]
    async fn test_scraper_config_crud() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselScraperConfigRepository::new(pool);

        // Initially empty
        assert!(repo.is_empty().await.unwrap());
        assert!(repo.get("test-source").await.unwrap().is_none());
        assert!(repo.list_source_ids().await.unwrap().is_empty());

        // Upsert a config
        let config = ScraperConfig {
            name: Some("Test Source".to_string()),
            base_url: Some("https://example.com".to_string()),
            ..Default::default()
        };
        repo.upsert("test-source", &config).await.unwrap();

        // Verify retrieval
        assert!(!repo.is_empty().await.unwrap());
        let retrieved = repo.get("test-source").await.unwrap().unwrap();
        assert_eq!(retrieved.name, Some("Test Source".to_string()));
        assert_eq!(retrieved.base_url, Some("https://example.com".to_string()));

        // List source IDs
        let ids = repo.list_source_ids().await.unwrap();
        assert_eq!(ids, vec!["test-source"]);

        // Get all
        let all = repo.get_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, "test-source");

        // Update
        let updated_config = ScraperConfig {
            name: Some("Updated Source".to_string()),
            base_url: Some("https://updated.com".to_string()),
            ..Default::default()
        };
        repo.upsert("test-source", &updated_config).await.unwrap();
        let retrieved = repo.get("test-source").await.unwrap().unwrap();
        assert_eq!(retrieved.name, Some("Updated Source".to_string()));

        // Delete
        assert!(repo.delete("test-source").await.unwrap());
        assert!(repo.is_empty().await.unwrap());
        assert!(!repo.delete("test-source").await.unwrap());
    }

    #[tokio::test]
    async fn test_max_updated_at() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselScraperConfigRepository::new(pool);

        // Empty table
        assert!(repo.max_updated_at().await.unwrap().is_none());

        // Add configs
        let config = ScraperConfig::default();
        repo.upsert("source-a", &config).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        repo.upsert("source-b", &config).await.unwrap();

        let max = repo.max_updated_at().await.unwrap().unwrap();
        assert!(!max.is_empty());
    }
}
