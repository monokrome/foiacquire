//! Diesel-based configuration history repository.
//!
//! Uses diesel-async for async database support. Works with both SQLite and PostgreSQL.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::models::{ConfigHistoryRecord, NewConfigHistory};
use super::parse_datetime;
use super::pool::{DbPool, DieselError};
use crate::schema::configuration_history;
use crate::with_conn;

/// Maximum number of configuration history entries to retain.
const MAX_HISTORY_ENTRIES: i64 = 16;

/// Represents a stored configuration entry.
#[derive(Debug, Clone)]
pub struct DieselConfigHistoryEntry {
    #[allow(dead_code)]
    pub uuid: String,
    #[allow(dead_code)]
    pub created_at: DateTime<Utc>,
    pub data: String,
    #[allow(dead_code)]
    pub format: String,
    #[allow(dead_code)]
    pub hash: String,
}

impl From<ConfigHistoryRecord> for DieselConfigHistoryEntry {
    fn from(record: ConfigHistoryRecord) -> Self {
        DieselConfigHistoryEntry {
            uuid: record.uuid,
            created_at: parse_datetime(&record.created_at),
            data: record.data,
            format: record.format,
            hash: record.hash,
        }
    }
}

/// Diesel-based configuration history repository with compile-time query checking.
#[derive(Clone)]
pub struct DieselConfigHistoryRepository {
    pool: DbPool,
}

impl DieselConfigHistoryRepository {
    /// Create a new Diesel configuration history repository.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Check if a config with the given hash already exists.
    pub async fn hash_exists(&self, hash: &str) -> Result<bool, DieselError> {
        use diesel::dsl::count_star;
        with_conn!(self.pool, conn, {
            let count: i64 = configuration_history::table
                .filter(configuration_history::hash.eq(hash))
                .select(count_star())
                .first(&mut conn)
                .await?;
            Ok(count > 0)
        })
    }

    /// Insert a new configuration entry if the hash doesn't already exist.
    /// Returns true if inserted, false if hash already exists.
    pub async fn insert_if_new(
        &self,
        data: &str,
        format: &str,
        hash: &str,
    ) -> Result<bool, DieselError> {
        if self.hash_exists(hash).await? {
            return Ok(false);
        }

        let now = Utc::now().to_rfc3339();
        let uuid = uuid::Uuid::new_v4().to_string();

        let new_entry = NewConfigHistory {
            uuid: &uuid,
            created_at: &now,
            data,
            format,
            hash,
        };

        with_conn!(self.pool, conn, {
            diesel::insert_into(configuration_history::table)
                .values(&new_entry)
                .execute(&mut conn)
                .await?;
            Ok::<(), DieselError>(())
        })?;

        // Prune old entries
        self.prune_old_entries().await?;

        Ok(true)
    }

    /// Get the most recent configuration entry.
    pub async fn get_latest(&self) -> Result<Option<DieselConfigHistoryEntry>, DieselError> {
        with_conn!(self.pool, conn, {
            configuration_history::table
                .order(configuration_history::created_at.desc())
                .first::<ConfigHistoryRecord>(&mut conn)
                .await
                .optional()
                .map(|opt| opt.map(DieselConfigHistoryEntry::from))
        })
    }

    /// Get all configuration history entries (most recent first).
    #[allow(dead_code)]
    pub async fn get_all(&self) -> Result<Vec<DieselConfigHistoryEntry>, DieselError> {
        with_conn!(self.pool, conn, {
            configuration_history::table
                .order(configuration_history::created_at.desc())
                .load::<ConfigHistoryRecord>(&mut conn)
                .await
                .map(|records| {
                    records
                        .into_iter()
                        .map(DieselConfigHistoryEntry::from)
                        .collect()
                })
        })
    }

    /// Get just the hash of the most recent configuration entry.
    pub async fn get_latest_hash(&self) -> Result<Option<String>, DieselError> {
        with_conn!(self.pool, conn, {
            configuration_history::table
                .select(configuration_history::hash)
                .order(configuration_history::created_at.desc())
                .first::<String>(&mut conn)
                .await
                .optional()
        })
    }

    /// Prune old entries to keep only the last MAX_HISTORY_ENTRIES.
    async fn prune_old_entries(&self) -> Result<(), DieselError> {
        with_conn!(self.pool, conn, {
            // Get UUIDs to keep (most recent MAX_HISTORY_ENTRIES)
            let uuids_to_keep: Vec<String> = configuration_history::table
                .select(configuration_history::uuid)
                .order(configuration_history::created_at.desc())
                .limit(MAX_HISTORY_ENTRIES)
                .load(&mut conn)
                .await?;

            if !uuids_to_keep.is_empty() {
                // Delete entries not in the keep list
                diesel::delete(
                    configuration_history::table
                        .filter(configuration_history::uuid.ne_all(&uuids_to_keep)),
                )
                .execute(&mut conn)
                .await?;
            }
            Ok(())
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

        conn.batch_execute(
            r#"CREATE TABLE IF NOT EXISTS configuration_history (
                uuid TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                data TEXT NOT NULL,
                format TEXT NOT NULL DEFAULT 'json',
                hash TEXT NOT NULL
            )"#,
        )
        .await
        .unwrap();

        (DbPool::Sqlite(sqlite_pool), dir)
    }

    #[tokio::test]
    async fn test_config_history_crud() {
        let (pool, _dir) = setup_test_db().await;
        let repo = DieselConfigHistoryRepository::new(pool);

        // Insert first entry
        let inserted = repo
            .insert_if_new("{\"key\": \"value1\"}", "json", "hash1")
            .await
            .unwrap();
        assert!(inserted);

        // Check hash exists
        assert!(repo.hash_exists("hash1").await.unwrap());
        assert!(!repo.hash_exists("nonexistent").await.unwrap());

        // Try to insert duplicate hash
        let duplicate = repo
            .insert_if_new("{\"key\": \"value2\"}", "json", "hash1")
            .await
            .unwrap();
        assert!(!duplicate);

        // Insert second entry
        let inserted2 = repo
            .insert_if_new("{\"key\": \"value2\"}", "json", "hash2")
            .await
            .unwrap();
        assert!(inserted2);

        // Get latest
        let latest = repo.get_latest().await.unwrap().unwrap();
        assert_eq!(latest.hash, "hash2");

        // Get latest hash
        let latest_hash = repo.get_latest_hash().await.unwrap().unwrap();
        assert_eq!(latest_hash, "hash2");

        // Get all
        let all = repo.get_all().await.unwrap();
        assert_eq!(all.len(), 2);
    }
}
