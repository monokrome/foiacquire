//! Diesel-based configuration history repository for SQLite.
//!
//! Uses diesel-async's SyncConnectionWrapper for async SQLite support.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::diesel_models::{ConfigHistoryRecord, NewConfigHistory};
use super::diesel_pool::{AsyncSqlitePool, DieselError};
use super::parse_datetime;
use crate::schema::config_history;

/// Maximum number of configuration history entries to retain.
const MAX_HISTORY_ENTRIES: i64 = 16;

/// Represents a stored configuration entry.
#[derive(Debug, Clone)]
pub struct DieselConfigHistoryEntry {
    pub id: i32,
    pub uuid: String,
    pub created_at: DateTime<Utc>,
    pub data: String,
    pub format: String,
    pub hash: String,
}

impl From<ConfigHistoryRecord> for DieselConfigHistoryEntry {
    fn from(record: ConfigHistoryRecord) -> Self {
        DieselConfigHistoryEntry {
            id: record.id,
            uuid: record.hash.clone(), // Use hash as uuid for display
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
    pool: AsyncSqlitePool,
}

impl DieselConfigHistoryRepository {
    /// Create a new Diesel configuration history repository.
    pub fn new(pool: AsyncSqlitePool) -> Self {
        Self { pool }
    }

    /// Check if a config with the given hash already exists.
    pub async fn hash_exists(&self, hash: &str) -> Result<bool, DieselError> {
        let mut conn = self.pool.get().await?;

        use diesel::dsl::count_star;
        let count: i64 = config_history::table
            .filter(config_history::hash.eq(hash))
            .select(count_star())
            .first(&mut conn)
            .await?;

        Ok(count > 0)
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

        let mut conn = self.pool.get().await?;
        let now = Utc::now().to_rfc3339();

        let new_entry = NewConfigHistory {
            data,
            format,
            hash,
            created_at: &now,
        };

        diesel::insert_into(config_history::table)
            .values(&new_entry)
            .execute(&mut conn)
            .await?;

        // Prune old entries
        self.prune_old_entries().await?;

        Ok(true)
    }

    /// Get the most recent configuration entry.
    pub async fn get_latest(&self) -> Result<Option<DieselConfigHistoryEntry>, DieselError> {
        let mut conn = self.pool.get().await?;

        config_history::table
            .order(config_history::created_at.desc())
            .first::<ConfigHistoryRecord>(&mut conn)
            .await
            .optional()
            .map(|opt| opt.map(DieselConfigHistoryEntry::from))
    }

    /// Get all configuration history entries (most recent first).
    pub async fn get_all(&self) -> Result<Vec<DieselConfigHistoryEntry>, DieselError> {
        let mut conn = self.pool.get().await?;

        config_history::table
            .order(config_history::created_at.desc())
            .load::<ConfigHistoryRecord>(&mut conn)
            .await
            .map(|records| records.into_iter().map(DieselConfigHistoryEntry::from).collect())
    }

    /// Get just the hash of the most recent configuration entry.
    pub async fn get_latest_hash(&self) -> Result<Option<String>, DieselError> {
        let mut conn = self.pool.get().await?;

        config_history::table
            .select(config_history::hash)
            .order(config_history::created_at.desc())
            .first::<String>(&mut conn)
            .await
            .optional()
    }

    /// Prune old entries to keep only the last MAX_HISTORY_ENTRIES.
    async fn prune_old_entries(&self) -> Result<(), DieselError> {
        let mut conn = self.pool.get().await?;

        // Get IDs to keep (most recent MAX_HISTORY_ENTRIES)
        let ids_to_keep: Vec<i32> = config_history::table
            .select(config_history::id)
            .order(config_history::created_at.desc())
            .limit(MAX_HISTORY_ENTRIES)
            .load(&mut conn)
            .await?;

        if !ids_to_keep.is_empty() {
            // Delete entries not in the keep list
            diesel::delete(config_history::table.filter(config_history::id.ne_all(&ids_to_keep)))
                .execute(&mut conn)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel_async::{AsyncConnection, SimpleAsyncConnection};
    use tempfile::tempdir;

    async fn setup_test_db() -> (AsyncSqlitePool, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_url = db_path.display().to_string();

        let pool = AsyncSqlitePool::new(&db_url, 5);
        let mut conn = pool.get().await.unwrap();

        conn.batch_execute(
            r#"CREATE TABLE IF NOT EXISTS config_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                format TEXT NOT NULL DEFAULT 'json',
                hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"#,
        )
        .await
        .unwrap();

        (pool, dir)
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
