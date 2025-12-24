//! Configuration history repository for tracking config changes.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::{parse_datetime, Result};

/// Maximum number of configuration history entries to retain.
const MAX_HISTORY_ENTRIES: usize = 16;

/// Represents a stored configuration entry.
#[derive(Debug, Clone)]
pub struct ConfigHistoryEntry {
    pub uuid: String,
    pub created_at: DateTime<Utc>,
    pub data: String,
    pub format: String,
    pub hash: String,
}

/// SQLite-backed configuration history repository.
pub struct ConfigHistoryRepository {
    db_path: PathBuf,
}

impl ConfigHistoryRepository {
    /// Create a new configuration history repository.
    pub fn new(db_path: &Path) -> Result<Self> {
        let repo = Self {
            db_path: db_path.to_path_buf(),
        };
        repo.init_schema()?;
        Ok(repo)
    }

    fn connect(&self) -> Result<Connection> {
        super::connect(&self.db_path)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS configuration_history (
                uuid TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                data TEXT NOT NULL,
                format TEXT NOT NULL,
                hash TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_config_history_created_at
                ON configuration_history(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_config_history_hash
                ON configuration_history(hash);
        "#,
        )?;
        Ok(())
    }

    /// Check if a config with the given hash already exists.
    pub fn hash_exists(&self, hash: &str) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM configuration_history WHERE hash = ?",
            params![hash],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Insert a new configuration entry if the hash doesn't already exist.
    /// Returns true if inserted, false if hash already exists.
    pub fn insert_if_new(
        &self,
        data: &str,
        format: &str,
        hash: &str,
    ) -> Result<bool> {
        if self.hash_exists(hash)? {
            return Ok(false);
        }

        let conn = self.connect()?;
        let uuid = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        conn.execute(
            r#"
            INSERT INTO configuration_history (uuid, created_at, data, format, hash)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![uuid, now, data, format, hash],
        )?;

        // Prune old entries to keep only the last MAX_HISTORY_ENTRIES
        self.prune_old_entries(&conn)?;

        Ok(true)
    }

    /// Get the most recent configuration entry.
    pub fn get_latest(&self) -> Result<Option<ConfigHistoryEntry>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT uuid, created_at, data, format, hash
             FROM configuration_history
             ORDER BY created_at DESC
             LIMIT 1",
        )?;

        let result = stmt.query_row([], |row| {
            Ok(ConfigHistoryEntry {
                uuid: row.get("uuid")?,
                created_at: parse_datetime(&row.get::<_, String>("created_at")?),
                data: row.get("data")?,
                format: row.get("format")?,
                hash: row.get("hash")?,
            })
        });

        match result {
            Ok(entry) => Ok(Some(entry)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get all configuration history entries (most recent first).
    pub fn get_all(&self) -> Result<Vec<ConfigHistoryEntry>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT uuid, created_at, data, format, hash
             FROM configuration_history
             ORDER BY created_at DESC",
        )?;

        let entries = stmt
            .query_map([], |row| {
                Ok(ConfigHistoryEntry {
                    uuid: row.get("uuid")?,
                    created_at: parse_datetime(&row.get::<_, String>("created_at")?),
                    data: row.get("data")?,
                    format: row.get("format")?,
                    hash: row.get("hash")?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(entries)
    }

    /// Prune old entries to keep only the last MAX_HISTORY_ENTRIES.
    fn prune_old_entries(&self, conn: &Connection) -> Result<()> {
        conn.execute(
            r#"
            DELETE FROM configuration_history
            WHERE uuid NOT IN (
                SELECT uuid FROM configuration_history
                ORDER BY created_at DESC
                LIMIT ?
            )
            "#,
            params![MAX_HISTORY_ENTRIES],
        )?;
        Ok(())
    }
}
