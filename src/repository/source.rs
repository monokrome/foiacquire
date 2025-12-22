//! Source repository for SQLite persistence.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};

use super::{parse_datetime, parse_datetime_opt, to_option, Result};
use crate::models::{Source, SourceType};

/// SQLite-backed source repository.
pub struct SourceRepository {
    db_path: PathBuf,
}

impl SourceRepository {
    /// Create a new source repository.
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
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_scraped TEXT
            );
        "#,
        )?;
        Ok(())
    }

    /// Get a source by ID.
    pub fn get(&self, id: &str) -> Result<Option<Source>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT * FROM sources WHERE id = ?")?;

        to_option(stmt.query_row(params![id], |row| {
            Ok(Source {
                id: row.get("id")?,
                source_type: SourceType::from_str(&row.get::<_, String>("source_type")?)
                    .unwrap_or(SourceType::Custom),
                name: row.get("name")?,
                base_url: row.get("base_url")?,
                metadata: serde_json::from_str(&row.get::<_, String>("metadata")?)
                    .unwrap_or_default(),
                created_at: parse_datetime(&row.get::<_, String>("created_at")?),
                last_scraped: parse_datetime_opt(row.get::<_, Option<String>>("last_scraped")?),
            })
        }))
    }

    /// Get all sources.
    pub fn get_all(&self) -> Result<Vec<Source>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT * FROM sources")?;

        let sources = stmt
            .query_map([], |row| {
                Ok(Source {
                    id: row.get("id")?,
                    source_type: SourceType::from_str(&row.get::<_, String>("source_type")?)
                        .unwrap_or(SourceType::Custom),
                    name: row.get("name")?,
                    base_url: row.get("base_url")?,
                    metadata: serde_json::from_str(&row.get::<_, String>("metadata")?)
                        .unwrap_or_default(),
                    created_at: parse_datetime(&row.get::<_, String>("created_at")?),
                    last_scraped: parse_datetime_opt(row.get::<_, Option<String>>("last_scraped")?),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(sources)
    }

    /// Save a source.
    pub fn save(&self, source: &Source) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            r#"
            INSERT INTO sources (id, source_type, name, base_url, metadata, created_at, last_scraped)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
                source_type = excluded.source_type,
                name = excluded.name,
                base_url = excluded.base_url,
                metadata = excluded.metadata,
                last_scraped = excluded.last_scraped
            "#,
            params![
                source.id,
                source.source_type.as_str(),
                source.name,
                source.base_url,
                serde_json::to_string(&source.metadata)?,
                source.created_at.to_rfc3339(),
                source.last_scraped.map(|dt| dt.to_rfc3339()),
            ],
        )?;

        Ok(())
    }

    /// Delete a source.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let conn = self.connect()?;
        let rows = conn.execute("DELETE FROM sources WHERE id = ?", params![id])?;
        Ok(rows > 0)
    }

    /// Check if a source exists.
    pub fn exists(&self, id: &str) -> Result<bool> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sources WHERE id = ?",
            params![id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Update last scraped timestamp.
    pub fn update_last_scraped(&self, id: &str, timestamp: DateTime<Utc>) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE sources SET last_scraped = ? WHERE id = ?",
            params![timestamp.to_rfc3339(), id],
        )?;
        Ok(())
    }
}
