//! Source repository for SQLite persistence.
//!
//! This module contains both sync (rusqlite) and async (sqlx) implementations.
//! The sync version is used by existing code, async is for new code.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};

use super::{parse_datetime, parse_datetime_opt, Result};
use crate::models::{Source, SourceType};

// ============================================================================
// SYNC (rusqlite) implementation - used by existing code
// ============================================================================

use rusqlite::{params, Connection};

/// SQLite-backed source repository (sync).
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

        super::to_option(stmt.query_row(params![id], |row| {
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

// ============================================================================
// ASYNC (sqlx) implementation - for new code and gradual migration
// ============================================================================

use sqlx::sqlite::SqlitePool;

/// Row type for SQLx query mapping.
#[derive(sqlx::FromRow)]
struct SourceRow {
    id: String,
    source_type: String,
    name: String,
    base_url: String,
    metadata: String,
    created_at: String,
    last_scraped: Option<String>,
}

impl From<SourceRow> for Source {
    fn from(row: SourceRow) -> Self {
        Source {
            id: row.id,
            source_type: SourceType::from_str(&row.source_type).unwrap_or(SourceType::Custom),
            name: row.name,
            base_url: row.base_url,
            metadata: serde_json::from_str(&row.metadata).unwrap_or_default(),
            created_at: parse_datetime(&row.created_at),
            last_scraped: parse_datetime_opt(row.last_scraped),
        }
    }
}

/// Async SQLx-backed source repository.
#[derive(Clone)]
pub struct AsyncSourceRepository {
    pool: SqlitePool,
}

impl AsyncSourceRepository {
    /// Create a new async source repository with an existing pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Get a source by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Source>> {
        let row = sqlx::query_as!(
            SourceRow,
            r#"SELECT
                id as "id!",
                source_type as "source_type!",
                name as "name!",
                base_url as "base_url!",
                metadata as "metadata!",
                created_at as "created_at!",
                last_scraped
               FROM sources WHERE id = ?"#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Source::from))
    }

    /// Get all sources.
    pub async fn get_all(&self) -> Result<Vec<Source>> {
        let rows = sqlx::query_as!(
            SourceRow,
            r#"SELECT
                id as "id!",
                source_type as "source_type!",
                name as "name!",
                base_url as "base_url!",
                metadata as "metadata!",
                created_at as "created_at!",
                last_scraped
               FROM sources"#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Source::from).collect())
    }

    /// Save a source (insert or update).
    pub async fn save(&self, source: &Source) -> Result<()> {
        let metadata_json = serde_json::to_string(&source.metadata)?;
        let created_at = source.created_at.to_rfc3339();
        let last_scraped = source.last_scraped.map(|dt| dt.to_rfc3339());
        let source_type = source.source_type.as_str();

        sqlx::query!(
            r#"INSERT INTO sources (id, source_type, name, base_url, metadata, created_at, last_scraped)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
               ON CONFLICT(id) DO UPDATE SET
                   source_type = excluded.source_type,
                   name = excluded.name,
                   base_url = excluded.base_url,
                   metadata = excluded.metadata,
                   last_scraped = excluded.last_scraped"#,
            source.id,
            source_type,
            source.name,
            source.base_url,
            metadata_json,
            created_at,
            last_scraped
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete a source.
    pub async fn delete(&self, id: &str) -> Result<bool> {
        let result = sqlx::query!("DELETE FROM sources WHERE id = ?", id)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Check if a source exists.
    pub async fn exists(&self, id: &str) -> Result<bool> {
        let count: i32 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i32" FROM sources WHERE id = ?"#,
            id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count > 0)
    }

    /// Update last scraped timestamp.
    pub async fn update_last_scraped(&self, id: &str, timestamp: DateTime<Utc>) -> Result<()> {
        let ts = timestamp.to_rfc3339();

        sqlx::query!("UPDATE sources SET last_scraped = ? WHERE id = ?", ts, id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
