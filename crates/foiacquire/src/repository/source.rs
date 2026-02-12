//! Source repository for managing FOIA sources.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::models::SourceRecord;
use super::pool::{DbError, DbPool};
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{Source, SourceType};
use crate::schema::sources;
use crate::with_conn;

#[cfg(feature = "postgres")]
use crate::with_conn_split;

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

/// Source repository.
#[derive(Clone)]
pub struct SourceRepository {
    pool: DbPool,
}

#[allow(dead_code)]
impl SourceRepository {
    /// Create a new source repository.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Get a source by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Source>, DbError> {
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
    pub async fn get_all(&self) -> Result<Vec<Source>, DbError> {
        with_conn!(self.pool, conn, {
            sources::table
                .load::<SourceRecord>(&mut conn)
                .await
                .and_then(|records| records.into_iter().map(Source::try_from).collect())
        })
    }

    /// Save a source (insert or update).
    pub async fn save(&self, source: &Source) -> Result<(), DbError> {
        let metadata_json =
            serde_json::to_string(&source.metadata).unwrap_or_else(|_| "{}".to_string());
        let created_at = source.created_at.to_rfc3339();
        let last_scraped = source.last_scraped.map(|dt| dt.to_rfc3339());
        let source_type = source.source_type.as_str().to_string();

        #[cfg(not(feature = "postgres"))]
        {
            with_conn!(self.pool, conn, {
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
            })
        }

        #[cfg(feature = "postgres")]
        {
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
                    diesel::sql_query(
                        "INSERT INTO sources (id, source_type, name, base_url, metadata, created_at, last_scraped)
                         VALUES ($1, $2, $3, $4, $5, $6, $7)
                         ON CONFLICT (id) DO UPDATE SET
                            source_type = EXCLUDED.source_type,
                            name = EXCLUDED.name,
                            base_url = EXCLUDED.base_url,
                            metadata = EXCLUDED.metadata,
                            created_at = EXCLUDED.created_at,
                            last_scraped = EXCLUDED.last_scraped"
                    )
                    .bind::<diesel::sql_types::Text, _>(&source.id)
                    .bind::<diesel::sql_types::Text, _>(&source_type)
                    .bind::<diesel::sql_types::Text, _>(&source.name)
                    .bind::<diesel::sql_types::Text, _>(&source.base_url)
                    .bind::<diesel::sql_types::Text, _>(&metadata_json)
                    .bind::<diesel::sql_types::Text, _>(&created_at)
                    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&last_scraped)
                    .execute(&mut conn)
                    .await?;
                    Ok(())
                }
            )
        }
    }

    /// Delete a source.
    #[allow(dead_code)]
    pub async fn delete(&self, id: &str) -> Result<bool, DbError> {
        with_conn!(self.pool, conn, {
            let rows = diesel::delete(sources::table.find(id))
                .execute(&mut conn)
                .await?;
            Ok(rows > 0)
        })
    }

    /// Check if a source exists.
    pub async fn exists(&self, id: &str) -> Result<bool, DbError> {
        with_conn!(self.pool, conn, {
            use diesel::dsl::count_star;
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
    ) -> Result<(), DbError> {
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
    pub async fn rename(&self, old_id: &str, new_id: &str) -> Result<(usize, usize), DbError> {
        #[cfg(not(feature = "postgres"))]
        {
            with_conn!(self.pool, conn, {
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
            })
        }

        #[cfg(feature = "postgres")]
        {
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
}
