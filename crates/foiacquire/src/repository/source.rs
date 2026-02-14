//! Source repository for managing FOIA sources.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::models::SourceRecord;
use super::pool::{DbError, DbPool};
use crate::models::Source;
use crate::schema::sources;
use crate::with_conn;

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
                        Sources::CreatedAt,
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
