//! Diesel-based service status repository.

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::models::ServiceStatusRecord;
use super::pool::{DbPool, DieselError};
use super::{parse_datetime, parse_datetime_opt};
use crate::models::{ServiceState, ServiceStatus, ServiceType};
use crate::schema::service_status;
use crate::with_conn;

/// Convert a database record to a domain model.
impl TryFrom<ServiceStatusRecord> for ServiceStatus {
    type Error = diesel::result::Error;

    fn try_from(record: ServiceStatusRecord) -> Result<Self, Self::Error> {
        let stats = serde_json::from_str(&record.stats)
            .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;

        Ok(ServiceStatus {
            id: record.id,
            service_type: ServiceType::from_str(&record.service_type).ok_or_else(|| {
                diesel::result::Error::DeserializationError(
                    format!("Invalid service_type: '{}'", record.service_type).into(),
                )
            })?,
            source_id: record.source_id,
            status: ServiceState::from_str(&record.status).ok_or_else(|| {
                diesel::result::Error::DeserializationError(
                    format!("Invalid service state: '{}'", record.status).into(),
                )
            })?,
            last_heartbeat: parse_datetime(&record.last_heartbeat),
            last_activity: parse_datetime_opt(record.last_activity),
            current_task: record.current_task,
            stats,
            started_at: parse_datetime(&record.started_at),
            host: record.host,
            version: record.version,
            last_error: record.last_error,
            last_error_at: parse_datetime_opt(record.last_error_at),
            error_count: record.error_count,
        })
    }
}

/// Diesel-based service status repository.
#[derive(Clone)]
pub struct DieselServiceStatusRepository {
    pool: DbPool,
}

#[allow(dead_code)]
impl DieselServiceStatusRepository {
    /// Create a new repository with an existing pool.
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Get all service statuses.
    pub async fn get_all(&self) -> Result<Vec<ServiceStatus>, DieselError> {
        with_conn!(self.pool, conn, {
            service_status::table
                .order(service_status::id.asc())
                .load::<ServiceStatusRecord>(&mut conn)
                .await
                .and_then(|records| records.into_iter().map(ServiceStatus::try_from).collect())
        })
    }

    /// Get service statuses by type.
    pub async fn get_by_type(&self, service_type: &str) -> Result<Vec<ServiceStatus>, DieselError> {
        with_conn!(self.pool, conn, {
            service_status::table
                .filter(service_status::service_type.eq(service_type))
                .order(service_status::id.asc())
                .load::<ServiceStatusRecord>(&mut conn)
                .await
                .and_then(|records| records.into_iter().map(ServiceStatus::try_from).collect())
        })
    }

    /// Get a service status by ID.
    pub async fn get(&self, id: &str) -> Result<Option<ServiceStatus>, DieselError> {
        with_conn!(self.pool, conn, {
            service_status::table
                .find(id)
                .first::<ServiceStatusRecord>(&mut conn)
                .await
                .optional()
                .and_then(|opt| opt.map(ServiceStatus::try_from).transpose())
        })
    }

    /// Upsert a service status (insert or update).
    pub async fn upsert(&self, status: &ServiceStatus) -> Result<(), DieselError> {
        use crate::repository::pool::build_sql;
        use crate::repository::sea_tables::ServiceStatusTable as Sst;
        use sea_query::{OnConflict, Query};

        let stats_json = serde_json::to_string(&status.stats).unwrap_or_else(|_| "{}".to_string());
        let last_heartbeat = status.last_heartbeat.to_rfc3339();
        let last_activity = status.last_activity.map(|dt| dt.to_rfc3339());
        let started_at = status.started_at.to_rfc3339();
        let last_error_at = status.last_error_at.map(|dt| dt.to_rfc3339());
        let service_type = status.service_type.as_str().to_string();
        let state = status.status.as_str().to_string();

        let stmt = Query::insert()
            .into_table(Sst::Table)
            .columns([
                Sst::Id,
                Sst::ServiceType,
                Sst::SourceId,
                Sst::Status,
                Sst::LastHeartbeat,
                Sst::LastActivity,
                Sst::CurrentTask,
                Sst::Stats,
                Sst::StartedAt,
                Sst::Host,
                Sst::Version,
                Sst::LastError,
                Sst::LastErrorAt,
                Sst::ErrorCount,
            ])
            .values_panic([
                status.id.clone().into(),
                service_type.clone().into(),
                status.source_id.clone().into(),
                state.clone().into(),
                last_heartbeat.clone().into(),
                last_activity.clone().into(),
                status.current_task.clone().into(),
                stats_json.clone().into(),
                started_at.clone().into(),
                status.host.clone().into(),
                status.version.clone().into(),
                status.last_error.clone().into(),
                last_error_at.clone().into(),
                status.error_count.into(),
            ])
            .on_conflict(
                OnConflict::column(Sst::Id)
                    .update_columns([
                        Sst::Status,
                        Sst::LastHeartbeat,
                        Sst::LastActivity,
                        Sst::CurrentTask,
                        Sst::Stats,
                        Sst::Host,
                        Sst::Version,
                        Sst::LastError,
                        Sst::LastErrorAt,
                        Sst::ErrorCount,
                    ])
                    .to_owned(),
            )
            .to_owned();

        let sql = build_sql(&self.pool, &stmt);

        with_conn!(self.pool, conn, {
            diesel::sql_query(&sql)
                .bind::<diesel::sql_types::Text, _>(&status.id)
                .bind::<diesel::sql_types::Text, _>(&service_type)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    status.source_id.as_deref(),
                )
                .bind::<diesel::sql_types::Text, _>(&state)
                .bind::<diesel::sql_types::Text, _>(&last_heartbeat)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    last_activity.as_deref(),
                )
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    status.current_task.as_deref(),
                )
                .bind::<diesel::sql_types::Text, _>(&stats_json)
                .bind::<diesel::sql_types::Text, _>(&started_at)
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    status.host.as_deref(),
                )
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    status.version.as_deref(),
                )
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    status.last_error.as_deref(),
                )
                .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                    last_error_at.as_deref(),
                )
                .bind::<diesel::sql_types::Integer, _>(status.error_count)
                .execute(&mut conn)
                .await?;
            Ok(())
        })
    }

    /// Delete a service status.
    pub async fn delete(&self, id: &str) -> Result<bool, DieselError> {
        with_conn!(self.pool, conn, {
            let rows = diesel::delete(service_status::table.find(id))
                .execute(&mut conn)
                .await?;
            Ok(rows > 0)
        })
    }

    /// Delete stale services (no heartbeat for given seconds).
    pub async fn cleanup_stale(&self, threshold_secs: i64) -> Result<usize, DieselError> {
        use chrono::Utc;
        let cutoff = (Utc::now() - chrono::Duration::seconds(threshold_secs)).to_rfc3339();

        with_conn!(self.pool, conn, {
            let rows = diesel::delete(
                service_status::table
                    .filter(service_status::last_heartbeat.lt(&cutoff))
                    .filter(service_status::status.eq("stopped")),
            )
            .execute(&mut conn)
            .await?;
            Ok(rows)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::diesel_context::DieselDbContext;
    use crate::repository::migrations;
    use tempfile::tempdir;

    async fn setup_test_db() -> (DieselDbContext, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let db_url = format!("sqlite:{}", db_path.display());
        migrations::run_migrations(&db_url, false).await.unwrap();
        let ctx = DieselDbContext::from_sqlite_path(&db_path).unwrap();
        (ctx, dir)
    }

    #[tokio::test]
    async fn test_upsert_and_get() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        let mut status = ServiceStatus::new_scraper("test-source");
        status.set_running(Some("Testing"));

        // Insert
        repo.upsert(&status).await.unwrap();

        // Get
        let retrieved = repo.get(&status.id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, "scraper:test-source");
        assert_eq!(retrieved.source_id, Some("test-source".to_string()));
        assert_eq!(retrieved.current_task, Some("Testing".to_string()));
    }

    #[tokio::test]
    async fn test_upsert_updates_existing() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        let mut status = ServiceStatus::new_scraper("test");
        status.set_running(Some("Task 1"));
        repo.upsert(&status).await.unwrap();

        // Update
        status.current_task = Some("Task 2".to_string());
        status.error_count = 5;
        repo.upsert(&status).await.unwrap();

        let retrieved = repo.get(&status.id).await.unwrap().unwrap();
        assert_eq!(retrieved.current_task, Some("Task 2".to_string()));
        assert_eq!(retrieved.error_count, 5);
    }

    #[tokio::test]
    async fn test_get_all() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        // Insert multiple
        let mut s1 = ServiceStatus::new_scraper("source1");
        s1.set_running(None);
        repo.upsert(&s1).await.unwrap();

        let mut s2 = ServiceStatus::new_scraper("source2");
        s2.set_running(None);
        repo.upsert(&s2).await.unwrap();

        let all = repo.get_all().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_get_by_type() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        let mut scraper = ServiceStatus::new_scraper("test");
        scraper.set_running(None);
        repo.upsert(&scraper).await.unwrap();

        let mut server = ServiceStatus::new_server();
        server.set_running(None);
        repo.upsert(&server).await.unwrap();

        let scrapers = repo.get_by_type("scraper").await.unwrap();
        assert_eq!(scrapers.len(), 1);
        assert_eq!(scrapers[0].id, "scraper:test");

        let servers = repo.get_by_type("server").await.unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].id, "server:main");
    }

    #[tokio::test]
    async fn test_delete() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        let mut status = ServiceStatus::new_scraper("to-delete");
        status.set_running(None);
        repo.upsert(&status).await.unwrap();

        // Verify it exists
        assert!(repo.get(&status.id).await.unwrap().is_some());

        // Delete
        let deleted = repo.delete(&status.id).await.unwrap();
        assert!(deleted);

        // Verify it's gone
        assert!(repo.get(&status.id).await.unwrap().is_none());

        // Delete non-existent returns false
        let deleted_again = repo.delete(&status.id).await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_cleanup_stale() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        // Insert a stopped service with old heartbeat
        let mut old_status = ServiceStatus::new_scraper("old");
        old_status.set_stopped();
        old_status.last_heartbeat = chrono::Utc::now() - chrono::Duration::seconds(120);
        repo.upsert(&old_status).await.unwrap();

        // Insert a running service (should not be cleaned up even if old)
        let mut running = ServiceStatus::new_scraper("running");
        running.set_running(None);
        running.last_heartbeat = chrono::Utc::now() - chrono::Duration::seconds(120);
        repo.upsert(&running).await.unwrap();

        // Insert a recent stopped service (should not be cleaned up)
        let mut recent = ServiceStatus::new_scraper("recent");
        recent.set_stopped();
        repo.upsert(&recent).await.unwrap();

        // Cleanup with 60 second threshold
        let cleaned = repo.cleanup_stale(60).await.unwrap();
        assert_eq!(cleaned, 1);

        // Verify correct services remain
        let all = repo.get_all().await.unwrap();
        assert_eq!(all.len(), 2);
        let ids: Vec<_> = all.iter().map(|s| s.id.as_str()).collect();
        assert!(ids.contains(&"scraper:running"));
        assert!(ids.contains(&"scraper:recent"));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        let result = repo.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_stats_persistence() {
        let (ctx, _dir) = setup_test_db().await;
        let repo = ctx.service_status();

        let mut status = ServiceStatus::new_scraper("test");
        status.update_scraper_stats(crate::models::ScraperStats {
            session_processed: 100,
            session_new: 50,
            session_errors: 2,
            rate_per_min: Some(12.5),
            queue_size: Some(500),
            browser_failures: None,
        });
        repo.upsert(&status).await.unwrap();

        let retrieved = repo.get(&status.id).await.unwrap().unwrap();
        let stats: crate::models::ScraperStats = serde_json::from_value(retrieved.stats).unwrap();
        assert_eq!(stats.session_processed, 100);
        assert_eq!(stats.session_new, 50);
        assert_eq!(stats.rate_per_min, Some(12.5));
    }

    #[tokio::test]
    async fn test_invalid_stats_json_returns_error() {
        let (ctx, _dir) = setup_test_db().await;

        // Insert a row with invalid JSON in the stats column via raw SQL
        use crate::repository::pool::DbPool;
        use diesel_async::SimpleAsyncConnection;
        match ctx.pool() {
            DbPool::Sqlite(ref sqlite_pool) => {
                let mut conn = sqlite_pool.get().await.unwrap();
                conn.batch_execute(
                    "INSERT INTO service_status (id, service_type, status, last_heartbeat, stats, started_at, error_count) \
                     VALUES ('bad:test', 'scraper', 'running', '2024-01-01T00:00:00Z', 'NOT JSON', '2024-01-01T00:00:00Z', 0)",
                )
                .await
                .unwrap();
            }
            #[cfg(feature = "postgres")]
            DbPool::Postgres(_) => unreachable!("test uses sqlite"),
        }

        let repo = ctx.service_status();
        let result = repo.get("bad:test").await;
        assert!(result.is_err());
        let err = format!("{:?}", result.unwrap_err());
        assert!(
            err.contains("Deserialization"),
            "Expected DeserializationError, got: {}",
            err,
        );
    }
}
