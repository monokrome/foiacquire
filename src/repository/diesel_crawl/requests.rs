//! Request logging operations for the crawl repository.

use diesel::prelude::*;
use diesel_async::RunQueryDsl;

#[cfg(feature = "postgres")]
use super::LastInsertId;
use super::{DieselCrawlRepository, LastInsertRowId};
use crate::models::CrawlRequest;
use crate::repository::pool::{DbPool, DieselError};
use crate::schema::crawl_requests;
use crate::with_conn;

impl DieselCrawlRepository {
    /// Log a completed request.
    pub async fn log_request(&self, request: &CrawlRequest) -> Result<i64, DieselError> {
        let request_headers =
            serde_json::to_string(&request.request_headers).unwrap_or_else(|_| "{}".to_string());
        let response_headers =
            serde_json::to_string(&request.response_headers).unwrap_or_else(|_| "{}".to_string());
        let request_at = request.request_at.to_rfc3339();
        let response_at = request.response_at.map(|dt| dt.to_rfc3339());
        let response_status = request.response_status.map(|s| s as i32);
        let response_size = request.response_size.map(|s| s as i32);
        let duration_ms = request.duration_ms.map(|d| d as i32);
        let was_conditional = if request.was_conditional { 1i32 } else { 0 };
        let was_not_modified = if request.was_not_modified { 1i32 } else { 0 };

        with_conn!(self.pool, conn, {
            diesel::insert_into(crawl_requests::table)
                .values((
                    crawl_requests::source_id.eq(&request.source_id),
                    crawl_requests::url.eq(&request.url),
                    crawl_requests::method.eq(&request.method),
                    crawl_requests::request_headers.eq(&request_headers),
                    crawl_requests::request_at.eq(&request_at),
                    crawl_requests::response_status.eq(&response_status),
                    crawl_requests::response_headers.eq(&response_headers),
                    crawl_requests::response_at.eq(&response_at),
                    crawl_requests::response_size.eq(&response_size),
                    crawl_requests::duration_ms.eq(&duration_ms),
                    crawl_requests::error.eq(&request.error),
                    crawl_requests::was_conditional.eq(was_conditional),
                    crawl_requests::was_not_modified.eq(was_not_modified),
                ))
                .execute(&mut conn)
                .await?;

            // Get the last inserted ID based on database type
            let id: i64 = match &self.pool {
                DbPool::Sqlite(_) => {
                    let result: LastInsertRowId = diesel::sql_query("SELECT last_insert_rowid()")
                        .get_result(&mut conn)
                        .await?;
                    result.id
                }
                #[cfg(feature = "postgres")]
                DbPool::Postgres(_) => {
                    let result: LastInsertId = diesel::sql_query("SELECT lastval()::integer as id")
                        .get_result(&mut conn)
                        .await?;
                    result.id as i64
                }
            };

            Ok(id)
        })
    }
}
