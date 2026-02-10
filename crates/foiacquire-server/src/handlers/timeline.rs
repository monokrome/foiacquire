//! Timeline-related handlers.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};

use super::super::AppState;
use super::helpers::{DateRangeParams, TimelineBucket, TimelineResponse};

fn timeline_response<E: std::fmt::Display>(
    result: Result<Vec<(String, i64, u64)>, E>,
) -> Json<TimelineResponse> {
    match result {
        Ok(raw_buckets) => {
            let total: u64 = raw_buckets.iter().map(|(_, _, count)| count).sum();
            let buckets: Vec<TimelineBucket> = raw_buckets
                .into_iter()
                .map(|(date, timestamp, count)| TimelineBucket {
                    date,
                    timestamp,
                    count,
                })
                .collect();
            Json(TimelineResponse {
                buckets,
                total,
                error: None,
            })
        }
        Err(e) => Json(TimelineResponse {
            buckets: vec![],
            total: 0,
            error: Some(e.to_string()),
        }),
    }
}

/// Timeline aggregate across all sources.
#[utoipa::path(
    get,
    path = "/api/timeline",
    params(DateRangeParams),
    responses(
        (status = 200, description = "Aggregated timeline data", body = TimelineResponse)
    ),
    tag = "Timeline"
)]
pub async fn timeline_aggregate(
    State(state): State<AppState>,
    Query(params): Query<DateRangeParams>,
) -> impl IntoResponse {
    let result = state
        .doc_repo
        .get_timeline_buckets(None, params.start.as_deref(), params.end.as_deref())
        .await;
    timeline_response(result)
}

/// Timeline for a specific source.
#[utoipa::path(
    get,
    path = "/api/timeline/{source_id}",
    params(
        ("source_id" = String, Path, description = "Source ID"),
        DateRangeParams,
    ),
    responses(
        (status = 200, description = "Source-specific timeline data", body = TimelineResponse)
    ),
    tag = "Timeline"
)]
pub async fn timeline_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Query(params): Query<DateRangeParams>,
) -> impl IntoResponse {
    let result = state
        .doc_repo
        .get_timeline_buckets(
            Some(&source_id),
            params.start.as_deref(),
            params.end.as_deref(),
        )
        .await;
    timeline_response(result)
}
