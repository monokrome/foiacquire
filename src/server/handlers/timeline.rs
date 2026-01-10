//! Timeline-related handlers.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};

use super::super::AppState;
use super::helpers::{DateRangeParams, TimelineBucket, TimelineResponse};

/// Timeline aggregate across all sources.
pub async fn timeline_aggregate(
    State(state): State<AppState>,
    Query(params): Query<DateRangeParams>,
) -> impl IntoResponse {
    match state
        .doc_repo
        .get_timeline_buckets(None, params.start.as_deref(), params.end.as_deref())
        .await
    {
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
            axum::Json(TimelineResponse {
                buckets,
                total,
                error: None,
            })
        }
        Err(e) => axum::Json(TimelineResponse {
            buckets: vec![],
            total: 0,
            error: Some(e.to_string()),
        }),
    }
}

/// Timeline for a specific source.
pub async fn timeline_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Query(params): Query<DateRangeParams>,
) -> impl IntoResponse {
    match state
        .doc_repo
        .get_timeline_buckets(
            Some(&source_id),
            params.start.as_deref(),
            params.end.as_deref(),
        )
        .await
    {
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
            axum::Json(TimelineResponse {
                buckets,
                total,
                error: None,
            })
        }
        Err(e) => axum::Json(TimelineResponse {
            buckets: vec![],
            total: 0,
            error: Some(e.to_string()),
        }),
    }
}
