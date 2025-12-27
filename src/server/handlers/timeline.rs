//! Timeline-related handlers.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};

use super::super::AppState;
use super::helpers::{DateRangeParams, TimelineResponse};

/// Timeline aggregate across all sources.
pub async fn timeline_aggregate(
    State(state): State<AppState>,
    Query(_params): Query<DateRangeParams>,
) -> impl IntoResponse {
    match state.doc_repo.get_all_summaries().await {
        Ok(summaries) => {
            // Simple timeline: count by acquired_at date
            let total = summaries.len() as u64;
            axum::Json(TimelineResponse {
                buckets: vec![], // TODO: implement bucketing
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
    Query(_params): Query<DateRangeParams>,
) -> impl IntoResponse {
    match state.doc_repo.get_summaries_by_source(&source_id).await {
        Ok(summaries) => {
            let total = summaries.len() as u64;
            axum::Json(TimelineResponse {
                buckets: vec![], // TODO: implement bucketing
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
