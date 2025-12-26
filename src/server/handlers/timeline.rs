//! Timeline-related handlers.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};

use super::super::AppState;
use super::helpers::{build_timeline_from_summaries, DateRangeParams, TimelineResponse};

/// Timeline aggregate across all sources.
pub async fn timeline_aggregate(
    State(state): State<AppState>,
    Query(_params): Query<DateRangeParams>,
) -> impl IntoResponse {
    let summaries = match state.doc_repo.get_all_summaries() {
        Ok(s) => s,
        Err(e) => {
            return axum::Json(TimelineResponse {
                buckets: vec![],
                total: 0,
                error: Some(e.to_string()),
            });
        }
    };

    let timeline = build_timeline_from_summaries(&summaries);
    axum::Json(timeline)
}

/// Timeline for a specific source.
pub async fn timeline_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Query(_params): Query<DateRangeParams>,
) -> impl IntoResponse {
    let summaries = match state.doc_repo.get_summaries_by_source(&source_id) {
        Ok(s) => s,
        Err(e) => {
            return axum::Json(TimelineResponse {
                buckets: vec![],
                total: 0,
                error: Some(e.to_string()),
            });
        }
    };

    let timeline = build_timeline_from_summaries(&summaries);
    axum::Json(timeline)
}
