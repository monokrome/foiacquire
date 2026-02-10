//! Annotations API endpoints for LLM-generated metadata.

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use super::super::AppState;
use super::api_types::{
    AnnotationListStats, AnnotationsListResponse, ApiResponse, UpdateAnnotationResponse,
};
use super::helpers::{internal_error, not_found};
use foiacquire::repository::diesel_document::BrowseParams;

/// Query params for annotations listing.
#[derive(Debug, Deserialize, IntoParams)]
pub struct AnnotationsQuery {
    /// Filter by source ID
    pub source: Option<String>,
    /// Filter to documents needing annotation
    pub needs_annotation: Option<bool>,
    /// Page number
    pub page: Option<usize>,
    /// Items per page
    pub per_page: Option<usize>,
}

/// Annotation response.
#[derive(Debug, Serialize, ToSchema)]
pub struct AnnotationResponse {
    pub document_id: String,
    pub title: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub has_annotation: bool,
}

/// Update annotation request.
#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateAnnotationRequest {
    pub synopsis: Option<String>,
    pub tags: Option<Vec<String>>,
}

/// List documents with their annotations.
#[utoipa::path(
    get,
    path = "/api/annotations",
    params(AnnotationsQuery),
    responses(
        (status = 200, description = "Annotations listing", body = AnnotationsListResponse)
    ),
    tag = "Annotations"
)]
pub async fn list_annotations(
    State(state): State<AppState>,
    Query(params): Query<AnnotationsQuery>,
) -> impl IntoResponse {
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    let page = params.page.unwrap_or(1).clamp(1, 100_000);

    let documents = if params.needs_annotation.unwrap_or(false) {
        state
            .doc_repo
            .get_needing_summarization(per_page)
            .await
            .unwrap_or_default()
    } else {
        let offset = page.saturating_sub(1) * per_page;
        state
            .doc_repo
            .browse(BrowseParams {
                source_id: params.source.as_deref(),
                limit: per_page as u32,
                offset: offset as u32,
                ..Default::default()
            })
            .await
            .unwrap_or_default()
    };

    let items: Vec<AnnotationResponse> = documents
        .into_iter()
        .map(|doc| AnnotationResponse {
            document_id: doc.id,
            title: doc.title,
            synopsis: doc.synopsis.clone(),
            tags: doc.tags.clone(),
            has_annotation: doc.synopsis.is_some() || !doc.tags.is_empty(),
        })
        .collect();

    let total_annotated = state
        .doc_repo
        .count_annotated(params.source.as_deref())
        .await
        .unwrap_or(0);
    let total_needing = state
        .doc_repo
        .count_needing_summarization(params.source.as_deref())
        .await
        .unwrap_or(0);

    ApiResponse::ok(AnnotationsListResponse {
        items,
        page,
        per_page,
        stats: AnnotationListStats {
            annotated: total_annotated,
            needing_annotation: total_needing,
        },
    })
    .into_response()
}

/// Get annotation for a specific document.
#[utoipa::path(
    get,
    path = "/api/annotations/{doc_id}",
    params(("doc_id" = String, Path, description = "Document ID")),
    responses(
        (status = 200, description = "Annotation details", body = AnnotationResponse),
        (status = 404, description = "Document not found")
    ),
    tag = "Annotations"
)]
pub async fn get_annotation(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
) -> impl IntoResponse {
    match state.doc_repo.get(&doc_id).await {
        Ok(Some(doc)) => ApiResponse::ok(AnnotationResponse {
            document_id: doc.id,
            title: doc.title,
            synopsis: doc.synopsis,
            tags: doc.tags,
            has_annotation: true,
        })
        .into_response(),
        Ok(None) => not_found("Document not found").into_response(),
        Err(e) => internal_error(e).into_response(),
    }
}

/// Update annotation for a document.
#[utoipa::path(
    put,
    path = "/api/annotations/{doc_id}",
    params(("doc_id" = String, Path, description = "Document ID")),
    request_body = UpdateAnnotationRequest,
    responses(
        (status = 200, description = "Updated annotation", body = UpdateAnnotationResponse),
        (status = 404, description = "Document not found")
    ),
    tag = "Annotations"
)]
pub async fn update_annotation(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
    Json(body): Json<UpdateAnnotationRequest>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id).await {
        Ok(Some(d)) => d,
        Ok(None) => return not_found("Document not found").into_response(),
        Err(e) => return internal_error(e).into_response(),
    };

    let synopsis = body.synopsis.or(doc.synopsis);
    let tags = body.tags.unwrap_or(doc.tags);

    if let Err(e) = state
        .doc_repo
        .update_synopsis_and_tags(&doc_id, synopsis.as_deref(), &tags)
        .await
    {
        return internal_error(e).into_response();
    }

    ApiResponse::ok(UpdateAnnotationResponse {
        document_id: doc_id,
        synopsis,
        tags,
        updated: true,
    })
    .into_response()
}

/// Annotation stats response.
#[derive(Debug, Serialize, ToSchema)]
pub struct AnnotationStats {
    pub total_documents: u64,
    pub annotated: u64,
    pub needing_annotation: u64,
    pub by_source: Vec<SourceAnnotationStats>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SourceAnnotationStats {
    pub source_id: String,
    pub annotated: u64,
    pub needing_annotation: u64,
}

/// Get annotation statistics.
#[utoipa::path(
    get,
    path = "/api/annotations/stats",
    responses(
        (status = 200, description = "Annotation statistics", body = AnnotationStats)
    ),
    tag = "Annotations"
)]
pub async fn annotation_stats(State(state): State<AppState>) -> impl IntoResponse {
    let total = state.doc_repo.count().await.unwrap_or(0);
    let annotated = state.doc_repo.count_annotated(None).await.unwrap_or(0);
    let needing = state
        .doc_repo
        .count_needing_summarization(None)
        .await
        .unwrap_or(0);

    let source_counts = state
        .doc_repo
        .get_all_source_counts()
        .await
        .unwrap_or_default();

    let mut by_source = Vec::new();
    for (source_id, _count) in source_counts {
        let annotated = state
            .doc_repo
            .count_annotated(Some(&source_id))
            .await
            .unwrap_or(0);
        let needing = state
            .doc_repo
            .count_needing_summarization(Some(&source_id))
            .await
            .unwrap_or(0);
        by_source.push(SourceAnnotationStats {
            source_id,
            annotated,
            needing_annotation: needing,
        });
    }

    ApiResponse::ok(AnnotationStats {
        total_documents: total,
        annotated,
        needing_annotation: needing,
        by_source,
    })
    .into_response()
}
