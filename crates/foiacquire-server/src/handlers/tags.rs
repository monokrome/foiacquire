//! Tag-related handlers.

use askama::Template;
use axum::{
    extract::{Path, State},
    response::{Html, IntoResponse},
};

use super::super::template_structs::{
    DocumentRow, ErrorTemplate, TagDocumentsTemplate, TagWithCount, TagsTemplate,
};
use super::super::AppState;
use super::api_types::{ApiResponse, TagCount};

/// List all tags with document counts.
pub async fn list_tags(State(state): State<AppState>) -> impl IntoResponse {
    let tags = match state.doc_repo.get_all_tags().await {
        Ok(t) => t,
        Err(e) => {
            let msg = format!("Failed to load tags: {}", e);
            let template = ErrorTemplate {
                title: "Error",
                message: &msg,
            };
            return Html(template.render().unwrap_or(msg));
        }
    };

    let tags_with_counts: Vec<TagWithCount> =
        tags.into_iter().map(|t| TagWithCount::new(t, 0)).collect();

    let template = TagsTemplate {
        title: "Tags",
        has_tags: !tags_with_counts.is_empty(),
        tags: tags_with_counts,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

/// List documents with a specific tag.
pub async fn list_tag_documents(
    State(state): State<AppState>,
    Path(tag): Path<String>,
) -> impl IntoResponse {
    let tag = urlencoding::decode(&tag)
        .unwrap_or(std::borrow::Cow::Borrowed(&tag))
        .to_string();

    let documents = match state.doc_repo.get_by_tag(&tag, None).await {
        Ok(docs) => docs,
        Err(e) => {
            let msg = format!("Failed to load documents: {}", e);
            let template = ErrorTemplate {
                title: "Error",
                message: &msg,
            };
            return Html(template.render().unwrap_or(msg));
        }
    };

    let doc_rows: Vec<DocumentRow> = documents
        .iter()
        .filter_map(|doc| DocumentRow::from_document(doc).map(|row| row.with_other_tags(&tag)))
        .collect();

    let title = format!("Tag: {}", tag);
    let template = TagDocumentsTemplate {
        title: &title,
        tag: &tag,
        document_count: doc_rows.len(),
        documents: doc_rows,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

/// API endpoint to get all tags as JSON.
#[utoipa::path(
    get,
    path = "/api/tags",
    responses(
        (status = 200, description = "All tags with counts", body = Vec<TagCount>)
    ),
    tag = "Status"
)]
pub async fn api_tags(State(state): State<AppState>) -> impl IntoResponse {
    let tags: Vec<(String, usize)> = match state.stats_cache.get_all_tags() {
        Some(cached) => cached,
        None => {
            let raw_tags = state.doc_repo.get_all_tags().await.unwrap_or_default();
            let tags_with_counts: Vec<(String, usize)> =
                raw_tags.into_iter().map(|t| (t, 0)).collect();
            state.stats_cache.set_all_tags(tags_with_counts.clone());
            tags_with_counts
        }
    };

    let tags_list: Vec<TagCount> = tags
        .into_iter()
        .map(|(tag, count)| TagCount { tag, count })
        .collect();
    ApiResponse::ok(tags_list).into_response()
}
