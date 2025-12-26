//! Tag-related handlers.

use axum::{
    extract::{Path, State},
    response::{Html, IntoResponse},
};

use super::super::templates;
use super::super::AppState;
use crate::models::DocumentDisplay;

/// List all tags with document counts.
pub async fn list_tags(State(state): State<AppState>) -> impl IntoResponse {
    let tags = match state.doc_repo.get_all_tags() {
        Ok(t) => t,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load tags: {}</p>", e),
                None,
            ));
        }
    };

    let content = templates::tags_list(&tags);
    Html(templates::base_template("Tags", &content, None))
}

/// List documents with a specific tag.
pub async fn list_tag_documents(
    State(state): State<AppState>,
    Path(tag): Path<String>,
) -> impl IntoResponse {
    let tag = urlencoding::decode(&tag)
        .unwrap_or(std::borrow::Cow::Borrowed(&tag))
        .to_string();

    let documents = match state.doc_repo.get_by_tag(&tag, None) {
        Ok(docs) => docs,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load documents: {}</p>", e),
                None,
            ));
        }
    };

    let doc_data: Vec<_> = documents
        .iter()
        .filter_map(|doc| DocumentDisplay::from_document(doc).map(|d| d.to_tuple()))
        .collect();

    let content = templates::tag_documents(&tag, &doc_data);
    Html(templates::base_template(
        &format!("Tag: {}", tag),
        &content,
        None,
    ))
}

/// API endpoint to get all tags as JSON.
pub async fn api_tags(State(state): State<AppState>) -> impl IntoResponse {
    let tags = state.stats_cache.get_all_tags().unwrap_or_else(|| {
        let tags = state.doc_repo.get_all_tags().unwrap_or_default();
        state.stats_cache.set_all_tags(tags.clone());
        tags
    });

    let tags_json: Vec<_> = tags
        .into_iter()
        .map(|(tag, count)| {
            serde_json::json!({
                "tag": tag,
                "count": count
            })
        })
        .collect();
    axum::Json(tags_json).into_response()
}
