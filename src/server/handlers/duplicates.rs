//! Duplicate document detection handlers.

use axum::{
    extract::State,
    response::{Html, IntoResponse},
};
use std::collections::HashMap;

use super::super::templates;
use super::super::AppState;

/// List documents that exist in multiple sources.
pub async fn list_duplicates(State(state): State<AppState>) -> impl IntoResponse {
    let hashes = match state.doc_repo.get_content_hashes() {
        Ok(h) => h,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load documents: {}</p>", e),
                None,
            ));
        }
    };

    let mut hash_to_docs: HashMap<String, Vec<(String, String, String)>> = HashMap::new();

    for (doc_id, source_id, content_hash, title) in hashes {
        hash_to_docs
            .entry(content_hash)
            .or_default()
            .push((doc_id, source_id, title));
    }

    let duplicates: Vec<_> = hash_to_docs
        .into_iter()
        .filter(|(_, docs)| {
            let unique_sources: std::collections::HashSet<_> =
                docs.iter().map(|(_, source, _)| source).collect();
            unique_sources.len() > 1
        })
        .collect();

    let content = templates::duplicates_list(&duplicates);
    Html(templates::base_template(
        "Cross-Source Duplicates",
        &content,
        None,
    ))
}
