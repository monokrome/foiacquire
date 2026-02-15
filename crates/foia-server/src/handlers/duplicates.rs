//! Duplicate document detection handlers.

use askama::Template;
use axum::{
    extract::State,
    response::{Html, IntoResponse},
};
use std::collections::HashMap;

use super::super::template_structs::{
    DuplicateDoc, DuplicateGroup, DuplicatesTemplate, ErrorTemplate,
};
use super::super::AppState;

/// List documents that exist in multiple sources.
pub async fn list_duplicates(State(state): State<AppState>) -> impl IntoResponse {
    let hashes = match state.doc_repo.get_content_hashes().await {
        Ok(h) => h,
        Err(e) => {
            let msg = format!("Failed to load documents: {}", e);
            let template = ErrorTemplate {
                title: "Error",
                message: &msg,
            };
            return Html(template.render().unwrap_or(msg));
        }
    };

    let mut hash_to_docs: HashMap<String, Vec<(String, String, String)>> = HashMap::new();

    for (doc_id, source_id, content_hash, title) in hashes {
        hash_to_docs
            .entry(content_hash)
            .or_default()
            .push((doc_id, source_id, title));
    }

    let duplicates: Vec<DuplicateGroup> = hash_to_docs
        .into_iter()
        .filter(|(_, docs)| {
            let unique_sources: std::collections::HashSet<_> =
                docs.iter().map(|(_, source, _)| source).collect();
            unique_sources.len() > 1
        })
        .map(|(content_hash, docs)| DuplicateGroup {
            hash_prefix: content_hash.chars().take(16).collect(),
            docs: docs
                .into_iter()
                .map(|(id, source_id, title)| DuplicateDoc {
                    id,
                    title,
                    source_id,
                })
                .collect(),
        })
        .collect();

    let template = DuplicatesTemplate {
        title: "Cross-Source Duplicates",
        has_duplicates: !duplicates.is_empty(),
        duplicates,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}
