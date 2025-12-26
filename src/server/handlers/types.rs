//! Document type handlers.

use axum::{
    extract::{Path, Query, State},
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::templates;
use super::super::AppState;
use super::helpers::mime_to_category;
use crate::models::DocumentDisplay;

/// Filter parameters for type listing.
#[derive(Debug, Deserialize)]
pub struct TypeFilterParams {
    pub limit: Option<usize>,
    pub source: Option<String>,
}

/// List all type categories.
pub async fn list_types(State(state): State<AppState>) -> impl IntoResponse {
    let type_stats = match state.doc_repo.get_type_stats(None) {
        Ok(stats) => stats,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load type stats: {}</p>", e),
                None,
            ));
        }
    };

    let stats_with_category: Vec<_> = type_stats
        .iter()
        .map(|(mime, count)| (mime_to_category(mime).to_string(), mime.clone(), *count))
        .collect();

    let content = templates::types_list(&stats_with_category);
    Html(templates::base_template("Document Types", &content, None))
}

/// List documents filtered by type.
pub async fn list_by_type(
    State(state): State<AppState>,
    Path(type_name): Path<String>,
    Query(params): Query<TypeFilterParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(500).clamp(1, 1000);
    let source_id = params.source.as_deref();

    let documents = match state
        .doc_repo
        .get_by_type_category(&type_name, source_id, limit)
    {
        Ok(docs) => docs,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load documents: {}</p>", e),
                None,
            ));
        }
    };

    let category_stats: Option<Vec<(String, u64)>> =
        state.doc_repo.get_type_stats(None).ok().map(|stats| {
            let mut cat_counts: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            for (mime, count) in stats {
                let cat = mime_to_category(&mime).to_string();
                *cat_counts.entry(cat).or_default() += count;
            }
            cat_counts.into_iter().collect()
        });

    let doc_data: Vec<_> = documents
        .iter()
        .filter_map(|doc| DocumentDisplay::from_document(doc).map(|d| d.to_tuple()))
        .collect();

    let content = templates::type_documents(&type_name, &doc_data, category_stats.as_deref());
    Html(templates::base_template(
        &format!("Type: {}", type_name),
        &content,
        None,
    ))
}
