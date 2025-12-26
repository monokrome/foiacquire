//! Source-related HTTP handlers.

use axum::{
    extract::{Path, Query, State},
    response::{Html, IntoResponse},
};

use super::super::templates;
use super::super::AppState;
use super::helpers::{build_timeline_data, find_cross_source_duplicates, DateRangeParams};

/// Index/home page - redirects to browse.
pub async fn index() -> impl IntoResponse {
    axum::response::Redirect::to("/browse")
}

/// List all sources.
pub async fn list_sources(State(state): State<AppState>) -> impl IntoResponse {
    let sources = match state.source_repo.get_all() {
        Ok(s) => s,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load sources: {}</p>", e),
                None,
            ));
        }
    };

    let counts = state.doc_repo.get_all_source_counts().unwrap_or_default();

    let source_data: Vec<_> = sources
        .into_iter()
        .map(|source| {
            let count = counts.get(&source.id).copied().unwrap_or(0);
            (source.id, source.name, count, source.last_scraped)
        })
        .collect();

    let content = templates::sources_list(&source_data);
    Html(templates::base_template("Sources", &content, None))
}

/// List documents for a specific source.
pub async fn list_source_documents(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Query(_params): Query<DateRangeParams>,
) -> impl IntoResponse {
    let source = match state.source_repo.get(&source_id) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Html(templates::base_template(
                "Not Found",
                "<p>Source not found.</p>",
                None,
            ));
        }
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load source: {}</p>", e),
                None,
            ));
        }
    };

    let documents = match state.doc_repo.get_by_source(&source_id) {
        Ok(docs) => docs,
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load documents: {}</p>", e),
                None,
            ));
        }
    };

    let timeline = build_timeline_data(&documents);
    let timeline_json = serde_json::to_string(&timeline).unwrap_or_default();

    let duplicates = find_cross_source_duplicates(&state, &documents);

    let doc_data: Vec<_> = documents
        .iter()
        .filter_map(|doc| {
            let version = doc.current_version()?;
            let other_sources = duplicates
                .get(&version.content_hash)
                .map(|sources| {
                    sources
                        .iter()
                        .filter(|s| *s != &source_id)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();

            let display_name = version
                .original_filename
                .clone()
                .unwrap_or_else(|| doc.title.clone());

            Some((
                doc.id.clone(),
                display_name,
                version.mime_type.clone(),
                version.file_size,
                version.acquired_at,
                other_sources,
            ))
        })
        .collect();

    let content = templates::document_list(&source.name, &doc_data);
    Html(templates::base_template(
        &source.name,
        &content,
        Some(&timeline_json),
    ))
}
