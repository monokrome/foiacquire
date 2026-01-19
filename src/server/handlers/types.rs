//! Document type handlers.

use askama::Template;
use axum::{
    extract::{Path, Query, State},
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::template_structs::{
    CategoryWithCount, DocumentRow, ErrorTemplate, TypeDocumentsTemplate, TypeStat, TypesTemplate,
};
use super::super::AppState;
use crate::utils::{mime_to_category, MimeCategory};

/// Filter parameters for type listing.
#[derive(Debug, Deserialize)]
pub struct TypeFilterParams {
    pub limit: Option<usize>,
    pub source: Option<String>,
}

/// List all type categories.
pub async fn list_types(State(state): State<AppState>) -> impl IntoResponse {
    let type_stats = match state.doc_repo.get_type_stats().await {
        Ok(stats) => stats,
        Err(e) => {
            let msg = format!("Failed to load type stats: {}", e);
            let template = ErrorTemplate {
                title: "Error",
                message: &msg,
            };
            return Html(template.render().unwrap_or(msg));
        }
    };

    // Build category counts
    let mut cat_counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (mime, count) in &type_stats {
        let cat = mime_to_category(mime).to_string();
        *cat_counts.entry(cat).or_default() += count;
    }

    let categories: Vec<CategoryWithCount> = MimeCategory::all()
        .iter()
        .filter_map(|(cat_id, cat_name)| {
            let count = cat_counts.get(*cat_id).copied().unwrap_or(0);
            if count > 0 {
                Some(CategoryWithCount {
                    id: cat_id.to_string(),
                    name: cat_name.to_string(),
                    count,
                    active: false,
                    checked: false,
                })
            } else {
                None
            }
        })
        .collect();

    let stats_with_category: Vec<TypeStat> = type_stats
        .iter()
        .map(|(mime, count)| TypeStat {
            category: mime_to_category(mime).to_string(),
            mime_type: mime.clone(),
            count: *count,
        })
        .collect();

    let template = TypesTemplate {
        title: "Document Types",
        categories,
        type_stats: stats_with_category,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
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
        .await
    {
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

    // Get category stats for tabs
    let tabs: Vec<CategoryWithCount> = match state.doc_repo.get_type_stats().await {
        Ok(stats) => {
            let mut cat_counts: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            for (mime, count) in stats {
                let cat = mime_to_category(&mime).to_string();
                *cat_counts.entry(cat).or_default() += count;
            }

            MimeCategory::all()
                .iter()
                .filter_map(|(cat_id, cat_name)| {
                    let count = cat_counts.get(*cat_id).copied().unwrap_or(0);
                    if count > 0 {
                        Some(CategoryWithCount {
                            id: cat_id.to_string(),
                            name: cat_name.to_string(),
                            count,
                            active: *cat_id == type_name,
                            checked: false,
                        })
                    } else {
                        None
                    }
                })
                .collect()
        }
        Err(_) => Vec::new(),
    };

    let doc_rows: Vec<DocumentRow> = documents
        .iter()
        .filter_map(DocumentRow::from_document)
        .collect();

    let title = format!("Type: {}", type_name);
    let template = TypeDocumentsTemplate {
        title: &title,
        type_name: &type_name,
        document_count: doc_rows.len(),
        tabs: tabs.clone(),
        has_tabs: !tabs.is_empty(),
        documents: doc_rows,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}
