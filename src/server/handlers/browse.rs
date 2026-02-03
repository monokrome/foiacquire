//! Browse page handler.

use askama::Template;
use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::template_structs::{
    ActiveTagDisplay, BrowseTemplate, CategoryWithCount, DocumentRow, ErrorTemplate, SourceOption,
    TagWithCount,
};
use super::super::AppState;
use super::helpers::{paginate, parse_csv_param_limit};
use crate::utils::MimeCategory;

/// Query params for the unified browse page.
#[derive(Debug, Clone, Deserialize)]
pub struct BrowseParams {
    pub types: Option<String>,
    pub tags: Option<String>,
    pub source: Option<String>,
    pub q: Option<String>,
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

/// Unified document browse page with filters.
pub async fn browse_documents(
    State(state): State<AppState>,
    Query(params): Query<BrowseParams>,
) -> impl IntoResponse {
    let (page, per_page, _offset) = paginate(params.page, params.per_page);
    let types = parse_csv_param_limit(params.types.as_ref(), Some(20));
    let tags = parse_csv_param_limit(params.tags.as_ref(), Some(50));

    let has_filters = !types.is_empty() || !tags.is_empty() || params.q.is_some();

    // Run all database queries concurrently
    let offset = page.saturating_sub(1) * per_page;
    let (browse_result, count_result, cat_stats_result, tags_and_sources) = tokio::join!(
        state.doc_repo.browse_fast(
            params.source.as_deref(),
            None,
            &types,
            &tags,
            per_page as u32,
            offset as u32,
        ),
        state.doc_repo.browse_count(
            params.source.as_deref(),
            None,
            &types,
            &tags,
            params.q.as_deref(),
        ),
        state
            .doc_repo
            .get_category_stats(params.source.as_deref()),
        async {
            if has_filters {
                return (Vec::new(), Vec::new());
            }
            let (tags_result, counts, source_list) = tokio::join!(
                state.doc_repo.get_all_tags(),
                state.doc_repo.get_all_source_counts(),
                state.source_repo.get_all(),
            );
            let tags = tags_result.unwrap_or_default();
            let counts = counts.unwrap_or_default();
            let sources: Vec<_> = source_list
                .unwrap_or_default()
                .into_iter()
                .map(|s| {
                    let count = counts.get(&s.id).copied().unwrap_or(0);
                    (s.id, s.name, count)
                })
                .collect();
            (tags, sources)
        },
    );

    let browse_rows = match browse_result {
        Ok(result) => result,
        Err(e) => {
            let template = ErrorTemplate {
                title: "Error",
                message: &format!("Failed to load documents: {}", e),
            };
            return Html(template.render().unwrap_or_else(|_| e.to_string()));
        }
    };

    let total = match count_result {
        Ok(count) => count,
        Err(_) => browse_rows.len() as u64,
    };

    let type_stats: Vec<(String, u64)> = cat_stats_result
        .unwrap_or_default()
        .into_iter()
        .collect();

    let (all_tags, sources) = tags_and_sources;

    // Convert BrowseRows to DocumentRows (fast path - no Document model needed)
    let doc_rows: Vec<DocumentRow> = browse_rows
        .into_iter()
        .map(DocumentRow::from_browse_row)
        .collect();

    // Calculate pagination cursors
    let start_position = offset as u64;
    let has_prev = page > 1;
    let has_next = start_position + (per_page as u64) < total;
    let prev_cursor = if has_prev {
        Some(format!("{}", page - 1))
    } else {
        None
    };
    let next_cursor = if has_next {
        Some(format!("{}", page + 1))
    } else {
        None
    };

    // Build query string for document links
    let nav_query_string = {
        let mut qs_parts = Vec::new();
        if !types.is_empty() {
            qs_parts.push(format!("types={}", urlencoding::encode(&types.join(","))));
        }
        if !tags.is_empty() {
            qs_parts.push(format!("tags={}", urlencoding::encode(&tags.join(","))));
        }
        if let Some(source) = params.source.as_deref() {
            qs_parts.push(format!("source={}", urlencoding::encode(source)));
        }
        if qs_parts.is_empty() {
            String::new()
        } else {
            format!("?{}", qs_parts.join("&"))
        }
    };

    // Build categories for type toggles
    let categories: Vec<CategoryWithCount> = MimeCategory::all()
        .iter()
        .filter_map(|(cat_id, cat_name)| {
            let count = type_stats
                .iter()
                .find(|(c, _)| c == *cat_id)
                .map(|(_, n)| *n)
                .unwrap_or(0);
            if count > 0 {
                let checked = types.is_empty() || types.iter().any(|t| t == *cat_id);
                Some(CategoryWithCount {
                    id: cat_id.to_string(),
                    name: cat_name.to_string(),
                    count,
                    active: false,
                    checked,
                })
            } else {
                None
            }
        })
        .collect();

    // Build source options
    let source_options: Vec<SourceOption> = sources
        .iter()
        .map(|(id, name, count)| SourceOption {
            id: id.clone(),
            name: name.clone(),
            count: *count,
            selected: params.source.as_deref() == Some(id.as_str()),
        })
        .collect();

    // Build tag list
    let all_tags_with_counts: Vec<TagWithCount> = all_tags
        .iter()
        .map(|t| TagWithCount::new(t.clone(), 0))
        .collect();

    // Active tags display
    let active_tags_display: Vec<ActiveTagDisplay> = tags
        .iter()
        .enumerate()
        .map(|(i, t)| ActiveTagDisplay {
            name: t.clone(),
            index: i,
        })
        .collect();

    // JSON for JavaScript
    let active_tags_json = serde_json::to_string(&tags).unwrap_or_else(|_| "[]".to_string());
    let active_types_json = serde_json::to_string(&types).unwrap_or_else(|_| "[]".to_string());
    let active_source_js = params
        .source
        .as_ref()
        .map(|s| format!("\"{}\"", s))
        .unwrap_or_else(|| "null".to_string());
    let prev_cursor_js = prev_cursor
        .as_ref()
        .map(|c| format!("\"{}\"", c))
        .unwrap_or_else(|| "null".to_string());
    let next_cursor_js = next_cursor
        .as_ref()
        .map(|c| format!("\"{}\"", c))
        .unwrap_or_else(|| "null".to_string());

    let end_position = start_position + doc_rows.len() as u64;

    let template = BrowseTemplate {
        title: "Browse",
        documents: doc_rows,
        categories,
        type_stats_empty: type_stats.is_empty(),
        sources: source_options,
        sources_empty: sources.is_empty(),
        has_active_source: params.source.is_some(),
        active_source_val: params.source.clone().unwrap_or_default(),
        all_tags: all_tags_with_counts,
        active_tags_display,
        has_prev_cursor: prev_cursor.is_some(),
        prev_cursor_val: prev_cursor.unwrap_or_default(),
        has_next_cursor: next_cursor.is_some(),
        next_cursor_val: next_cursor.unwrap_or_default(),
        start_position,
        end_position,
        total_count: total,
        per_page,
        has_pagination: has_prev || has_next,
        nav_query_string,
        active_tags_json,
        active_types_json,
        active_source_js,
        prev_cursor_js,
        next_cursor_js,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}
