//! Browse page handler.

use askama::Template;
use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use foia::utils::MimeCategory;

use super::super::template_structs::{
    ActiveTagDisplay, BrowseTemplate, CategoryWithCount, DocumentRow, ErrorTemplate, SourceOption,
    TagWithCount,
};
use super::super::AppState;
use super::helpers::{paginate, parse_csv_param_limit};

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

    let offset = page.saturating_sub(1) * per_page;
    let (browse_result, count_result, category_stats, source_counts, sources, all_tags) =
        tokio::join!(
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
            async {
                match state.stats_cache.get_category_stats() {
                    Some(cached) => cached,
                    None => {
                        let stats = state
                            .doc_repo
                            .get_category_stats(None)
                            .await
                            .unwrap_or_default();
                        state.stats_cache.set_category_stats(stats.clone());
                        stats
                    }
                }
            },
            async {
                match state.stats_cache.get_source_counts() {
                    Some(cached) => cached,
                    None => {
                        let counts = state
                            .doc_repo
                            .get_all_source_counts()
                            .await
                            .unwrap_or_default();
                        state.stats_cache.set_source_counts(counts.clone());
                        counts
                    }
                }
            },
            state.source_repo.get_all(),
            async {
                match state.stats_cache.get_all_tags() {
                    Some(cached) => cached,
                    None => {
                        let raw = state.doc_repo.get_all_tags().await.unwrap_or_default();
                        let with_counts: Vec<(String, usize)> =
                            raw.into_iter().map(|t| (t, 0)).collect();
                        state.stats_cache.set_all_tags(with_counts.clone());
                        with_counts
                    }
                }
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

    let doc_rows: Vec<DocumentRow> = browse_rows
        .into_iter()
        .map(DocumentRow::from_browse_row)
        .collect();

    // Build category filter checkboxes
    let categories: Vec<CategoryWithCount> = MimeCategory::all()
        .iter()
        .filter_map(|(id, name)| {
            let count = category_stats.get(*id).copied().unwrap_or(0);
            if count == 0 {
                return None;
            }
            let checked = types.is_empty() || types.iter().any(|t| t == *id);
            Some(CategoryWithCount {
                id: id.to_string(),
                name: name.to_string(),
                count,
                active: checked,
                checked,
            })
        })
        .collect();

    // Build source dropdown options
    let source_options: Vec<SourceOption> = sources
        .unwrap_or_default()
        .into_iter()
        .map(|s| {
            let count = source_counts.get(&s.id).copied().unwrap_or(0);
            let selected = params.source.as_deref() == Some(&s.id);
            SourceOption {
                id: s.id,
                name: s.name,
                count,
                selected,
            }
        })
        .collect();

    // Build tag datalist
    let tag_list: Vec<TagWithCount> = all_tags
        .into_iter()
        .map(|(name, count)| TagWithCount::new(name, count))
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

    // Active tags display
    let active_tags_display: Vec<ActiveTagDisplay> = tags
        .iter()
        .enumerate()
        .map(|(i, t)| ActiveTagDisplay {
            name: t.clone(),
            index: i,
        })
        .collect();

    // JSON for JavaScript (passed via data attributes to avoid Askama HTML escaping)
    let active_tags_json = serde_json::to_string(&tags).unwrap_or_else(|_| "[]".to_string());

    let end_position = start_position + doc_rows.len() as u64;

    let template = BrowseTemplate {
        title: "Browse",
        documents: doc_rows,
        categories,
        sources: source_options,
        all_tags: tag_list,
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
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}
