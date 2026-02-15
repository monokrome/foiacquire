//! Browse page handler.

use askama::Template;
use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::template_structs::{
    ActiveTagDisplay, BrowseTemplate, DocumentRow, ErrorTemplate,
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

    // Fetch only document rows and count — tags, sources, and type stats
    // are loaded client-side via cached API endpoints.
    let offset = page.saturating_sub(1) * per_page;
    let (browse_result, count_result) = tokio::join!(
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
        categories: Vec::new(),
        type_stats_empty: true,
        sources: Vec::new(),
        sources_empty: true,
        has_active_source: params.source.is_some(),
        active_source_val: params.source.clone().unwrap_or_default(),
        all_tags: Vec::new(),
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
