//! Browse page handler.

use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
};
use serde::Deserialize;

use super::super::cache::StatsCache;
use super::super::templates;
use super::super::AppState;
use super::helpers::build_timeline_data;

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
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    let page = params.page.unwrap_or(1).clamp(1, 100_000);

    let types: Vec<String> = params
        .types
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .take(20)
                .collect()
        })
        .unwrap_or_default();

    let tags: Vec<String> = params
        .tags
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .take(50)
                .collect()
        })
        .unwrap_or_default();

    let (cached_total, skip_count) = if types.is_empty() && tags.is_empty() && params.q.is_none() {
        let count = if let Some(source_id) = params.source.as_deref() {
            state.doc_repo.count_by_source(source_id).ok()
        } else {
            state.doc_repo.count().ok()
        };
        (count, false)
    } else {
        let cache_key = StatsCache::browse_count_key(
            params.source.as_deref(),
            &types,
            &tags,
            params.q.as_deref(),
        );
        let cached = state.stats_cache.get_browse_count(&cache_key);
        (cached, cached.is_none())
    };

    let state_browse = state.clone();
    let types_browse = types.clone();
    let tags_browse = tags.clone();
    let source_browse = params.source.clone();
    let q_browse = params.q.clone();

    let effective_total = if skip_count { Some(0) } else { cached_total };

    let browse_handle = tokio::task::spawn_blocking(move || {
        state_browse.doc_repo.browse(
            &types_browse,
            &tags_browse,
            source_browse.as_deref(),
            q_browse.as_deref(),
            page,
            per_page,
            effective_total,
        )
    });

    let has_filters = !types.is_empty() || !tags.is_empty() || params.q.is_some();

    let state_types = state.clone();
    let type_stats_handle = tokio::task::spawn_blocking(move || {
        state_types
            .doc_repo
            .get_category_stats(None)
            .unwrap_or_default()
    });

    let (tags_handle, sources_handle) = if has_filters {
        (None, None)
    } else {
        let state_tags = state.clone();
        let tags_handle = Some(tokio::task::spawn_blocking(move || {
            state_tags.doc_repo.get_all_tags().unwrap_or_default()
        }));

        let state_sources = state.clone();
        let sources_handle = Some(tokio::task::spawn_blocking(move || {
            let counts = state_sources
                .doc_repo
                .get_all_source_counts()
                .unwrap_or_default();
            let sources = state_sources.source_repo.get_all().unwrap_or_default();
            sources
                .into_iter()
                .map(|s| {
                    let count = counts.get(&s.id).copied().unwrap_or(0);
                    (s.id, s.name, count)
                })
                .collect::<Vec<_>>()
        }));

        (tags_handle, sources_handle)
    };

    let browse_res = browse_handle.await;
    let type_stats_res = type_stats_handle.await;

    let tags_res = match tags_handle {
        Some(h) => Some(h.await),
        None => None,
    };
    let sources_res = match sources_handle {
        Some(h) => Some(h.await),
        None => None,
    };

    let browse_result = match browse_res {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load documents: {}</p>", e),
                None,
            ));
        }
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Task failed: {}</p>", e),
                None,
            ));
        }
    };

    if skip_count {
        let state_for_count = state.clone();
        let state_for_cache = state.clone();
        let types_bg = types.clone();
        let tags_bg = tags.clone();
        let source_bg = params.source.clone();
        let q_bg = params.q.clone();

        let cache_key = StatsCache::browse_count_key(
            source_bg.as_deref(),
            &types_bg,
            &tags_bg,
            q_bg.as_deref(),
        );

        tokio::spawn(async move {
            if let Ok(Ok(count)) = tokio::task::spawn_blocking(move || {
                state_for_count.doc_repo.browse_count(
                    &types_bg,
                    &tags_bg,
                    source_bg.as_deref(),
                    q_bg.as_deref(),
                )
            })
            .await
            {
                state_for_cache
                    .stats_cache
                    .set_browse_count(cache_key, count);
            }
        });
    }

    let type_stats: Vec<(String, u64)> = type_stats_res.unwrap_or_else(|_| Vec::new());
    let all_tags: Vec<(String, usize)> = tags_res.and_then(|r| r.ok()).unwrap_or_default();
    let sources: Vec<(String, String, u64)> = sources_res.and_then(|r| r.ok()).unwrap_or_default();

    let timeline = build_timeline_data(&browse_result.documents);
    let timeline_json = serde_json::to_string(&timeline).unwrap_or_else(|_| "{}".to_string());

    let doc_data: Vec<_> = browse_result
        .documents
        .iter()
        .filter_map(|doc| {
            let version = doc.current_version()?;
            let display_name = version
                .original_filename
                .clone()
                .unwrap_or_else(|| doc.title.clone());

            Some((
                doc.id.clone(),
                display_name,
                doc.source_id.clone(),
                version.mime_type.clone(),
                version.file_size,
                version.acquired_at,
                doc.synopsis.clone(),
                doc.tags.clone(),
            ))
        })
        .collect();

    let content = templates::browse_page(
        &doc_data,
        &type_stats,
        &types,
        &tags,
        params.source.as_deref(),
        &all_tags,
        &sources,
        browse_result.prev_cursor.as_deref(),
        browse_result.next_cursor.as_deref(),
        browse_result.start_position,
        browse_result.total,
        per_page,
    );
    Html(templates::base_template(
        "Browse",
        &content,
        Some(&timeline_json),
    ))
}
