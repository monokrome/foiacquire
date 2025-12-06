//! HTTP request handlers for the web server.

#![allow(dead_code)]

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::templates;
use super::AppState;
use crate::models::VirtualFile;
use crate::repository::DocumentSummary;

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

    // Get all document counts in a single query (instead of N+1)
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
    // Get source info
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

    // Get documents
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

    // Build timeline data
    let timeline = build_timeline_data(&documents);
    let timeline_json = serde_json::to_string(&timeline).unwrap_or_default();

    // Find duplicates across sources
    let duplicates = find_cross_source_duplicates(&state, &documents);

    // Transform documents for display
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

            // Use original_filename if available, otherwise fall back to document title
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

/// Query params for document detail navigation context.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DocumentDetailParams {
    /// Comma-separated list of type categories (for navigation context)
    pub types: Option<String>,
    /// Comma-separated list of tags (for navigation context)
    pub tags: Option<String>,
    /// Source filter (for navigation context)
    pub source: Option<String>,
    /// Search query (for navigation context)
    pub q: Option<String>,
}

/// Document detail page.
pub async fn document_detail(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
    Query(params): Query<DocumentDetailParams>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id) {
        Ok(Some(d)) => d,
        Ok(None) => {
            return Html(templates::base_template(
                "Not Found",
                "<p>Document not found.</p>",
                None,
            ));
        }
        Err(e) => {
            return Html(templates::base_template(
                "Error",
                &format!("<p>Failed to load document: {}</p>", e),
                None,
            ));
        }
    };

    // Parse filter context for navigation
    let types: Vec<String> = params
        .types
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
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
                .collect()
        })
        .unwrap_or_default();

    // Get navigation context using window functions
    let navigation = state
        .doc_repo
        .get_document_navigation(
            &doc_id,
            &types,
            &tags,
            params.source.as_deref(),
            params.q.as_deref(),
        )
        .ok()
        .flatten();

    // Build query string for navigation links
    let nav_query_string = {
        let mut qs_parts = Vec::new();
        if let Some(ref t) = params.types {
            qs_parts.push(format!("types={}", urlencoding::encode(t)));
        }
        if let Some(ref t) = params.tags {
            qs_parts.push(format!("tags={}", urlencoding::encode(t)));
        }
        if let Some(ref s) = params.source {
            qs_parts.push(format!("source={}", urlencoding::encode(s)));
        }
        if let Some(ref q) = params.q {
            qs_parts.push(format!("q={}", urlencoding::encode(q)));
        }
        if qs_parts.is_empty() {
            String::new()
        } else {
            format!("?{}", qs_parts.join("&"))
        }
    };

    // Get version info
    let versions: Vec<_> = doc
        .versions
        .iter()
        .map(|v| {
            let relative_path = v
                .file_path
                .strip_prefix(&state.documents_dir)
                .unwrap_or(&v.file_path)
                .to_string_lossy()
                .to_string();
            (
                v.content_hash.clone(),
                relative_path,
                v.file_size,
                v.acquired_at,
                v.original_filename.clone(),
                v.server_date,
            )
        })
        .collect();

    // Find other sources with same content
    let other_sources = if let Some(version) = doc.current_version() {
        find_sources_with_hash(&state, &version.content_hash, &doc.source_id)
    } else {
        vec![]
    };

    // Get virtual files (archive contents) for this document
    let virtual_files: Vec<VirtualFile> = state
        .doc_repo
        .get_virtual_files(&doc_id)
        .unwrap_or_default();

    // Get page count for current version (if any) - uses COUNT query instead of loading all pages
    let current_version_id = doc.current_version().map(|v| v.id);
    let page_count: Option<u32> =
        current_version_id.and_then(|vid| state.doc_repo.count_pages(&doc_id, vid).ok());

    let content = templates::document_detail(
        &doc.id,
        &doc.title,
        &doc.source_id,
        &doc.source_url,
        &versions,
        &other_sources,
        doc.extracted_text.as_deref(),
        doc.synopsis.as_deref(),
        &virtual_files,
        navigation
            .as_ref()
            .and_then(|n| n.prev_id.as_ref())
            .map(|s| s.as_str()),
        navigation
            .as_ref()
            .and_then(|n| n.prev_title.as_ref())
            .map(|s| s.as_str()),
        navigation
            .as_ref()
            .and_then(|n| n.next_id.as_ref())
            .map(|s| s.as_str()),
        navigation
            .as_ref()
            .and_then(|n| n.next_title.as_ref())
            .map(|s| s.as_str()),
        navigation.as_ref().map(|n| n.position).unwrap_or(0),
        navigation.as_ref().map(|n| n.total).unwrap_or(0),
        &nav_query_string,
        page_count,
        current_version_id,
    );

    Html(templates::base_template(&doc.title, &content, None))
}

/// Get document versions as JSON.
pub async fn document_versions(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id) {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "Document not found").into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let versions: Vec<_> = doc
        .versions
        .iter()
        .map(|v| VersionInfo {
            content_hash: v.content_hash.clone(),
            file_size: v.file_size,
            mime_type: v.mime_type.clone(),
            acquired_at: v.acquired_at.to_rfc3339(),
        })
        .collect();

    axum::Json(versions).into_response()
}

/// Parameters for pages view/API.
#[derive(Debug, Deserialize)]
pub struct PagesParams {
    pub version: Option<i64>,
    pub offset: Option<u32>,
    pub limit: Option<u32>,
}

/// Single page data for API response.
#[derive(Debug, Serialize)]
pub struct PageData {
    pub page_number: u32,
    pub ocr_text: Option<String>,
    pub pdf_text: Option<String>,
    pub final_text: Option<String>,
    pub image_base64: Option<String>,
    pub ocr_status: String,
    pub deepseek_text: Option<String>,
}

/// Pages API response.
#[derive(Debug, Serialize)]
pub struct PagesResponse {
    pub pages: Vec<PageData>,
    pub total_pages: u32,
    pub has_more: bool,
    pub document_id: String,
    pub version_id: i64,
}

/// API endpoint to get paginated pages with rendered images and OCR text.
pub async fn api_document_pages(
    State(state): State<AppState>,
    Path(doc_id): Path<String>,
    Query(params): Query<PagesParams>,
) -> impl IntoResponse {
    let doc = match state.doc_repo.get(&doc_id) {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "Document not found").into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Get version ID (use provided or default to current)
    let version_id = params
        .version
        .or_else(|| doc.current_version().map(|v| v.id));
    let version_id = match version_id {
        Some(id) => id,
        None => {
            return (StatusCode::NOT_FOUND, "No version found").into_response();
        }
    };

    // Find the version to get file path
    let version = doc.versions.iter().find(|v| v.id == version_id);
    let version = match version {
        Some(v) => v,
        None => {
            return (StatusCode::NOT_FOUND, "Version not found").into_response();
        }
    };

    // Get all pages from database
    let all_pages = match state.doc_repo.get_pages(&doc_id, version_id) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let total_pages = all_pages.len() as u32;
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(5).min(20); // Cap at 20 pages per request

    // Slice the pages we need
    let start = offset as usize;
    let end = (offset + limit) as usize;
    let selected_pages: Vec<_> = all_pages
        .into_iter()
        .skip(start)
        .take(limit as usize)
        .collect();

    // Build a map of page_id -> deepseek_text for comparison view
    let mut deepseek_map: std::collections::HashMap<i64, Option<String>> =
        std::collections::HashMap::new();
    for page in &selected_pages {
        if let Ok(ocr_results) = state.doc_repo.get_page_ocr_results(page.id) {
            // Find DeepSeek result
            for (backend, text, _, _) in ocr_results {
                if backend == "deepseek" {
                    deepseek_map.insert(page.id, text);
                    break;
                }
            }
        }
    }

    // Render pages to images if this is a PDF
    // Use spawn_blocking to avoid blocking the async runtime
    let is_pdf = version.mime_type.contains("pdf");
    let pdf_path = version.file_path.clone();

    let page_data_list: Vec<PageData> = if is_pdf {
        // Render all pages in parallel using spawn_blocking
        let mut handles = Vec::new();
        for page in selected_pages {
            let path = pdf_path.clone();
            let page_num = page.page_number;
            let page_id = page.id;
            let ocr_text = page.ocr_text;
            let pdf_text = page.pdf_text;
            let final_text = page.final_text;
            let ocr_status = page.ocr_status.as_str().to_string();
            let deepseek_text = deepseek_map.get(&page_id).cloned().flatten();

            let handle = tokio::task::spawn_blocking(move || {
                let image_base64 = render_pdf_page_to_base64(&path, page_num);
                PageData {
                    page_number: page_num,
                    ocr_text,
                    pdf_text,
                    final_text,
                    image_base64,
                    ocr_status,
                    deepseek_text,
                }
            });
            handles.push(handle);
        }

        // Collect results
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            if let Ok(page_data) = handle.await {
                results.push(page_data);
            }
        }
        // Sort by page number to maintain order
        results.sort_by_key(|p| p.page_number);
        results
    } else {
        // Non-PDF: no image rendering needed
        selected_pages
            .into_iter()
            .map(|page| {
                let deepseek_text = deepseek_map.get(&page.id).cloned().flatten();
                PageData {
                    page_number: page.page_number,
                    ocr_text: page.ocr_text,
                    pdf_text: page.pdf_text,
                    final_text: page.final_text,
                    image_base64: None,
                    ocr_status: page.ocr_status.as_str().to_string(),
                    deepseek_text,
                }
            })
            .collect()
    };

    let has_more = end < total_pages as usize;

    axum::Json(PagesResponse {
        pages: page_data_list,
        total_pages,
        has_more,
        document_id: doc_id,
        version_id,
    })
    .into_response()
}

/// Render a PDF page to a base64-encoded PNG image.
fn render_pdf_page_to_base64(pdf_path: &std::path::Path, page_number: u32) -> Option<String> {
    use base64::Engine;
    use std::process::Command;

    // Create a temporary file for the output
    let temp_dir = std::env::temp_dir();
    let output_prefix = temp_dir.join(format!("foiacquire_page_{}", uuid::Uuid::new_v4()));

    // Use pdftoppm to render the page (150 DPI for web viewing)
    let status = Command::new("pdftoppm")
        .args([
            "-png",
            "-r",
            "150",
            "-f",
            &page_number.to_string(),
            "-l",
            &page_number.to_string(),
            "-singlefile",
        ])
        .arg(pdf_path)
        .arg(&output_prefix)
        .status();

    if status.map(|s| s.success()).unwrap_or(false) {
        let output_path = output_prefix.with_extension("png");
        if let Ok(image_data) = std::fs::read(&output_path) {
            // Clean up temp file
            let _ = std::fs::remove_file(&output_path);
            let base64_str = base64::engine::general_purpose::STANDARD.encode(&image_data);
            return Some(format!("data:image/png;base64,{}", base64_str));
        }
    }

    None
}


/// Serve a document file.
pub async fn serve_file(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    let file_path = state.documents_dir.join(&path);

    if !file_path.exists() {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    }

    // Security: ensure path is within documents_dir
    match file_path.canonicalize() {
        Ok(canonical) => {
            if !canonical.starts_with(&state.documents_dir) {
                return (StatusCode::FORBIDDEN, "Access denied").into_response();
            }
        }
        Err(_) => {
            return (StatusCode::NOT_FOUND, "File not found").into_response();
        }
    }

    let content = match std::fs::read(&file_path) {
        Ok(c) => c,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response();
        }
    };

    let mime = mime_guess::from_path(&file_path)
        .first_or_octet_stream()
        .to_string();

    ([(header::CONTENT_TYPE, mime)], content).into_response()
}

/// Timeline aggregate across all sources.
pub async fn timeline_aggregate(
    State(state): State<AppState>,
    Query(_params): Query<DateRangeParams>,
) -> impl IntoResponse {
    // Use lightweight summaries to avoid loading extracted_text
    let summaries = match state.doc_repo.get_all_summaries() {
        Ok(s) => s,
        Err(e) => {
            return axum::Json(TimelineResponse {
                buckets: vec![],
                total: 0,
                error: Some(e.to_string()),
            });
        }
    };

    let timeline = build_timeline_from_summaries(&summaries);
    axum::Json(timeline)
}

/// Timeline for a specific source.
pub async fn timeline_source(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Query(_params): Query<DateRangeParams>,
) -> impl IntoResponse {
    // Use lightweight summaries to avoid loading extracted_text
    let summaries = match state.doc_repo.get_summaries_by_source(&source_id) {
        Ok(s) => s,
        Err(e) => {
            return axum::Json(TimelineResponse {
                buckets: vec![],
                total: 0,
                error: Some(e.to_string()),
            });
        }
    };

    let timeline = build_timeline_from_summaries(&summaries);
    axum::Json(timeline)
}

/// List documents that exist in multiple sources.
pub async fn list_duplicates(State(state): State<AppState>) -> impl IntoResponse {
    // Use lightweight query that only fetches hashes (no document content)
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

    // Group by content hash: (doc_id, source_id, hash, title)
    let mut hash_to_docs: HashMap<String, Vec<(String, String, String)>> = HashMap::new();

    for (doc_id, source_id, content_hash, title) in hashes {
        hash_to_docs
            .entry(content_hash)
            .or_default()
            .push((doc_id, source_id, title));
    }

    // Filter to only duplicates (more than one document per hash, different sources)
    let duplicates: Vec<_> = hash_to_docs
        .into_iter()
        .filter(|(_, docs)| {
            // Count unique sources
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

/// Serve CSS.
pub async fn serve_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css")], templates::CSS)
}

/// Serve JavaScript.
pub async fn serve_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript")],
        templates::JS,
    )
}

// Helper types and functions

#[derive(Debug, Deserialize)]
pub struct DateRangeParams {
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TimelineResponse {
    buckets: Vec<TimelineBucket>,
    total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TimelineBucket {
    date: String,
    timestamp: i64,
    count: u64,
}

#[derive(Debug, Serialize)]
pub struct VersionInfo {
    content_hash: String,
    file_size: u64,
    mime_type: String,
    acquired_at: String,
}

fn build_timeline_data(documents: &[crate::models::Document]) -> TimelineResponse {
    use std::collections::BTreeMap;

    // Group by date (YYYY-MM-DD)
    let mut date_counts: BTreeMap<String, u64> = BTreeMap::new();

    for doc in documents {
        if let Some(version) = doc.current_version() {
            let date = version.acquired_at.format("%Y-%m-%d").to_string();
            *date_counts.entry(date).or_default() += 1;
        }
    }

    let buckets: Vec<_> = date_counts
        .into_iter()
        .map(|(date, count)| {
            let timestamp = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
                .unwrap_or(0);
            TimelineBucket {
                date,
                timestamp,
                count,
            }
        })
        .collect();

    let total = buckets.iter().map(|b| b.count).sum();

    TimelineResponse {
        buckets,
        total,
        error: None,
    }
}

/// Build timeline from lightweight summaries (no extracted_text loaded).
fn build_timeline_from_summaries(summaries: &[DocumentSummary]) -> TimelineResponse {
    use std::collections::BTreeMap;

    let mut date_counts: BTreeMap<String, u64> = BTreeMap::new();

    for summary in summaries {
        if let Some(ref version) = summary.current_version {
            let date = version.acquired_at.format("%Y-%m-%d").to_string();
            *date_counts.entry(date).or_default() += 1;
        }
    }

    let buckets: Vec<_> = date_counts
        .into_iter()
        .map(|(date, count)| {
            let timestamp = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
                .unwrap_or(0);
            TimelineBucket {
                date,
                timestamp,
                count,
            }
        })
        .collect();

    let total = buckets.iter().map(|b| b.count).sum();

    TimelineResponse {
        buckets,
        total,
        error: None,
    }
}

fn find_cross_source_duplicates(
    state: &AppState,
    documents: &[crate::models::Document],
) -> HashMap<String, Vec<String>> {
    let mut result: HashMap<String, Vec<String>> = HashMap::new();

    // Use lightweight hash query instead of loading all documents
    let hashes = match state.doc_repo.get_content_hashes() {
        Ok(h) => h,
        Err(_) => return result,
    };

    // Build hash -> sources map
    let mut hash_to_sources: HashMap<String, Vec<String>> = HashMap::new();
    for (_, source_id, content_hash, _) in &hashes {
        hash_to_sources
            .entry(content_hash.clone())
            .or_default()
            .push(source_id.clone());
    }

    // Collect hashes from current documents that exist in other sources
    for doc in documents {
        if let Some(version) = doc.current_version() {
            if let Some(sources) = hash_to_sources.get(&version.content_hash) {
                if sources.len() > 1 {
                    result.insert(version.content_hash.clone(), sources.clone());
                }
            }
        }
    }

    result
}

fn find_sources_with_hash(
    state: &AppState,
    content_hash: &str,
    exclude_source: &str,
) -> Vec<String> {
    // Use indexed query to find documents with matching hash
    match state
        .doc_repo
        .find_sources_by_hash(content_hash, Some(exclude_source))
    {
        Ok(results) => {
            // Deduplicate source IDs
            let mut sources: Vec<String> = results
                .into_iter()
                .map(|(source_id, _, _)| source_id)
                .collect();
            sources.sort();
            sources.dedup();
            sources
        }
        Err(_) => vec![],
    }
}

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
    // URL decode the tag
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

    // Transform documents for display
    use crate::models::DocumentDisplay;
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
    // Use cache to avoid expensive query
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

/// API endpoint to get all sources with document counts.
pub async fn api_sources(State(state): State<AppState>) -> impl IntoResponse {
    // Get source counts (cached)
    let source_counts = state.stats_cache.get_source_counts().unwrap_or_else(|| {
        let counts = state.doc_repo.get_all_source_counts().unwrap_or_default();
        state.stats_cache.set_source_counts(counts.clone());
        counts
    });

    let sources: Vec<_> = state
        .source_repo
        .get_all()
        .unwrap_or_default()
        .into_iter()
        .map(|s| {
            let count = source_counts.get(&s.id).copied().unwrap_or(0);
            serde_json::json!({
                "id": s.id,
                "name": s.name,
                "count": count
            })
        })
        .collect();

    axum::Json(sources).into_response()
}

/// API endpoint to get overall database status.
pub async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    let doc_count = state.doc_repo.count().unwrap_or(0);
    let needing_ocr = state.doc_repo.count_needing_ocr(None).unwrap_or(0);
    let needing_summary = state
        .doc_repo
        .count_needing_summarization(None)
        .unwrap_or(0);

    // Get crawl stats
    let crawl_stats = state.crawl_repo.get_all_stats().unwrap_or_default();

    // Aggregate crawl stats
    let mut total_pending = 0u64;
    let mut total_failed = 0u64;
    let mut total_discovered = 0u64;
    let mut source_stats = Vec::new();

    for (source_id, stats) in &crawl_stats {
        total_pending += stats.urls_pending;
        total_failed += stats.urls_failed;
        total_discovered += stats.urls_discovered;
        source_stats.push(serde_json::json!({
            "source_id": source_id,
            "discovered": stats.urls_discovered,
            "fetched": stats.urls_fetched,
            "pending": stats.urls_pending,
            "failed": stats.urls_failed,
            "has_pending": stats.has_pending_urls,
        }));
    }

    // Get recent downloads from crawl
    let recent_urls: Vec<_> = state
        .crawl_repo
        .get_recent_downloads(None, 10)
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "source_id": u.source_id,
                "fetched_at": u.fetched_at.map(|dt| dt.to_rfc3339()),
                "document_id": u.document_id,
            })
        })
        .collect();

    // Get recent failures
    let failed_urls: Vec<_> = state
        .crawl_repo
        .get_failed_urls(None, 10)
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "source_id": u.source_id,
                "error": u.last_error,
                "retry_count": u.retry_count,
            })
        })
        .collect();

    // Get type stats
    let type_stats: Vec<_> = state
        .doc_repo
        .get_type_stats(None)
        .unwrap_or_default()
        .into_iter()
        .map(|(mime, count)| {
            serde_json::json!({
                "mime_type": mime,
                "count": count
            })
        })
        .collect();

    axum::Json(serde_json::json!({
        "documents": {
            "total": doc_count,
            "needing_ocr": needing_ocr,
            "needing_summarization": needing_summary,
        },
        "crawl": {
            "total_discovered": total_discovered,
            "total_pending": total_pending,
            "total_failed": total_failed,
            "sources": source_stats,
        },
        "recent_downloads": recent_urls,
        "recent_failures": failed_urls,
        "type_stats": type_stats,
    }))
}

/// API endpoint to get status for a specific source.
pub async fn api_source_status(
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    let doc_count = state.doc_repo.count_by_source(&source_id).unwrap_or(0);
    let needing_ocr = state
        .doc_repo
        .count_needing_ocr(Some(&source_id))
        .unwrap_or(0);
    let needing_summary = state
        .doc_repo
        .count_needing_summarization(Some(&source_id))
        .unwrap_or(0);

    // Get crawl state for this source
    let crawl_state = state.crawl_repo.get_crawl_state(&source_id).ok();
    let request_stats = state.crawl_repo.get_request_stats(&source_id).ok();

    // Recent downloads for this source
    let recent_urls: Vec<_> = state
        .crawl_repo
        .get_recent_downloads(Some(&source_id), 20)
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "fetched_at": u.fetched_at.map(|dt| dt.to_rfc3339()),
                "document_id": u.document_id,
            })
        })
        .collect();

    // Failed URLs for this source
    let failed_urls: Vec<_> = state
        .crawl_repo
        .get_failed_urls(Some(&source_id), 20)
        .unwrap_or_default()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "url": u.url,
                "error": u.last_error,
                "retry_count": u.retry_count,
            })
        })
        .collect();

    // Type stats for this source
    let type_stats: Vec<_> = state
        .doc_repo
        .get_type_stats(Some(&source_id))
        .unwrap_or_default()
        .into_iter()
        .map(|(mime, count)| {
            serde_json::json!({
                "mime_type": mime,
                "count": count
            })
        })
        .collect();

    axum::Json(serde_json::json!({
        "source_id": source_id,
        "documents": {
            "total": doc_count,
            "needing_ocr": needing_ocr,
            "needing_summarization": needing_summary,
        },
        "crawl": crawl_state.map(|s| serde_json::json!({
            "discovered": s.urls_discovered,
            "fetched": s.urls_fetched,
            "pending": s.urls_pending,
            "failed": s.urls_failed,
            "has_pending": s.has_pending_urls,
            "last_crawl_started": s.last_crawl_started.map(|dt| dt.to_rfc3339()),
            "last_crawl_completed": s.last_crawl_completed.map(|dt| dt.to_rfc3339()),
        })),
        "request_stats": request_stats.map(|s| serde_json::json!({
            "total_requests": s.total_requests,
            "success_200": s.success_200,
            "not_modified_304": s.not_modified_304,
            "errors": s.errors,
            "avg_duration_ms": s.avg_duration_ms,
            "total_bytes": s.total_bytes,
        })),
        "recent_downloads": recent_urls,
        "recent_failures": failed_urls,
        "type_stats": type_stats,
    }))
}

/// API endpoint to get recent documents.
pub async fn api_recent_docs(
    State(state): State<AppState>,
    Query(params): Query<RecentParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20).min(100);
    let source_id = params.source.as_deref();

    match state.doc_repo.get_recent(source_id, limit) {
        Ok(docs) => {
            let doc_list: Vec<_> = docs
                .into_iter()
                .map(|d| {
                    let version = d.current_version.as_ref();
                    serde_json::json!({
                        "id": d.id,
                        "title": d.title,
                        "source_id": d.source_id,
                        "synopsis": d.synopsis,
                        "tags": d.tags,
                        "status": format!("{:?}", d.status),
                        "updated_at": d.updated_at.to_rfc3339(),
                        "mime_type": version.map(|v| v.mime_type.as_str()),
                        "file_size": version.map(|v| v.file_size),
                    })
                })
                .collect();
            axum::Json(doc_list).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// API endpoint to get document type statistics.
pub async fn api_type_stats(
    State(state): State<AppState>,
    Query(params): Query<SourceFilterParams>,
) -> impl IntoResponse {
    // Use get_category_stats for instant O(1) lookup from file_categories table
    let stats = if params.source.is_none() {
        // Check cache first for unfiltered global stats
        state.stats_cache.get_type_stats().unwrap_or_else(|| {
            let stats = state.doc_repo.get_category_stats(None).unwrap_or_default();
            state.stats_cache.set_type_stats(stats.clone());
            stats
        })
    } else {
        // Source-filtered stats - compute from documents table
        state
            .doc_repo
            .get_category_stats(params.source.as_deref())
            .unwrap_or_default()
    };

    let stats_json: Vec<_> = stats
        .into_iter()
        .map(|(category, count)| {
            serde_json::json!({
                "category": category,
                "count": count
            })
        })
        .collect();
    axum::Json(stats_json).into_response()
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

    // Transform to (category, mime_type, count)
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
    let limit = params.limit.unwrap_or(500);
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

    // Get category stats for tabs
    let category_stats: Option<Vec<(String, u64)>> =
        state.doc_repo.get_type_stats(None).ok().map(|stats| {
            // Group by category
            let mut cat_counts: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            for (mime, count) in stats {
                let cat = mime_to_category(&mime).to_string();
                *cat_counts.entry(cat).or_default() += count;
            }
            cat_counts.into_iter().collect()
        });

    // Transform documents for display
    use crate::models::DocumentDisplay;
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

#[derive(Debug, Deserialize)]
pub struct RecentParams {
    pub limit: Option<usize>,
    pub source: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SourceFilterParams {
    pub source: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TypeFilterParams {
    pub limit: Option<usize>,
    pub source: Option<String>,
}

/// Query params for the unified browse page.
#[derive(Debug, Clone, Deserialize)]
pub struct BrowseParams {
    /// Comma-separated list of type categories to include
    pub types: Option<String>,
    /// Comma-separated list of tags to filter by
    pub tags: Option<String>,
    /// Source to filter by
    pub source: Option<String>,
    /// Search query
    pub q: Option<String>,
    /// Page number (1-indexed)
    pub page: Option<usize>,
    /// Items per page (default 50)
    pub per_page: Option<usize>,
}

/// Unified document browse page with filters.
pub async fn browse_documents(
    State(state): State<AppState>,
    Query(params): Query<BrowseParams>,
) -> impl IntoResponse {
    let per_page = params.per_page.unwrap_or(50).min(200);

    // Parse types from comma-separated string
    let types: Vec<String> = params
        .types
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // Parse tags from comma-separated string
    let tags: Vec<String> = params
        .tags
        .as_ref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // Page-based pagination (1-indexed)
    let page = params.page.unwrap_or(1);

    // Get count: O(1) for unfiltered queries via trigger-maintained table.
    // For filtered queries, use cache or skip count entirely (expensive COUNT DISTINCT).
    let (cached_total, skip_count) = if types.is_empty() && tags.is_empty() && params.q.is_none() {
        // Simple source-only filter: O(1) via document_counts table
        let count = if let Some(source_id) = params.source.as_deref() {
            state.doc_repo.count_by_source(source_id).ok()
        } else {
            // No filters: O(1) total count
            state.doc_repo.count().ok()
        };
        (count, false)
    } else {
        // Complex filters: check browse_count cache
        let cache_key = super::cache::StatsCache::browse_count_key(
            params.source.as_deref(),
            &types,
            &tags,
            params.q.as_deref(),
        );
        let cached = state.stats_cache.get_browse_count(&cache_key);
        // If no cache hit, skip the expensive count query - just paginate without total
        (cached, cached.is_none())
    };

    // Run browse query (always needed)
    let state_browse = state.clone();
    let types_browse = types.clone();
    let tags_browse = tags.clone();
    let source_browse = params.source.clone();
    let q_browse = params.q.clone();

    // When skip_count is true, pass 0 as total to avoid expensive COUNT query.
    // The real count will be computed in background and cached for next request.
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

    // Check if we have filters active (affects which stats we load)
    let has_filters = !types.is_empty() || !tags.is_empty() || params.q.is_some();

    // Always load type_stats - it's now O(1) from file_categories table
    let state_types = state.clone();
    let type_stats_handle = tokio::task::spawn_blocking(move || {
        state_types.doc_repo.get_category_stats(None).unwrap_or_default()
    });

    // Only load tags and sources when no filters (they're slower and less needed when filtering)
    let (tags_handle, sources_handle) = if has_filters {
        (None, None)
    } else {
        let state_tags = state.clone();
        let tags_handle = Some(tokio::task::spawn_blocking(move || {
            state_tags.doc_repo.get_all_tags().unwrap_or_default()
        }));

        let state_sources = state.clone();
        let sources_handle = Some(tokio::task::spawn_blocking(move || {
            let counts = state_sources.doc_repo.get_all_source_counts().unwrap_or_default();
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

    // Await browse result (always)
    let browse_res = browse_handle.await;

    // Await type_stats (always loaded - O(1) query)
    let type_stats_res = type_stats_handle.await;

    // Await other stats results (only if we started them)
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

    // Spawn background count computation for filtered queries (if not cached)
    if skip_count {
        let state_for_count = state.clone();
        let state_for_cache = state.clone();
        let types_bg = types.clone();
        let tags_bg = tags.clone();
        let source_bg = params.source.clone();
        let q_bg = params.q.clone();

        // Precompute cache key before spawning
        let cache_key = super::cache::StatsCache::browse_count_key(
            source_bg.as_deref(),
            &types_bg,
            &tags_bg,
            q_bg.as_deref(),
        );

        tokio::spawn(async move {
            // Compute count in blocking task
            if let Ok(count) = tokio::task::spawn_blocking(move || {
                state_for_count
                    .doc_repo
                    .browse_count(&types_bg, &tags_bg, source_bg.as_deref(), q_bg.as_deref())
            })
            .await
            {
                if let Ok(count) = count {
                    state_for_cache.stats_cache.set_browse_count(cache_key, count);
                }
            }
        });
    }

    // Category stats are already aggregated from get_category_stats
    let type_stats: Vec<(String, u64)> = type_stats_res.unwrap_or_else(|_| Vec::new());

    let all_tags: Vec<(String, usize)> = tags_res.and_then(|r| r.ok()).unwrap_or_default();
    let sources: Vec<(String, String, u64)> = sources_res.and_then(|r| r.ok()).unwrap_or_default();

    // Build timeline data from the filtered documents
    let timeline = build_timeline_data(&browse_result.documents);
    let timeline_json = serde_json::to_string(&timeline).unwrap_or_else(|_| "{}".to_string());

    // Transform documents for display
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

/// API endpoint for tag autocomplete.
pub async fn api_search_tags(
    State(state): State<AppState>,
    Query(params): Query<TagSearchParams>,
) -> impl IntoResponse {
    let query = params.q.unwrap_or_default();
    let limit = params.limit.unwrap_or(20);

    match state.doc_repo.search_tags(&query, limit) {
        Ok(tags) => {
            let result: Vec<_> = tags
                .iter()
                .map(|(tag, count)| serde_json::json!({ "tag": tag, "count": count }))
                .collect();
            axum::Json(result).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct TagSearchParams {
    pub q: Option<String>,
    pub limit: Option<usize>,
}

fn mime_to_category(mime: &str) -> &'static str {
    match mime {
        // Documents: PDFs, Word docs, emails, text files
        "application/pdf" => "documents",
        m if m.contains("word") || m == "application/msword" => "documents",
        m if m.contains("rfc822") || m.contains("message") => "documents",
        m if m.starts_with("text/") && !m.contains("csv") => "documents",
        // Data: spreadsheets, CSV, JSON, XML
        m if m.contains("excel")
            || m.contains("spreadsheet")
            || m == "text/csv"
            || m == "application/json"
            || m == "application/xml" =>
        {
            "data"
        }
        // Images
        m if m.starts_with("image/") => "images",
        _ => "other",
    }
}

/// Request body for re-OCR API.
#[derive(Debug, Deserialize)]
pub struct ReOcrRequest {
    /// Backend to use (currently only "deepseek" supported)
    #[serde(default = "default_backend")]
    pub backend: String,
}

fn default_backend() -> String {
    "deepseek".to_string()
}

/// Response for re-OCR API.
#[derive(Debug, Serialize)]
pub struct ReOcrResponse {
    pub document_id: String,
    pub backend: String,
    pub pages_processed: u32,
    pub pages_total: u32,
    pub status: String,
    pub message: Option<String>,
}

/// Trigger re-OCR for a document using an alternative backend.
/// POST /api/documents/{id}/reocr
///
/// This starts a background job and returns immediately.
/// Poll GET /api/documents/reocr/status for progress.
pub async fn api_reocr_document(
    State(state): State<AppState>,
    Path(document_id): Path<String>,
    axum::Json(request): axum::Json<ReOcrRequest>,
) -> impl IntoResponse {
    use crate::ocr::{DeepSeekBackend, OcrBackend, OcrConfig};

    // Validate backend
    if request.backend != "deepseek" {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "error".to_string(),
            message: Some("Only 'deepseek' backend is currently supported".to_string()),
        })
        .into_response();
    }

    // Check if a job is already running
    {
        let job_status = state.deepseek_job.read().await;
        if job_status.document_id.is_some() && !job_status.completed {
            return (
                StatusCode::CONFLICT,
                axum::Json(ReOcrResponse {
                    document_id: document_id.clone(),
                    backend: request.backend,
                    pages_processed: job_status.pages_processed,
                    pages_total: job_status.total_pages,
                    status: "busy".to_string(),
                    message: Some(format!(
                        "DeepSeek OCR is already running on document '{}' ({}/{} pages)",
                        job_status.document_id.as_ref().unwrap_or(&"unknown".to_string()),
                        job_status.pages_processed,
                        job_status.total_pages
                    )),
                }),
            )
                .into_response();
        }
    }

    // Get document
    let doc = match state.doc_repo.get(&document_id) {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(ReOcrResponse {
                    document_id,
                    backend: request.backend,
                    pages_processed: 0,
                    pages_total: 0,
                    status: "error".to_string(),
                    message: Some("Document not found".to_string()),
                }),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(ReOcrResponse {
                    document_id,
                    backend: request.backend,
                    pages_processed: 0,
                    pages_total: 0,
                    status: "error".to_string(),
                    message: Some(format!("Database error: {}", e)),
                }),
            )
                .into_response();
        }
    };

    // Get latest version's file path
    let version = match doc.versions.last() {
        Some(v) => v,
        None => {
            return axum::Json(ReOcrResponse {
                document_id,
                backend: request.backend,
                pages_processed: 0,
                pages_total: 0,
                status: "error".to_string(),
                message: Some("Document has no versions".to_string()),
            })
            .into_response();
        }
    };

    // Only support PDF for now
    if version.mime_type != "application/pdf" {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "error".to_string(),
            message: Some("Only PDF documents are supported for re-OCR".to_string()),
        })
        .into_response();
    }

    let pdf_path = state.documents_dir.join(&version.file_path);

    // Check if DeepSeek is available
    let config = OcrConfig {
        use_gpu: true,
        ..Default::default()
    };
    let backend = DeepSeekBackend::with_config(config);

    if !backend.is_available() {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "error".to_string(),
            message: Some(format!(
                "DeepSeek backend not available. {}",
                backend.availability_hint()
            )),
        })
        .into_response();
    }

    // Get pages without DeepSeek OCR
    let pages_needing_ocr = match state
        .doc_repo
        .get_pages_without_backend(&document_id, "deepseek")
    {
        Ok(pages) => pages,
        Err(e) => {
            return axum::Json(ReOcrResponse {
                document_id,
                backend: request.backend,
                pages_processed: 0,
                pages_total: 0,
                status: "error".to_string(),
                message: Some(format!("Failed to get pages: {}", e)),
            })
            .into_response();
        }
    };

    if pages_needing_ocr.is_empty() {
        return axum::Json(ReOcrResponse {
            document_id,
            backend: request.backend,
            pages_processed: 0,
            pages_total: 0,
            status: "complete".to_string(),
            message: Some("All pages already have DeepSeek OCR results".to_string()),
        })
        .into_response();
    }

    let total_pages = pages_needing_ocr.len() as u32;

    // Initialize job status
    {
        let mut job_status = state.deepseek_job.write().await;
        *job_status = super::DeepSeekJobStatus {
            document_id: Some(document_id.clone()),
            pages_processed: 0,
            total_pages,
            error: None,
            completed: false,
        };
    }

    // Clone what we need for the background task
    let job_state = state.clone();
    let job_doc_id = document_id.clone();

    // Spawn background task
    tokio::spawn(async move {
        let mut processed = 0u32;

        for (page_id, page_number) in pages_needing_ocr {
            // Run OCR in blocking task to not block async runtime
            let pdf_path_clone = pdf_path.clone();
            let ocr_result = tokio::task::spawn_blocking(move || {
                let config = OcrConfig {
                    use_gpu: true,
                    ..Default::default()
                };
                let backend = DeepSeekBackend::with_config(config);
                backend.ocr_pdf_page(&pdf_path_clone, page_number as u32)
            })
            .await;

            match ocr_result {
                Ok(Ok(result)) => {
                    // Store result
                    if let Err(e) = job_state.doc_repo.store_page_ocr_result(
                        page_id,
                        "deepseek",
                        Some(&result.text),
                        result.confidence.map(|c| c as f64),
                        Some(result.processing_time_ms),
                    ) {
                        tracing::error!(
                            "Failed to store OCR result for page {}: {}",
                            page_number,
                            e
                        );
                    } else {
                        processed += 1;
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!("OCR failed for page {}: {:?}", page_number, e);
                    // Store error as null text
                    let _ = job_state.doc_repo.store_page_ocr_result(
                        page_id, "deepseek", None, None, None,
                    );
                }
                Err(e) => {
                    tracing::error!("Task panic for page {}: {:?}", page_number, e);
                }
            }

            // Update progress
            {
                let mut job_status = job_state.deepseek_job.write().await;
                job_status.pages_processed = processed;
            }
        }

        // Mark job complete
        {
            let mut job_status = job_state.deepseek_job.write().await;
            job_status.pages_processed = processed;
            job_status.completed = true;
        }

        tracing::info!(
            "DeepSeek OCR complete for {}: {}/{} pages",
            job_doc_id,
            processed,
            total_pages
        );
    });

    // Return immediately
    axum::Json(ReOcrResponse {
        document_id,
        backend: request.backend,
        pages_processed: 0,
        pages_total: total_pages,
        status: "started".to_string(),
        message: Some(format!(
            "DeepSeek OCR started for {} pages. Poll /api/documents/reocr/status for progress.",
            total_pages
        )),
    })
    .into_response()
}

/// Get the current status of a DeepSeek OCR job.
/// GET /api/documents/reocr/status
pub async fn api_reocr_status(State(state): State<AppState>) -> impl IntoResponse {
    let job_status = state.deepseek_job.read().await;

    let (status, document_id) = if job_status.document_id.is_none() {
        ("idle".to_string(), String::new())
    } else if job_status.completed {
        ("complete".to_string(), job_status.document_id.clone().unwrap_or_default())
    } else {
        ("running".to_string(), job_status.document_id.clone().unwrap_or_default())
    };

    axum::Json(ReOcrResponse {
        document_id,
        backend: "deepseek".to_string(),
        pages_processed: job_status.pages_processed,
        pages_total: job_status.total_pages,
        status,
        message: job_status.error.clone(),
    })
}
