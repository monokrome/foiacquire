//! HTML templates for the web interface.
//!
//! Includes a Wayback Machine-style sticky header with timeline controls.

#![allow(dead_code)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

use crate::models::{VirtualFile, VirtualFileStatus};
use crate::utils::{format_size, html_escape, mime_icon, MimeCategory};
use chrono::{DateTime, Utc};

/// Base HTML template with timeline ruler.
pub fn base_template(title: &str, content: &str, timeline_data: Option<&str>) -> String {
    let timeline_section = if let Some(data) = timeline_data {
        format!(
            r#"
        <div id="timeline-container">
            <div id="timeline-header">
                <div id="timeline-info">
                    <span id="date-range">All dates</span>
                    <span id="doc-count"></span>
                    <button id="reset-timeline" class="btn-small">reset</button>
                </div>
                <div id="timeline-ruler">
                    <div id="ruler-track"></div>
                    <div id="ruler-selection"></div>
                    <div id="ruler-labels"></div>
                </div>
                <div id="timeline-controls">
                    <span>from</span>
                    <input type="range" id="start-range" min="0" max="100" value="0">
                    <span>to</span>
                    <input type="range" id="end-range" min="0" max="100" value="100">
                </div>
            </div>
        </div>
        <script>
            window.TIMELINE_DATA = {};
        </script>
        "#,
            data
        )
    } else {
        String::new()
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{} - FOIAcquire</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <header id="main-header">
        <nav>
            <a href="/" class="logo">FOIAcquire</a>
            <a href="/tags">tags</a>
        </nav>
    </header>
    {}
    <main>
        <h1>{}</h1>
        {}
    </main>
    <script src="/static/timeline.js"></script>
</body>
</html>"#,
        title, timeline_section, title, content
    )
}

/// Render the source listing page.
pub fn sources_list(sources: &[(String, String, u64, Option<DateTime<Utc>>)]) -> String {
    let mut rows = String::new();

    for (id, name, doc_count, last_scraped) in sources {
        let last_scraped_str = last_scraped
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "Never".to_string());

        rows.push_str(&format!(
            r#"
        <tr>
            <td><a href="/sources/{}">{}/</a></td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
            id, name, doc_count, last_scraped_str
        ));
    }

    format!(
        r#"
    <table class="file-listing">
        <thead>
            <tr>
                <th>Source</th>
                <th>Documents</th>
                <th>Last Scraped</th>
            </tr>
        </thead>
        <tbody>
            {}
        </tbody>
    </table>
    "#,
        rows
    )
}

/// Render a document listing for a source.
pub fn document_list(
    source_name: &str,
    documents: &[(String, String, String, u64, DateTime<Utc>, Vec<String>)],
) -> String {
    let mut rows = String::new();

    for (id, title, mime_type, size, acquired_at, other_sources) in documents {
        let icon = mime_icon(mime_type);
        let size_str = format_size(*size);
        let date_str = acquired_at.format("%Y-%m-%d %H:%M").to_string();

        // Show symlink indicator if document exists in other sources
        let symlink = if !other_sources.is_empty() {
            format!(
                r#" <span class="symlink" title="Also in: {}">[+{}]</span>"#,
                other_sources.join(", "),
                other_sources.len()
            )
        } else {
            String::new()
        };

        rows.push_str(&format!(
            r#"
        <tr data-date="{}">
            <td><a href="/documents/{}">{} {}</a>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
            acquired_at.timestamp(),
            id,
            icon,
            title,
            symlink,
            mime_type,
            size_str,
            date_str
        ));
    }

    format!(
        r#"
    <nav class="breadcrumb">
        <a href="/sources">Sources</a> / {}
    </nav>
    <table class="file-listing" id="document-table">
        <thead>
            <tr>
                <th>Document</th>
                <th>Type</th>
                <th>Size</th>
                <th>Acquired</th>
            </tr>
        </thead>
        <tbody>
            {}
        </tbody>
    </table>
    "#,
        source_name, rows
    )
}

/// Render document detail page with page viewer as main focus.
pub fn document_detail(
    doc_id: &str,
    title: &str,
    source_id: &str,
    source_url: &str,
    versions: &[(
        String,
        String,
        u64,
        DateTime<Utc>,
        Option<String>,
        Option<DateTime<Utc>>,
    )],
    other_sources: &[String],
    extracted_text: Option<&str>,
    _synopsis: Option<&str>, // Not used in detail view (for index only)
    virtual_files: &[VirtualFile],
    prev_id: Option<&str>,
    prev_title: Option<&str>,
    next_id: Option<&str>,
    next_title: Option<&str>,
    position: u64,
    total: u64,
    nav_query_string: &str,
    page_count: Option<u32>,
    current_version_id: Option<i64>,
) -> String {
    // Build version timeline (compact horizontal display)
    let version_timeline = if !versions.is_empty() {
        let mut items = String::new();
        for (i, (_hash, path, size, acquired_at, original_filename, server_date)) in
            versions.iter().enumerate()
        {
            let is_current = i == 0;
            let date_str = server_date
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| acquired_at.format("%Y-%m-%d").to_string());
            let size_str = format_size(*size);
            let filename = original_filename
                .as_ref()
                .map(|f| html_escape(f))
                .unwrap_or_else(|| "unknown".to_string());

            items.push_str(&format!(
                r#"<a href="/files/{}" class="version-item{}" title="{} ({})">
                    <span class="version-date">{}</span>
                    <span class="version-size">{}</span>
                </a>"#,
                path,
                if is_current { " current" } else { "" },
                filename,
                size_str,
                date_str,
                size_str
            ));
        }
        format!(
            r#"<div class="version-timeline"><span class="timeline-label">Versions:</span>{}</div>"#,
            items
        )
    } else {
        String::new()
    };

    let other_sources_section = if !other_sources.is_empty() {
        format!(
            r#"<div class="also-in-compact">Also in: {}</div>"#,
            other_sources
                .iter()
                .map(|s| format!("<a href=\"/sources/{}\">{}</a>", s, s))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };

    // Build document navigation
    let doc_nav = if total > 0 {
        let prev_link = if let (Some(id), Some(title)) = (prev_id, prev_title) {
            let title_preview: String = title.chars().take(40).collect();
            let ellipsis = if title.len() > 40 { "..." } else { "" };
            format!(
                r#"<a href="/documents/{}{}" class="doc-nav-link prev" title="{}">« {}{}</a>"#,
                id,
                nav_query_string,
                html_escape(title),
                html_escape(&title_preview),
                ellipsis
            )
        } else {
            String::new()
        };

        let next_link = if let (Some(id), Some(title)) = (next_id, next_title) {
            let title_preview: String = title.chars().take(40).collect();
            let ellipsis = if title.len() > 40 { "..." } else { "" };
            format!(
                r#"<a href="/documents/{}{}" class="doc-nav-link next" title="{}">{}{}  »</a>"#,
                id,
                nav_query_string,
                html_escape(title),
                html_escape(&title_preview),
                ellipsis
            )
        } else {
            String::new()
        };

        let position_str = if position > 0 {
            format!(
                r#"<span class="doc-position">{} of {}</span>"#,
                position, total
            )
        } else {
            String::new()
        };

        format!(
            r#"<nav class="doc-navigation">{}{}{}</nav>"#,
            prev_link, position_str, next_link
        )
    } else {
        String::new()
    };

    // Main page viewer - this is the focus of the detail view
    let pages_section = if let (Some(count), Some(version_id)) = (page_count, current_version_id) {
        if count > 0 {
            format!(
                r#"
            <div id="pages-container"
                 class="page-viewer"
                 data-doc-id="{}"
                 data-version-id="{}"
                 data-total-pages="{}"
                 data-loaded="0">
                <div id="pages-list"></div>
                <div id="pages-loading" class="loading-indicator">Loading pages...</div>
                <div id="pages-end" class="pages-end" style="display:none">End of document ({} pages)</div>
            </div>

            <script>
            (function() {{
                const container = document.getElementById('pages-container');
                const pagesList = document.getElementById('pages-list');
                const loadingIndicator = document.getElementById('pages-loading');
                const endIndicator = document.getElementById('pages-end');

                const docId = container.dataset.docId;
                const versionId = container.dataset.versionId;
                const totalPages = parseInt(container.dataset.totalPages);

                let loadedPages = 0;
                let isLoading = false;
                let hasMore = true;
                const PAGES_PER_LOAD = 3;

                async function loadMorePages() {{
                    if (isLoading || !hasMore) return;

                    isLoading = true;
                    loadingIndicator.style.display = 'block';

                    try {{
                        const response = await fetch(
                            `/api/documents/${{docId}}/pages?version=${{versionId}}&offset=${{loadedPages}}&limit=${{PAGES_PER_LOAD}}`
                        );

                        if (!response.ok) throw new Error('Failed to load pages');

                        const data = await response.json();

                        for (const page of data.pages) {{
                            const pageEl = createPageElement(page);
                            pagesList.appendChild(pageEl);
                        }}

                        loadedPages += data.pages.length;
                        hasMore = data.has_more;

                        if (!hasMore) {{
                            loadingIndicator.style.display = 'none';
                            endIndicator.style.display = 'block';
                        }}
                    }} catch (err) {{
                        console.error('Error loading pages:', err);
                        loadingIndicator.textContent = 'Error loading pages. Click to retry.';
                        loadingIndicator.onclick = () => {{
                            loadingIndicator.textContent = 'Loading pages...';
                            loadingIndicator.onclick = null;
                            isLoading = false;
                            loadMorePages();
                        }};
                    }} finally {{
                        isLoading = false;
                    }}
                }}

                function createPageElement(page) {{
                    const div = document.createElement('div');
                    div.className = 'page-item';
                    div.id = `page-${{page.page_number}}`;

                    const content = document.createElement('div');
                    content.className = 'page-content';

                    // Image column (left)
                    const imageCol = document.createElement('div');
                    imageCol.className = 'page-image-col';
                    if (page.image_base64) {{
                        const img = document.createElement('img');
                        img.src = page.image_base64;
                        img.alt = `Page ${{page.page_number}}`;
                        img.className = 'page-image';
                        img.loading = 'lazy';
                        imageCol.appendChild(img);
                    }} else {{
                        imageCol.innerHTML = '<div class="no-image">No preview</div>';
                    }}

                    // Text column (right)
                    const textCol = document.createElement('div');
                    textCol.className = 'page-text-col';

                    const header = document.createElement('div');
                    header.className = 'page-text-header';

                    const originalText = page.final_text || page.ocr_text || page.pdf_text || '';
                    const deepseekText = page.deepseek_text || '';
                    const hasComparison = !!deepseekText;

                    if (hasComparison) {{
                        // Show tabs for comparison
                        header.innerHTML = `
                            <span class="page-num">Page ${{page.page_number}}</span>
                            <div class="ocr-tabs">
                                <button class="ocr-tab active" data-tab="original">Original</button>
                                <button class="ocr-tab" data-tab="deepseek">DeepSeek</button>
                            </div>
                        `;

                        const originalPre = document.createElement('pre');
                        originalPre.className = 'page-text ocr-panel active';
                        originalPre.dataset.panel = 'original';
                        originalPre.textContent = originalText || '(No text extracted)';

                        const deepseekPre = document.createElement('pre');
                        deepseekPre.className = 'page-text ocr-panel';
                        deepseekPre.dataset.panel = 'deepseek';
                        deepseekPre.textContent = deepseekText;

                        textCol.appendChild(header);
                        textCol.appendChild(originalPre);
                        textCol.appendChild(deepseekPre);

                        // Tab switching
                        header.querySelectorAll('.ocr-tab').forEach(tab => {{
                            tab.addEventListener('click', () => {{
                                const target = tab.dataset.tab;
                                header.querySelectorAll('.ocr-tab').forEach(t => t.classList.remove('active'));
                                tab.classList.add('active');
                                textCol.querySelectorAll('.ocr-panel').forEach(p => {{
                                    p.classList.toggle('active', p.dataset.panel === target);
                                }});
                            }});
                        }});
                    }} else {{
                        header.innerHTML = `<span class="page-num">Page ${{page.page_number}}</span>`;
                        const pre = document.createElement('pre');
                        pre.className = 'page-text';
                        pre.textContent = originalText || '(No text extracted)';
                        textCol.appendChild(header);
                        textCol.appendChild(pre);
                    }}

                    content.appendChild(imageCol);
                    content.appendChild(textCol);
                    div.appendChild(content);

                    return div;
                }}

                // Intersection Observer for infinite scroll
                const observer = new IntersectionObserver((entries) => {{
                    for (const entry of entries) {{
                        if (entry.isIntersecting && hasMore) {{
                            loadMorePages();
                        }}
                    }}
                }}, {{
                    rootMargin: '400px'
                }});

                observer.observe(loadingIndicator);

                // Initial load
                loadMorePages();
            }})();
            </script>
            "#,
                doc_id, version_id, count, count
            )
        } else {
            // No pages, show full extracted text if available
            if let Some(text) = extracted_text {
                format!(
                    r#"<div class="page-viewer fallback-text">
                        <pre class="extracted-text-full">{}</pre>
                    </div>"#,
                    html_escape(text)
                )
            } else {
                String::new()
            }
        }
    } else {
        // No page data, show full extracted text if available
        if let Some(text) = extracted_text {
            format!(
                r#"<div class="page-viewer fallback-text">
                    <pre class="extracted-text-full">{}</pre>
                </div>"#,
                html_escape(text)
            )
        } else {
            String::new()
        }
    };

    // Re-OCR section with DeepSeek button
    let reocr_section = if page_count.is_some() && page_count.unwrap() > 0 {
        format!(
            r#"
            <div class="reocr-section">
                <button id="reocr-btn" class="btn-action" data-doc-id="{}">
                    Run DeepSeek OCR
                </button>
                <span id="reocr-status"></span>
            </div>
            <script>
            (function() {{
                const btn = document.getElementById('reocr-btn');
                const status = document.getElementById('reocr-status');
                if (!btn) return;

                let pollInterval = null;

                async function pollStatus() {{
                    try {{
                        const resp = await fetch('/api/documents/reocr/status');
                        const data = await resp.json();

                        if (data.status === 'running') {{
                            status.textContent = `Processing: ${{data.pages_processed}}/${{data.pages_total}} pages...`;
                            status.className = 'reocr-progress';
                        }} else if (data.status === 'complete') {{
                            clearInterval(pollInterval);
                            pollInterval = null;
                            status.textContent = `Completed: ${{data.pages_processed}}/${{data.pages_total}} pages`;
                            status.className = 'reocr-success';
                            btn.disabled = false;
                            btn.textContent = 'Re-run DeepSeek OCR';
                            // Reload page to show new OCR results
                            if (data.pages_processed > 0) {{
                                setTimeout(() => location.reload(), 1500);
                            }}
                        }} else if (data.status === 'idle') {{
                            clearInterval(pollInterval);
                            pollInterval = null;
                            btn.disabled = false;
                            btn.textContent = 'Run DeepSeek OCR';
                        }}
                    }} catch (err) {{
                        console.error('Poll error:', err);
                    }}
                }}

                btn.addEventListener('click', async function() {{
                    const docId = btn.dataset.docId;
                    btn.disabled = true;
                    btn.textContent = 'Starting...';
                    status.textContent = 'Initializing DeepSeek OCR...';
                    status.className = 'reocr-progress';

                    try {{
                        const response = await fetch(`/api/documents/${{docId}}/reocr`, {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify({{ backend: 'deepseek' }})
                        }});

                        const data = await response.json();

                        if (data.status === 'started') {{
                            // Start polling for progress
                            btn.textContent = 'Running...';
                            status.textContent = `Processing: 0/${{data.pages_total}} pages...`;
                            pollInterval = setInterval(pollStatus, 2000);
                        }} else if (data.status === 'busy') {{
                            // Another job is running
                            status.textContent = data.message || 'Another OCR job is running';
                            status.className = 'reocr-error';
                            btn.disabled = false;
                            btn.textContent = 'Run DeepSeek OCR';
                        }} else if (data.status === 'complete') {{
                            // Already done
                            status.textContent = 'All pages already have DeepSeek OCR results';
                            status.className = 'reocr-success';
                            btn.disabled = false;
                            btn.textContent = 'Re-run DeepSeek OCR';
                        }} else if (data.status === 'error') {{
                            status.textContent = data.message || 'OCR failed';
                            status.className = 'reocr-error';
                            btn.disabled = false;
                            btn.textContent = 'Retry DeepSeek OCR';
                        }}
                    }} catch (err) {{
                        status.textContent = `Error: ${{err.message}}`;
                        status.className = 'reocr-error';
                        btn.disabled = false;
                        btn.textContent = 'Retry DeepSeek OCR';
                    }}
                }});

                // Check if a job is already running when page loads
                pollStatus();
            }})();
            </script>
            "#,
            doc_id
        )
    } else {
        String::new()
    };

    // Archive contents section (for ZIP/archive files)
    let archive_section = if !virtual_files.is_empty() {
        let mut file_rows = String::new();
        for vf in virtual_files {
            let icon = mime_icon(&vf.mime_type);
            let size_str = format_size(vf.file_size);
            let status_badge = match vf.status {
                VirtualFileStatus::Pending => {
                    r#"<span class="status-badge pending">pending</span>"#
                }
                VirtualFileStatus::OcrComplete => {
                    r#"<span class="status-badge complete">OCR</span>"#
                }
                VirtualFileStatus::Failed => r#"<span class="status-badge failed">failed</span>"#,
                VirtualFileStatus::Unsupported => {
                    r#"<span class="status-badge unsupported">—</span>"#
                }
            };

            file_rows.push_str(&format!(
                r#"<tr class="archive-file" data-vf-id="{}">
                    <td><span class="vf-icon">{}</span> {}</td>
                    <td>{}</td>
                    <td>{}</td>
                    <td>{}</td>
                </tr>"#,
                vf.id,
                icon,
                html_escape(&vf.filename),
                vf.mime_type,
                size_str,
                status_badge
            ));
        }

        let total = virtual_files.len();
        format!(
            r#"
        <section class="archive-contents">
            <h3>Archive Contents ({} files)</h3>
            <table class="file-listing archive-listing">
                <thead>
                    <tr><th>File</th><th>Type</th><th>Size</th><th>Status</th></tr>
                </thead>
                <tbody>{}</tbody>
            </table>
        </section>
        "#,
            total, file_rows
        )
    } else {
        String::new()
    };

    format!(
        r#"
    <div class="document-header">
        <nav class="breadcrumb">
            <a href="/">Browse</a> /
            <a href="/?source={}">{}</a> /
            <span class="current">{}</span>
        </nav>
        {}
        <h1 class="document-title">{}</h1>
        <div class="document-meta-compact">
            <a href="{}" target="_blank" class="source-link">{}</a>
            {}
        </div>
        {}
    </div>

    {}

    {}

    {}

    {}
    "#,
        source_id,
        source_id,
        html_escape(title),
        doc_nav,
        html_escape(title),
        source_url,
        source_url,
        other_sources_section,
        version_timeline,
        pages_section,
        reocr_section,
        archive_section,
        doc_nav
    )
}

/// Render duplicates list page.
pub fn duplicates_list(duplicates: &[(String, Vec<(String, String, String)>)]) -> String {
    if duplicates.is_empty() {
        return "<p>No duplicate documents found across sources.</p>".to_string();
    }

    let mut sections = String::new();

    for (content_hash, docs) in duplicates {
        let mut doc_list = String::new();
        for (doc_id, source_id, title) in docs {
            doc_list.push_str(&format!(
                r#"
            <li>
                <a href="/documents/{}">{}</a>
                from <a href="/sources/{}">{}</a>
            </li>
            "#,
                doc_id, title, source_id, source_id
            ));
        }

        sections.push_str(&format!(
            r#"
        <div class="duplicate-group">
            <h3>Hash: <code>{}</code></h3>
            <ul>{}</ul>
        </div>
        "#,
            &content_hash[..16],
            doc_list
        ));
    }

    format!(
        r#"
    <p>Documents with identical content found in multiple sources:</p>
    {}
    "#,
        sections
    )
}

/// Render the tags listing page.
pub fn tags_list(tags: &[(String, usize)]) -> String {
    if tags.is_empty() {
        return "<p>No tags found. Run 'foiacquire summarize' to generate tags for your documents.</p>".to_string();
    }

    let mut tag_items = String::new();
    for (tag, count) in tags {
        tag_items.push_str(&format!(
            r#"<a href="/tags/{}" class="tag-chip">{} <span class="tag-count">{}</span></a>"#,
            urlencoding::encode(tag),
            html_escape(tag),
            count
        ));
    }

    format!(
        r#"
    <nav class="breadcrumb">
        <a href="/tags">Tags</a>
    </nav>
    <p>Click a tag to view all documents with that tag:</p>
    <div class="tag-cloud">
        {}
    </div>
    "#,
        tag_items
    )
}

/// Render documents filtered by tag.
pub fn tag_documents(
    tag: &str,
    documents: &[(
        String,
        String,
        String,
        String,
        u64,
        DateTime<Utc>,
        Option<String>,
        Vec<String>,
    )],
) -> String {
    let mut rows = String::new();

    for (id, title, source_id, mime_type, size, acquired_at, synopsis, doc_tags) in documents {
        let icon = mime_icon(mime_type);
        let size_str = format_size(*size);
        let date_str = acquired_at.format("%Y-%m-%d %H:%M").to_string();

        // Synopsis preview
        let synopsis_str = synopsis
            .as_ref()
            .map(|s| {
                let preview: String = s.chars().take(100).collect();
                format!(
                    r#"<div class="synopsis">{}{}</div>"#,
                    html_escape(&preview),
                    if s.len() > 100 { "..." } else { "" }
                )
            })
            .unwrap_or_default();

        // Other tags for this document
        let other_tags: String = doc_tags
            .iter()
            .filter(|t| t.to_lowercase() != tag.to_lowercase())
            .take(5)
            .map(|t| {
                format!(
                    r#"<a href="/tags/{}" class="tag-small">{}</a>"#,
                    urlencoding::encode(t),
                    html_escape(t)
                )
            })
            .collect::<Vec<_>>()
            .join(" ");

        rows.push_str(&format!(
            r#"
        <tr data-date="{}">
            <td>
                <a href="/documents/{}">{} {}</a>
                {}
                <div class="doc-tags">{}</div>
            </td>
            <td><a href="/sources/{}">{}</a></td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
            acquired_at.timestamp(),
            id,
            icon,
            html_escape(title),
            synopsis_str,
            other_tags,
            source_id,
            source_id,
            mime_type,
            size_str,
            date_str
        ));
    }

    format!(
        r#"
    <nav class="breadcrumb">
        <a href="/tags">Tags</a> / {}
    </nav>
    <p>{} documents with tag "{}"</p>
    <table class="file-listing" id="document-table">
        <thead>
            <tr>
                <th>Document</th>
                <th>Source</th>
                <th>Type</th>
                <th>Size</th>
                <th>Acquired</th>
            </tr>
        </thead>
        <tbody>
            {}
        </tbody>
    </table>
    "#,
        html_escape(tag),
        documents.len(),
        html_escape(tag),
        rows
    )
}

/// Render the types listing page with category tabs.
pub fn types_list(type_stats: &[(String, String, u64)]) -> String {
    // Group stats by category
    let mut category_counts: std::collections::HashMap<&str, u64> =
        std::collections::HashMap::new();
    for (category, _, count) in type_stats {
        *category_counts.entry(category.as_str()).or_default() += count;
    }

    let mut tabs = String::new();
    for (cat_id, cat_name) in MimeCategory::all() {
        let count = category_counts.get(*cat_id).unwrap_or(&0);
        if *count > 0 {
            tabs.push_str(&format!(
                r#"<a href="/types/{}" class="type-tab">{} <span class="count">{}</span></a>"#,
                cat_id, cat_name, count
            ));
        }
    }

    // Also show detailed breakdown
    let mut rows = String::new();
    for (category, mime_type, count) in type_stats {
        rows.push_str(&format!(
            r#"
        <tr>
            <td><a href="/types/{}">{}</a></td>
            <td><code>{}</code></td>
            <td>{}</td>
        </tr>
        "#,
            category, category, mime_type, count
        ));
    }

    format!(
        r#"
    <nav class="breadcrumb">
        <a href="/types">Types</a>
    </nav>
    <div class="type-tabs">
        {}
    </div>
    <h2>MIME Type Breakdown</h2>
    <table class="file-listing">
        <thead>
            <tr>
                <th>Category</th>
                <th>MIME Type</th>
                <th>Count</th>
            </tr>
        </thead>
        <tbody>
            {}
        </tbody>
    </table>
    "#,
        tabs, rows
    )
}

/// Render documents filtered by type with category tabs.
pub fn type_documents(
    type_name: &str,
    documents: &[(
        String,
        String,
        String,
        String,
        u64,
        DateTime<Utc>,
        Option<String>,
        Vec<String>,
    )],
    type_stats: Option<&[(String, u64)]>,
) -> String {
    let mut rows = String::new();

    for (id, title, source_id, mime_type, size, acquired_at, synopsis, doc_tags) in documents {
        let icon = mime_icon(mime_type);
        let size_str = format_size(*size);
        let date_str = acquired_at.format("%Y-%m-%d %H:%M").to_string();

        // Synopsis preview
        let synopsis_str = synopsis
            .as_ref()
            .map(|s| {
                let preview: String = s.chars().take(100).collect();
                format!(
                    r#"<div class="synopsis">{}{}</div>"#,
                    html_escape(&preview),
                    if s.len() > 100 { "..." } else { "" }
                )
            })
            .unwrap_or_default();

        // Tags
        let tags_str: String = doc_tags
            .iter()
            .take(5)
            .map(|t| {
                format!(
                    r#"<a href="/tags/{}" class="tag-small">{}</a>"#,
                    urlencoding::encode(t),
                    html_escape(t)
                )
            })
            .collect::<Vec<_>>()
            .join(" ");

        rows.push_str(&format!(
            r#"
        <tr data-date="{}">
            <td>
                <a href="/documents/{}">{} {}</a>
                {}
                <div class="doc-tags">{}</div>
            </td>
            <td><a href="/sources/{}">{}</a></td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
            acquired_at.timestamp(),
            id,
            icon,
            html_escape(title),
            synopsis_str,
            tags_str,
            source_id,
            source_id,
            mime_type,
            size_str,
            date_str
        ));
    }

    // Build category tabs
    let mut tabs = String::new();
    if let Some(stats) = type_stats {
        for (cat_id, cat_name) in MimeCategory::all() {
            let count = stats
                .iter()
                .find(|(c, _)| c == *cat_id)
                .map(|(_, n)| *n)
                .unwrap_or(0);
            if count > 0 {
                let active = if *cat_id == type_name { " active" } else { "" };
                tabs.push_str(&format!(
                    r#"<a href="/types/{}" class="type-tab{}">{} <span class="count">{}</span></a>"#,
                    cat_id, active, cat_name, count
                ));
            }
        }
    }

    let tabs_html = if !tabs.is_empty() {
        format!(r#"<div class="type-tabs">{}</div>"#, tabs)
    } else {
        String::new()
    };

    format!(
        r#"
    <nav class="breadcrumb">
        <a href="/types">Types</a> / {}
    </nav>
    {}
    <p>{} documents</p>
    <table class="file-listing" id="document-table">
        <thead>
            <tr>
                <th>Document</th>
                <th>Source</th>
                <th>Type</th>
                <th>Size</th>
                <th>Acquired</th>
            </tr>
        </thead>
        <tbody>
            {}
        </tbody>
    </table>
    "#,
        html_escape(type_name),
        tabs_html,
        documents.len(),
        rows
    )
}

/// Unified browse page with type toggles, tag search, and source filter.
pub fn browse_page(
    documents: &[(
        String,
        String,
        String,
        String,
        u64,
        DateTime<Utc>,
        Option<String>,
        Vec<String>,
    )],
    type_stats: &[(String, u64)],
    active_types: &[String],
    active_tags: &[String],
    active_source: Option<&str>,
    all_tags: &[(String, usize)],
    sources: &[(String, String, u64)],
    prev_cursor: Option<&str>,
    next_cursor: Option<&str>,
    start_position: u64,
    total_count: u64,
    per_page: usize,
) -> String {
    // Build query string for document links to preserve navigation context
    let nav_query_string = {
        let mut qs_parts = Vec::new();
        if !active_types.is_empty() {
            qs_parts.push(format!(
                "types={}",
                urlencoding::encode(&active_types.join(","))
            ));
        }
        if !active_tags.is_empty() {
            qs_parts.push(format!(
                "tags={}",
                urlencoding::encode(&active_tags.join(","))
            ));
        }
        if let Some(source) = active_source {
            qs_parts.push(format!("source={}", urlencoding::encode(source)));
        }
        if qs_parts.is_empty() {
            String::new()
        } else {
            format!("?{}", qs_parts.join("&"))
        }
    };

    let mut rows = String::new();

    for (id, title, source_id, mime_type, size, acquired_at, synopsis, doc_tags) in documents {
        let icon = mime_icon(mime_type);
        let size_str = format_size(*size);
        let date_str = acquired_at.format("%Y-%m-%d %H:%M").to_string();

        let synopsis_str = synopsis
            .as_ref()
            .map(|s| {
                let preview: String = s.chars().take(100).collect();
                format!(
                    r#"<div class="synopsis">{}{}</div>"#,
                    html_escape(&preview),
                    if s.len() > 100 { "..." } else { "" }
                )
            })
            .unwrap_or_default();

        let tags_str: String = doc_tags
            .iter()
            .take(5)
            .map(|t| {
                format!(
                    r#"<a href="/browse?tag={}" class="tag-small">{}</a>"#,
                    urlencoding::encode(t),
                    html_escape(t)
                )
            })
            .collect::<Vec<_>>()
            .join(" ");

        rows.push_str(&format!(
            r#"
        <tr data-date="{}">
            <td>
                <a href="/documents/{}{}">{} {}</a>
                {}
                <div class="doc-tags">{}</div>
            </td>
            <td><a href="/sources/{}">{}</a></td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        "#,
            acquired_at.timestamp(),
            id,
            nav_query_string,
            icon,
            html_escape(title),
            synopsis_str,
            tags_str,
            source_id,
            source_id,
            mime_type,
            size_str,
            date_str
        ));
    }

    // Build type toggle switches - show loading placeholder if empty
    let type_toggles = if type_stats.is_empty() {
        r#"<span class="loading-placeholder" id="types-loading">Loading types...</span>"#
            .to_string()
    } else {
        let mut toggles = String::new();
        for (cat_id, cat_name) in MimeCategory::all() {
            let count = type_stats
                .iter()
                .find(|(c, _)| c == *cat_id)
                .map(|(_, n)| *n)
                .unwrap_or(0);
            if count > 0 {
                let checked =
                    if active_types.is_empty() || active_types.iter().any(|t| t == *cat_id) {
                        "checked"
                    } else {
                        ""
                    };
                toggles.push_str(&format!(
                    r#"<label class="type-toggle">
                        <input type="checkbox" name="type" value="{}" {} data-count="{}">
                        <span class="toggle-label">{}</span>
                        <span class="toggle-count">{}</span>
                    </label>"#,
                    cat_id, checked, count, cat_name, count
                ));
            }
        }
        toggles
    };

    // Active types as JSON for JS async loading
    let active_types_json: String = format!(
        "[{}]",
        active_types
            .iter()
            .map(|t| format!("\"{}\"", html_escape(t)))
            .collect::<Vec<_>>()
            .join(",")
    );

    // Build tag datalist for autocomplete (all tags, sorted by count)
    let mut tag_options = String::new();
    for (tag, count) in all_tags.iter() {
        tag_options.push_str(&format!(
            r#"<option value="{}" data-count="{}">"#,
            html_escape(tag),
            count
        ));
    }

    // Build active tags display (chips with remove buttons)
    let active_tags_display: String = active_tags.iter().enumerate().map(|(i, t)| {
        format!(
            r#"<span class="active-tag">{} <button type="button" class="clear-tag" onclick="removeTag({})">×</button></span>"#,
            html_escape(t), i
        )
    }).collect::<Vec<_>>().join(" ");

    // Build source dropdown options - show loading if empty
    let source_options = if sources.is_empty() {
        let active_opt = active_source
            .map(|s| {
                format!(
                    r#"<option value="{}" selected>{}</option>"#,
                    html_escape(s),
                    html_escape(s)
                )
            })
            .unwrap_or_default();
        format!(
            r#"<option value="">Loading sources...</option>{}"#,
            active_opt
        )
    } else {
        let mut opts = String::from(r#"<option value="">All Sources</option>"#);
        for (source_id, source_name, count) in sources {
            let selected = if active_source == Some(source_id.as_str()) {
                " selected"
            } else {
                ""
            };
            opts.push_str(&format!(
                r#"<option value="{}"{}>{}  ({})</option>"#,
                html_escape(source_id),
                selected,
                html_escape(source_name),
                count
            ));
        }
        opts
    };

    // Active source as JS string for async loading
    let active_source_js = active_source
        .map(|s| format!("\"{}\"", html_escape(s)))
        .unwrap_or_else(|| "null".to_string());

    // Active tags as JSON for JavaScript
    let active_tags_json: String = format!(
        "[{}]",
        active_tags
            .iter()
            .map(|t| format!("\"{}\"", html_escape(t)))
            .collect::<Vec<_>>()
            .join(",")
    );

    // Build cursor-based pagination controls
    let end_position =
        start_position + documents.len() as u64 - if documents.is_empty() { 0 } else { 1 };

    // Pagination with prev/next cursors
    let has_pagination = prev_cursor.is_some() || next_cursor.is_some();
    let pagination = if has_pagination {
        let mut nav = String::new();

        // Previous button
        if let Some(cursor) = prev_cursor {
            nav.push_str(&format!(
                r#"<a href="javascript:void(0)" onclick="goToPage('{}')" class="page-link">&laquo; Previous</a> "#,
                html_escape(cursor)
            ));
        }

        // Position indicator
        if start_position > 0 {
            nav.push_str(&format!(
                r#"<span class="page-position">{}-{} of {}</span> "#,
                start_position, end_position, total_count
            ));
        }

        // Next button
        if let Some(cursor) = next_cursor {
            nav.push_str(&format!(
                r#"<a href="javascript:void(0)" onclick="goToPage('{}')" class="page-link">Next &raquo;</a>"#,
                html_escape(cursor)
            ));
        }

        format!(r#"<div class="pagination">{}</div>"#, nav)
    } else if total_count > 0 {
        // No pagination needed but show count
        format!(
            r#"<div class="pagination"><span class="page-position">1-{} of {}</span></div>"#,
            documents.len().min(total_count as usize),
            total_count
        )
    } else {
        String::new()
    };

    // Cursors as JS variables
    let prev_cursor_js = prev_cursor
        .map(|c| format!("\"{}\"", html_escape(c)))
        .unwrap_or_else(|| "null".to_string());
    let next_cursor_js = next_cursor
        .map(|c| format!("\"{}\"", html_escape(c)))
        .unwrap_or_else(|| "null".to_string());

    format!(
        r#"
    <div class="browse-filters">
        <div class="filter-row">
            <div class="filter-section source-filter">
                <span class="filter-label">Source:</span>
                <select id="source-select">
                    {}
                </select>
            </div>
            <div class="filter-section tag-filter">
                <span class="filter-label">Tags:</span>
                <div class="tag-input-wrapper">
                    <input type="text" id="tag-search" list="tag-list" placeholder="Add tag..." autocomplete="off">
                    <datalist id="tag-list">{}</datalist>
                    <div class="active-tags">{}</div>
                </div>
            </div>
        </div>
        <div class="filter-row type-row">
            <div class="filter-section type-filters">
                <span class="filter-label">Types:</span>
                <div class="type-toggles">
                    {}
                </div>
            </div>
        </div>
    </div>
    <div class="result-info">
        <span class="result-count">{} documents</span>
    </div>
    {}
    <table class="file-listing" id="document-table">
        <thead>
            <tr>
                <th>Document</th>
                <th>Source</th>
                <th>Type</th>
                <th>Size</th>
                <th>Acquired</th>
            </tr>
        </thead>
        <tbody>
            {}
        </tbody>
    </table>
    {}
    <script>
    (function() {{
        const typeToggles = document.querySelectorAll('.type-toggle input');
        const tagInput = document.getElementById('tag-search');
        const sourceSelect = document.getElementById('source-select');
        let activeTags = {};
        const perPage = {};
        const prevCursor = {};
        const nextCursor = {};

        function buildParams(cursor) {{
            const params = new URLSearchParams();

            const types = [];
            typeToggles.forEach(t => {{
                if (t.checked) types.push(t.value);
            }});
            if (types.length > 0 && types.length < typeToggles.length) {{
                params.set('types', types.join(','));
            }}

            if (activeTags.length > 0) {{
                params.set('tags', activeTags.join(','));
            }}

            const source = sourceSelect.value;
            if (source) params.set('source', source);

            // Cursor-based pagination: page param is a document ID
            if (cursor) params.set('page', cursor);
            if (perPage !== 50) params.set('per_page', perPage);

            return params;
        }}

        function updateFilters() {{
            // Reset to first page (no cursor) on filter change
            const params = buildParams(null);
            const qs = params.toString();
            window.location.href = '/' + (qs ? '?' + qs : '');
        }}

        window.goToPage = function(cursor) {{
            const params = buildParams(cursor);
            const qs = params.toString();
            window.location.href = '/' + (qs ? '?' + qs : '');
        }};

        typeToggles.forEach(t => {{
            t.addEventListener('change', updateFilters);
        }});

        sourceSelect.addEventListener('change', updateFilters);

        tagInput.addEventListener('change', function() {{
            const tag = tagInput.value.trim();
            if (tag && !activeTags.includes(tag)) {{
                activeTags.push(tag);
                tagInput.value = '';
                updateFilters();
            }}
        }});

        tagInput.addEventListener('keypress', function(e) {{
            if (e.key === 'Enter') {{
                e.preventDefault();
                const tag = tagInput.value.trim();
                if (tag && !activeTags.includes(tag)) {{
                    activeTags.push(tag);
                    tagInput.value = '';
                    updateFilters();
                }}
            }}
        }});

        window.removeTag = function(index) {{
            activeTags.splice(index, 1);
            updateFilters();
        }};

        // Async loading of filter options
        const activeTypes = {};
        const activeSource = {};

        // Type categories mapping
        const TYPE_CATEGORIES = {{
            'pdf': 'Documents',
            'image': 'Images',
            'word': 'Word Documents',
            'excel': 'Spreadsheets',
            'email': 'Email',
            'html': 'Web Pages',
            'text': 'Text Files',
            'archive': 'Archives',
            'other': 'Other'
        }};

        // Load type stats
        async function loadTypes() {{
            const container = document.querySelector('.type-toggles');
            const loading = document.getElementById('types-loading');
            if (!loading) return; // Already loaded from server

            try {{
                const res = await fetch('/api/types');
                const data = await res.json();

                // Aggregate counts by category
                const catCounts = {{}};
                data.forEach(item => {{
                    const cat = item.category;
                    catCounts[cat] = (catCounts[cat] || 0) + item.count;
                }});

                // Build toggles HTML
                let html = '';
                for (const [catId, catName] of Object.entries(TYPE_CATEGORIES)) {{
                    const count = catCounts[catId] || 0;
                    if (count > 0) {{
                        const checked = activeTypes.length === 0 || activeTypes.includes(catId) ? 'checked' : '';
                        html += `<label class="type-toggle">
                            <input type="checkbox" name="type" value="${{catId}}" ${{checked}} data-count="${{count}}">
                            <span class="toggle-label">${{catName}}</span>
                            <span class="toggle-count">${{count}}</span>
                        </label>`;
                    }}
                }}
                container.innerHTML = html;

                // Re-attach event listeners
                document.querySelectorAll('.type-toggle input').forEach(t => {{
                    t.addEventListener('change', updateFilters);
                }});
            }} catch (e) {{
                console.error('Failed to load types:', e);
                container.innerHTML = '<span class="error">Failed to load types</span>';
            }}
        }}

        // Load sources
        async function loadSources() {{
            const select = document.getElementById('source-select');
            if (select.options.length > 2) return; // Already loaded from server

            try {{
                const res = await fetch('/api/sources');
                const data = await res.json();

                let html = '<option value="">All Sources</option>';
                data.forEach(s => {{
                    const selected = activeSource === s.id ? ' selected' : '';
                    html += `<option value="${{s.id}}"${{selected}}>${{s.name}}  (${{s.count}})</option>`;
                }});
                select.innerHTML = html;
            }} catch (e) {{
                console.error('Failed to load sources:', e);
            }}
        }}

        // Load tags for autocomplete
        async function loadTags() {{
            const datalist = document.getElementById('tag-list');
            if (datalist.options.length > 0) return; // Already loaded

            try {{
                const res = await fetch('/api/tags');
                const data = await res.json();

                let html = '';
                data.forEach(t => {{
                    html += `<option value="${{t.tag}}" data-count="${{t.count}}">`;
                }});
                datalist.innerHTML = html;
            }} catch (e) {{
                console.error('Failed to load tags:', e);
            }}
        }}

        // Load all filter data async
        loadTypes();
        loadSources();
        loadTags();
    }})();
    </script>
    "#,
        source_options,
        tag_options,
        active_tags_display,
        type_toggles,
        total_count,
        pagination,
        rows,
        pagination,
        active_tags_json,
        per_page,
        prev_cursor_js,
        next_cursor_js,
        active_types_json,
        active_source_js
    )
}

/// CSS styles for the web interface - minimal text-based design.
pub const CSS: &str = include_str!("styles.css");

/// JavaScript for timeline ruler interaction (Wayback Machine style).
pub const JS: &str = include_str!("scripts.js");
