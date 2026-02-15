//! Helper functions for scrape commands.

use std::path::Path;

use foia::models::{Document, DocumentVersion};
use foia::storage::{compute_storage_path_with_dedup, mime_to_extension};

/// Parse server date from Last-Modified header.
pub fn parse_server_date(last_modified: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    last_modified.and_then(|lm| {
        chrono::DateTime::parse_from_rfc2822(lm)
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
    })
}

/// Update document metadata without re-downloading content.
pub fn update_document_metadata(
    doc: &Document,
    filename: Option<String>,
    server_date: Option<chrono::DateTime<chrono::Utc>>,
) -> Document {
    let mut updated_doc = doc.clone();
    if let Some(version) = updated_doc.versions.first_mut() {
        if version.original_filename.is_none() {
            version.original_filename = filename;
        }
        if version.server_date.is_none() {
            version.server_date = server_date;
        }
    }
    updated_doc
}

/// Save new content and add a new version to the document.
#[allow(clippy::too_many_arguments)]
pub fn save_new_version(
    doc: &Document,
    content: &[u8],
    new_hash: &str,
    mime_type: &str,
    url: &str,
    filename: Option<String>,
    server_date: Option<chrono::DateTime<chrono::Utc>>,
    documents_dir: &Path,
) -> std::io::Result<Document> {
    let ext = mime_to_extension(mime_type);
    let basename = filename
        .as_deref()
        .and_then(|f| f.rfind('.').map(|p| &f[..p]))
        .unwrap_or_else(|| url.split('/').next_back().unwrap_or("document"));

    let (relative_path, dedup_index) =
        compute_storage_path_with_dedup(documents_dir, new_hash, basename, ext, content);
    let abs_path = documents_dir.join(&relative_path);

    if let Some(parent) = abs_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&abs_path, content)?;

    let mut new_version = DocumentVersion::new_with_metadata(
        content,
        mime_type.to_string(),
        Some(url.to_string()),
        filename,
        server_date,
    );
    new_version.dedup_index = dedup_index;

    let mut updated_doc = doc.clone();
    updated_doc.add_version(new_version);
    Ok(updated_doc)
}

/// Result of processing an HTTP response for refresh.
pub enum RefreshResult {
    Updated(Document),
    Redownloaded(Document),
    Skipped,
}

/// Process an HTTP GET response for metadata refresh.
pub async fn process_get_response_for_refresh(
    response: foia::http_client::HttpResponse,
    doc: &Document,
    current_version: &DocumentVersion,
    documents_dir: &Path,
) -> RefreshResult {
    let filename = response.content_disposition_filename();
    let last_modified = response.last_modified().map(|s| s.to_string());
    let server_date = parse_server_date(last_modified.as_deref());

    let content = match response.bytes().await {
        Ok(b) => b,
        Err(_) => return RefreshResult::Skipped,
    };

    let new_hash = DocumentVersion::compute_hash(&content);
    let content_changed = new_hash != current_version.content_hash;

    if content_changed {
        let updated = match save_new_version(
            doc,
            &content,
            &new_hash,
            &current_version.mime_type,
            &doc.source_url,
            filename,
            server_date,
            documents_dir,
        ) {
            Ok(doc) => doc,
            Err(_) => return RefreshResult::Skipped,
        };
        RefreshResult::Redownloaded(updated)
    } else {
        let updated = update_document_metadata(doc, filename, server_date);
        RefreshResult::Updated(updated)
    }
}
