//! Storage helpers for document content on disk.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::models::{Document, DocumentVersion};
use crate::repository::{extract_filename_parts, sanitize_filename, DieselDocumentRepository};

/// Metadata needed to save a document to disk and database.
///
/// Generic input type so callers don't need to depend on scraper types.
pub struct DocumentInput {
    pub url: String,
    pub title: String,
    pub mime_type: String,
    pub metadata: serde_json::Value,
    pub original_filename: Option<String>,
    pub server_date: Option<DateTime<Utc>>,
}

/// Minimum length required for a content hash used in storage paths.
const MIN_HASH_LEN: usize = 8;

/// Construct the storage path for document content (no basename).
///
/// Uses a two-level directory structure based on hash prefix for filesystem efficiency:
/// `{documents_dir}/{hash[0..2]}/{hash[0..8]}.{extension}`
///
/// # Panics
/// Panics if `content_hash` is shorter than 8 characters.
pub fn content_storage_path(documents_dir: &Path, content_hash: &str, extension: &str) -> PathBuf {
    assert!(
        content_hash.len() >= MIN_HASH_LEN,
        "content hash too short ({} chars, need at least {}): '{}'",
        content_hash.len(),
        MIN_HASH_LEN,
        content_hash,
    );
    documents_dir
        .join(&content_hash[..2])
        .join(format!("{}.{}", &content_hash[..8], extension))
}

/// Construct the storage path with a full filename (including basename).
///
/// Uses a two-level directory structure based on hash prefix:
/// `{documents_dir}/{hash[0..2]}/{sanitized_basename}-{hash[0..8]}.{extension}`
pub fn content_storage_path_with_name(
    documents_dir: &Path,
    content_hash: &str,
    basename: &str,
    extension: &str,
) -> PathBuf {
    assert!(
        content_hash.len() >= MIN_HASH_LEN,
        "content hash too short ({} chars, need at least {}): '{}'",
        content_hash.len(),
        MIN_HASH_LEN,
        content_hash,
    );
    let filename = format!(
        "{}-{}.{}",
        sanitize_filename(basename),
        &content_hash[..8],
        extension
    );
    documents_dir.join(&content_hash[..2]).join(filename)
}

/// Compute storage path with collision detection.
///
/// Returns `(relative_path, dedup_index)` where `dedup_index` is `None` when
/// no collision occurred (default depth=2).
pub fn compute_storage_path_with_dedup(
    documents_dir: &Path,
    content_hash: &str,
    basename: &str,
    extension: &str,
    _content: &[u8],
) -> (PathBuf, Option<u32>) {
    assert!(
        content_hash.len() >= MIN_HASH_LEN,
        "content hash too short ({} chars, need at least {}): '{}'",
        content_hash.len(),
        MIN_HASH_LEN,
        content_hash,
    );
    let sanitized = sanitize_filename(basename);
    let filename = format!("{}-{}.{}", sanitized, &content_hash[..8], extension);

    for dedup_index in 0u32..6 {
        let depth = 2 + dedup_index as usize;
        let prefix = &content_hash[..depth.min(content_hash.len())];
        let relative = PathBuf::from(prefix).join(&filename);
        let abs = documents_dir.join(&relative);

        if !abs.exists() {
            let idx = if dedup_index == 0 {
                None
            } else {
                Some(dedup_index)
            };
            return (relative, idx);
        }

        // File exists - check if same content
        if let Ok(existing) = std::fs::read(&abs) {
            if DocumentVersion::compute_hash(&existing) == content_hash {
                let idx = if dedup_index == 0 {
                    None
                } else {
                    Some(dedup_index)
                };
                return (relative, idx);
            }
        }
        // Different content at this path - try deeper prefix
    }

    // Exhausted 6 levels, use full hash as prefix (extremely unlikely)
    let relative = PathBuf::from(content_hash).join(&filename);
    (relative, Some(content_hash.len() as u32 - 2))
}

/// Save document content to disk and database.
///
/// Uses `DocumentInput` so callers don't need to depend on `ScraperResult`.
/// New records store `file_path: None` (paths are deterministic).
pub async fn save_document_async(
    doc_repo: &DieselDocumentRepository,
    content: &[u8],
    input: &DocumentInput,
    source_id: &str,
    documents_dir: &Path,
) -> anyhow::Result<bool> {
    let content_hash = DocumentVersion::compute_hash(content);

    let (basename, extension) = extract_filename_parts(&input.url, &input.title, &input.mime_type);

    // Compute path with collision detection
    let (relative_path, dedup_index) =
        compute_storage_path_with_dedup(documents_dir, &content_hash, &basename, &extension, content);
    let abs_path = documents_dir.join(&relative_path);
    if let Some(parent) = abs_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&abs_path, content)?;

    let mut version = DocumentVersion::new_with_metadata(
        content,
        input.mime_type.clone(),
        Some(input.url.clone()),
        input.original_filename.clone(),
        input.server_date,
    );
    version.dedup_index = dedup_index;

    // Check existing document
    let existing = doc_repo.get_by_url(&input.url).await?;

    if let Some(mut doc) = existing.into_iter().next() {
        if doc.add_version(version) {
            doc_repo.save(&doc).await?;
        }
        Ok(false) // Updated existing
    } else {
        let doc = Document::new(
            uuid::Uuid::new_v4().to_string(),
            source_id.to_string(),
            input.title.clone(),
            input.url.clone(),
            version,
            input.metadata.clone(),
        );
        doc_repo.save(&doc).await?;
        Ok(true) // Created new
    }
}

/// Map MIME type to file extension.
pub fn mime_to_extension(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "pdf",
        "text/html" => "html",
        "text/plain" => "txt",
        "application/json" => "json",
        "application/xml" | "text/xml" => "xml",
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "application/vnd.ms-excel" => "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xlsx",
        "application/zip" => "zip",
        "application/gzip" => "gz",
        _ => "bin",
    }
}

/// Save new version content to disk.
///
/// Returns the path where the content was saved.
#[allow(dead_code)]
pub fn save_version_content(
    content: &[u8],
    mime_type: &str,
    documents_dir: &Path,
) -> anyhow::Result<PathBuf> {
    let content_hash = DocumentVersion::compute_hash(content);
    let content_path =
        content_storage_path(documents_dir, &content_hash, mime_to_extension(mime_type));

    if let Some(parent) = content_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&content_path, content)?;

    Ok(content_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn test_content_storage_path() {
        let docs_dir = Path::new("/docs");
        let hash = "abcdef1234567890abcdef1234567890";
        let path = content_storage_path(docs_dir, hash, "pdf");
        assert_eq!(path, PathBuf::from("/docs/ab/abcdef12.pdf"));
    }

    #[test]
    fn test_content_storage_path_with_name() {
        let docs_dir = Path::new("/docs");
        let hash = "abcdef1234567890abcdef1234567890";
        let path = content_storage_path_with_name(docs_dir, hash, "report", "pdf");
        assert_eq!(path, PathBuf::from("/docs/ab/report-abcdef12.pdf"));
    }

    #[test]
    fn test_content_storage_path_with_name_sanitizes() {
        let docs_dir = Path::new("/docs");
        let hash = "abcdef1234567890abcdef1234567890";
        let path = content_storage_path_with_name(docs_dir, hash, "My Report (2024)", "pdf");
        assert!(path.to_string_lossy().contains("abcdef12.pdf"));
    }

    #[test]
    fn test_mime_to_extension_pdf() {
        assert_eq!(mime_to_extension("application/pdf"), "pdf");
    }

    #[test]
    fn test_mime_to_extension_html() {
        assert_eq!(mime_to_extension("text/html"), "html");
    }

    #[test]
    fn test_mime_to_extension_text() {
        assert_eq!(mime_to_extension("text/plain"), "txt");
    }

    #[test]
    fn test_mime_to_extension_json() {
        assert_eq!(mime_to_extension("application/json"), "json");
    }

    #[test]
    fn test_mime_to_extension_xml() {
        assert_eq!(mime_to_extension("application/xml"), "xml");
        assert_eq!(mime_to_extension("text/xml"), "xml");
    }

    #[test]
    fn test_mime_to_extension_images() {
        assert_eq!(mime_to_extension("image/jpeg"), "jpg");
        assert_eq!(mime_to_extension("image/png"), "png");
        assert_eq!(mime_to_extension("image/gif"), "gif");
    }

    #[test]
    fn test_mime_to_extension_office() {
        assert_eq!(mime_to_extension("application/msword"), "doc");
        assert_eq!(
            mime_to_extension(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            ),
            "docx"
        );
        assert_eq!(mime_to_extension("application/vnd.ms-excel"), "xls");
        assert_eq!(
            mime_to_extension("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "xlsx"
        );
    }

    #[test]
    fn test_mime_to_extension_archives() {
        assert_eq!(mime_to_extension("application/zip"), "zip");
        assert_eq!(mime_to_extension("application/gzip"), "gz");
    }

    #[test]
    fn test_mime_to_extension_unknown() {
        assert_eq!(mime_to_extension("application/unknown"), "bin");
        assert_eq!(mime_to_extension("some/random"), "bin");
    }

    #[test]
    fn test_save_version_content() {
        let dir = tempdir().unwrap();
        let content = b"test document content";

        let path = save_version_content(content, "application/pdf", dir.path()).unwrap();

        assert!(path.exists());

        let saved = std::fs::read(&path).unwrap();
        assert_eq!(saved, content);

        let parent = path.parent().unwrap();
        let parent_name = parent.file_name().unwrap().to_str().unwrap();
        assert_eq!(parent_name.len(), 2);
    }

    #[test]
    fn test_compute_storage_path_with_dedup_no_collision() {
        let dir = tempdir().unwrap();
        let content = b"unique content for dedup test";
        let hash = DocumentVersion::compute_hash(content);

        let (relative, dedup_index) =
            compute_storage_path_with_dedup(dir.path(), &hash, "report", "pdf", content);

        assert!(dedup_index.is_none());
        assert!(relative.to_string_lossy().starts_with(&hash[..2]));
        assert!(relative.to_string_lossy().contains("report-"));
        assert!(relative.to_string_lossy().ends_with(".pdf"));
    }

    #[test]
    fn test_compute_storage_path_with_dedup_same_content_reuses() {
        let dir = tempdir().unwrap();
        let content = b"content to be written twice";
        let hash = DocumentVersion::compute_hash(content);

        // Write the file first
        let (rel1, _) =
            compute_storage_path_with_dedup(dir.path(), &hash, "report", "pdf", content);
        let abs1 = dir.path().join(&rel1);
        std::fs::create_dir_all(abs1.parent().unwrap()).unwrap();
        std::fs::write(&abs1, content).unwrap();

        // Should reuse the same path (same content)
        let (rel2, dedup2) =
            compute_storage_path_with_dedup(dir.path(), &hash, "report", "pdf", content);
        assert_eq!(rel1, rel2);
        assert!(dedup2.is_none());
    }

    #[test]
    #[should_panic(expected = "content hash too short")]
    fn test_content_storage_path_panics_on_short_hash() {
        content_storage_path(Path::new("/docs"), "abc", "pdf");
    }

    #[test]
    #[should_panic(expected = "content hash too short")]
    fn test_content_storage_path_panics_on_empty_hash() {
        content_storage_path(Path::new("/docs"), "", "pdf");
    }

    #[test]
    #[should_panic(expected = "content hash too short")]
    fn test_content_storage_path_panics_on_7_chars() {
        content_storage_path(Path::new("/docs"), "abcdef1", "pdf");
    }

    #[test]
    #[should_panic(expected = "content hash too short")]
    fn test_content_storage_path_with_name_panics_on_short_hash() {
        content_storage_path_with_name(Path::new("/docs"), "abc", "report", "pdf");
    }

    #[test]
    #[should_panic(expected = "content hash too short")]
    fn test_compute_storage_path_with_dedup_panics_on_short_hash() {
        let dir = tempdir().unwrap();
        compute_storage_path_with_dedup(dir.path(), "abc", "report", "pdf", b"content");
    }
}
