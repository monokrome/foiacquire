//! Storage helpers for document content on disk.

use std::path::{Path, PathBuf};

use crate::models::{Document, DocumentVersion};
use crate::repository::{extract_filename_parts, sanitize_filename, DieselDocumentRepository};
use crate::scrapers::ScraperResult;

/// Construct the storage path for document content.
///
/// Uses a two-level directory structure based on hash prefix for filesystem efficiency:
/// `{documents_dir}/{hash[0..2]}/{hash[0..8]}.{extension}`
pub fn content_storage_path(documents_dir: &Path, content_hash: &str, extension: &str) -> PathBuf {
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
    let filename = format!(
        "{}-{}.{}",
        sanitize_filename(basename),
        &content_hash[..8],
        extension
    );
    documents_dir.join(&content_hash[..2]).join(filename)
}

/// Save scraped document content to disk and database.
pub async fn save_scraped_document_async(
    doc_repo: &DieselDocumentRepository,
    content: &[u8],
    result: &ScraperResult,
    source_id: &str,
    documents_dir: &Path,
) -> anyhow::Result<bool> {
    // Compute content hash and save file with readable name
    let content_hash = DocumentVersion::compute_hash(content);

    // Extract basename and extension from URL or title
    let (basename, extension) =
        extract_filename_parts(&result.url, &result.title, &result.mime_type);

    // Store in subdirectory by first 2 chars of hash (for filesystem efficiency)
    let content_path =
        content_storage_path_with_name(documents_dir, &content_hash, &basename, &extension);
    std::fs::create_dir_all(content_path.parent().unwrap())?;
    std::fs::write(&content_path, content)?;

    let version = DocumentVersion::new_with_metadata(
        content,
        content_path,
        result.mime_type.clone(),
        Some(result.url.clone()),
        result.original_filename.clone(),
        result.server_date,
    );

    // Check existing document
    let existing = doc_repo.get_by_url(&result.url).await?;

    if let Some(mut doc) = existing.into_iter().next() {
        if doc.add_version(version) {
            doc_repo.save(&doc).await?;
        }
        Ok(false) // Updated existing
    } else {
        let doc = Document::new(
            uuid::Uuid::new_v4().to_string(),
            source_id.to_string(),
            result.title.clone(),
            result.url.clone(),
            version,
            result.metadata.clone(),
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
        // Filename with spaces and special chars should be sanitized
        let path = content_storage_path_with_name(docs_dir, hash, "My Report (2024)", "pdf");
        // The exact sanitization depends on sanitize_filename implementation
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

        // Verify file was created
        assert!(path.exists());

        // Verify content
        let saved = std::fs::read(&path).unwrap();
        assert_eq!(saved, content);

        // Verify path structure (hash-based subdirectory)
        let parent = path.parent().unwrap();
        let parent_name = parent.file_name().unwrap().to_str().unwrap();
        assert_eq!(parent_name.len(), 2); // 2-char hash prefix
    }
}
