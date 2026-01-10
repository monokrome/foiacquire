//! Shared types and helper utilities for the document repository.

/// Navigation context for a document within a filtered list.
/// Uses window functions to efficiently find prev/next documents.
#[derive(Debug, Clone)]
pub struct DocumentNavigation {
    pub prev_id: Option<String>,
    pub prev_title: Option<String>,
    pub next_id: Option<String>,
    pub next_title: Option<String>,
    pub position: u64,
    pub total: u64,
}

/// Extract filename parts (basename and extension) from URL, title, or mime type.
pub fn extract_filename_parts(url: &str, title: &str, mime_type: &str) -> (String, String) {
    // Try to get filename from URL path
    if let Some(filename) = url.split('/').next_back() {
        if let Some(dot_pos) = filename.rfind('.') {
            let basename = &filename[..dot_pos];
            let ext = &filename[dot_pos + 1..];
            // Only use if it looks like a real extension
            if !basename.is_empty() && ext.len() <= 5 && ext.chars().all(|c| c.is_alphanumeric()) {
                return (basename.to_string(), ext.to_lowercase());
            }
        }
    }

    // Fall back to title + mime type extension
    let ext = match mime_type {
        "application/pdf" => "pdf",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "text/html" => "html",
        "text/plain" => "txt",
        "image/jpeg" => "jpg",
        "image/png" => "png",
        _ => "bin",
    };

    let basename = if title.is_empty() { "document" } else { title };
    (basename.to_string(), ext.to_string())
}

/// Sanitize a string for use as a filename.
pub fn sanitize_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    // Trim and limit length
    let trimmed = sanitized.trim().trim_matches('_');
    if trimmed.len() > 100 {
        trimmed[..100].to_string()
    } else if trimmed.is_empty() {
        "document".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_filename_from_url() {
        let (basename, ext) = extract_filename_parts(
            "https://example.com/docs/report.pdf",
            "Some Title",
            "application/pdf",
        );
        assert_eq!(basename, "report");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_extract_filename_fallback_to_mime() {
        let (basename, ext) = extract_filename_parts(
            "https://example.com/api/download?id=123",
            "Annual Report",
            "application/pdf",
        );
        assert_eq!(basename, "Annual Report");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_extract_filename_empty_title() {
        let (basename, ext) =
            extract_filename_parts("https://example.com/api/download", "", "application/pdf");
        assert_eq!(basename, "document");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_sanitize_filename_special_chars() {
        assert_eq!(
            sanitize_filename("file/with:bad*chars?"),
            "file_with_bad_chars"
        );
    }

    #[test]
    fn test_sanitize_filename_empty() {
        assert_eq!(sanitize_filename(""), "document");
    }

    #[test]
    fn test_sanitize_filename_long() {
        let long_name = "a".repeat(200);
        assert_eq!(sanitize_filename(&long_name).len(), 100);
    }
}
