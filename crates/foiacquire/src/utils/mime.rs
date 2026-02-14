//! MIME type categorization and display utilities.

/// Known document file extensions (PDF, Office documents).
const DOCUMENT_EXTENSIONS: &[&str] = &["pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx"];

/// Known file extensions (documents + images + archives).
const FILE_EXTENSIONS: &[&str] = &[
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "jpg", "jpeg", "png", "gif", "tif", "tiff",
    "bmp", "zip",
];

/// Guess MIME type from a filename's extension.
pub fn guess_mime_from_filename(name: &str) -> &'static str {
    let ext = name
        .rsplit('.')
        .next()
        .map(|e| e.to_lowercase())
        .unwrap_or_default();

    match ext.as_str() {
        "pdf" => "application/pdf",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt" => "application/vnd.ms-powerpoint",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "tif" | "tiff" => "image/tiff",
        "bmp" => "image/bmp",
        "msg" => "application/vnd.ms-outlook",
        "eml" => "message/rfc822",
        "zip" => "application/zip",
        _ => "application/octet-stream",
    }
}

/// Guess MIME type from a URL, stripping query params and fragments first.
pub fn guess_mime_from_url(url: &str) -> &'static str {
    let path = url.split('?').next().unwrap_or(url);
    let path = path.split('#').next().unwrap_or(path);
    guess_mime_from_filename(path)
}

/// Extract the lowercased file extension from a URL path (stripping query/fragment).
fn url_path_extension(url: &str) -> String {
    let path = url.split('?').next().unwrap_or(url);
    let path = path.split('#').next().unwrap_or(path);
    path.rsplit('.')
        .next()
        .map(|e| e.to_lowercase())
        .unwrap_or_default()
}

/// Check if a URL path ends with a known document extension.
pub fn has_document_extension(url: &str) -> bool {
    let ext = url_path_extension(url);
    DOCUMENT_EXTENSIONS.contains(&ext.as_str())
}

/// Check if a URL path ends with any known file extension (documents, images, archives).
pub fn has_file_extension(url: &str) -> bool {
    let ext = url_path_extension(url);
    FILE_EXTENSIONS.contains(&ext.as_str())
}

/// Check if a MIME type is supported for text extraction (OCR/parsing).
pub fn is_extractable_mimetype(mime_type: &str) -> bool {
    matches!(
        mime_type,
        "application/pdf"
            | "image/png"
            | "image/jpeg"
            | "image/tiff"
            | "image/gif"
            | "image/bmp"
            | "text/plain"
            | "text/html"
    )
}

/// Check if a MIME type represents a FOIA-relevant document format.
pub fn is_document_mimetype(mimetype: &str) -> bool {
    matches!(
        mimetype,
        "application/pdf"
            | "application/msword"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.ms-excel"
            | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            | "application/vnd.ms-powerpoint"
            | "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            | "text/html"
            | "application/xhtml+xml"
    )
}

/// MIME type categories for document classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MimeCategory {
    Documents,
    Markup,
    Images,
    Data,
    Archives,
    Other,
}

impl MimeCategory {
    /// Get the category ID as a string.
    pub fn id(&self) -> &'static str {
        match self {
            Self::Documents => "documents",
            Self::Markup => "markup",
            Self::Images => "images",
            Self::Data => "data",
            Self::Archives => "archives",
            Self::Other => "other",
        }
    }

    /// Get the display name for the category.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Documents => "Documents",
            Self::Markup => "Markup",
            Self::Images => "Images",
            Self::Data => "Data",
            Self::Archives => "Archives",
            Self::Other => "Other",
        }
    }

    /// Get all categories as (id, display_name) pairs.
    pub fn all() -> &'static [(&'static str, &'static str)] {
        &[
            ("documents", "Documents"),
            ("markup", "Markup"),
            ("images", "Images"),
            ("data", "Data"),
            ("archives", "Archives"),
            ("other", "Other"),
        ]
    }

    /// Parse a category from its string ID.
    pub fn from_id(id: &str) -> Option<Self> {
        match id.to_lowercase().as_str() {
            "documents" | "pdf" | "text" | "email" => Some(Self::Documents),
            "markup" | "html" | "xml" => Some(Self::Markup),
            "images" => Some(Self::Images),
            "data" => Some(Self::Data),
            "archives" => Some(Self::Archives),
            "other" => Some(Self::Other),
            _ => None,
        }
    }
}

/// Categorize a MIME type into a category.
pub fn mime_type_category(mime: &str) -> MimeCategory {
    let mime_lower = mime.to_lowercase();

    // Markup types (HTML, XML, XHTML)
    if mime_lower == "text/html"
        || mime_lower == "application/xhtml+xml"
        || mime_lower == "text/xml"
        || mime_lower == "application/xml"
    {
        MimeCategory::Markup
    } else if mime_lower == "application/pdf"
        || mime_lower.contains("word")
        || mime_lower == "application/msword"
        || mime_lower.contains("rfc822")
        || mime_lower.starts_with("message/")
        || (mime_lower.starts_with("text/") && mime_lower != "text/csv")
    {
        MimeCategory::Documents
    } else if mime_lower.starts_with("image/") {
        MimeCategory::Images
    } else if mime_lower.contains("spreadsheet")
        || mime_lower.contains("excel")
        || mime_lower == "application/vnd.ms-excel"
        || mime_lower == "text/csv"
        || mime_lower == "application/json"
    {
        MimeCategory::Data
    } else if mime_lower == "application/zip"
        || mime_lower == "application/x-zip"
        || mime_lower == "application/x-zip-compressed"
        || mime_lower == "application/x-tar"
        || mime_lower == "application/gzip"
        || mime_lower == "application/x-rar-compressed"
        || mime_lower == "application/x-7z-compressed"
    {
        MimeCategory::Archives
    } else {
        MimeCategory::Other
    }
}

/// Get an icon string for a MIME type.
pub fn mime_icon(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "[pdf]",
        m if m.starts_with("image/") => "[img]",
        m if m.contains("word") => "[doc]",
        m if m.contains("excel") || m.contains("spreadsheet") => "[xls]",
        "text/html" => "[htm]",
        "text/plain" => "[txt]",
        "message/rfc822" => "[eml]",
        "application/zip" | "application/x-zip" | "application/x-zip-compressed" => "[zip]",
        _ => "[---]",
    }
}

/// Get the category name for a MIME type.
pub fn mime_to_category(mime: &str) -> &'static str {
    mime_type_category(mime).id()
}

/// Get SQL LIKE patterns for a category.
/// Returns patterns that can be used with LIKE to match MIME types.
pub fn category_to_mime_patterns(category: &str) -> Vec<&'static str> {
    match category.to_lowercase().as_str() {
        "documents" => vec![
            "application/pdf",
            "%word%",
            "application/msword",
            "%rfc822%",
            "message/%",
            "text/plain",
            "text/rtf",
        ],
        "markup" | "html" | "xml" => vec![
            "text/html",
            "application/xhtml+xml",
            "text/xml",
            "application/xml",
        ],
        "images" => vec!["image/%"],
        "data" => vec![
            "%spreadsheet%",
            "%excel%",
            "application/vnd.ms-excel",
            "text/csv",
            "application/json",
        ],
        "archives" => vec![
            "application/zip",
            "application/x-zip",
            "application/x-zip-compressed",
            "application/x-tar",
            "application/gzip",
            "application/x-rar-compressed",
            "application/x-7z-compressed",
        ],
        _ => vec![],
    }
}

/// Generate SQL WHERE clause fragment for filtering by category.
/// The clause refers to `dv.mime_type` (document_versions table alias).
#[allow(dead_code)]
pub fn mime_type_sql_condition(category: &str) -> Option<String> {
    match category.to_lowercase().as_str() {
        "pdf" => Some("dv.mime_type = 'application/pdf'".to_string()),
        "documents" => Some(
            "(dv.mime_type = 'application/pdf' OR dv.mime_type LIKE '%word%' \
             OR dv.mime_type = 'application/msword' OR dv.mime_type LIKE '%rfc822%' \
             OR dv.mime_type LIKE 'message/%' \
             OR (dv.mime_type LIKE 'text/%' AND dv.mime_type != 'text/csv' \
                 AND dv.mime_type != 'text/html' AND dv.mime_type != 'text/xml'))"
                .to_string(),
        ),
        "markup" | "html" | "xml" => Some(
            "(dv.mime_type = 'text/html' OR dv.mime_type = 'application/xhtml+xml' \
             OR dv.mime_type = 'text/xml' OR dv.mime_type = 'application/xml')"
                .to_string(),
        ),
        "data" => Some(
            "(dv.mime_type LIKE '%spreadsheet%' OR dv.mime_type LIKE '%excel%' \
             OR dv.mime_type = 'application/vnd.ms-excel' OR dv.mime_type = 'text/csv' \
             OR dv.mime_type = 'application/json')"
                .to_string(),
        ),
        "images" => Some("dv.mime_type LIKE 'image/%'".to_string()),
        "text" => Some(
            "(dv.mime_type LIKE 'text/%' AND dv.mime_type != 'text/html' \
             AND dv.mime_type != 'text/xml' AND dv.mime_type != 'text/csv')"
                .to_string(),
        ),
        "email" => {
            Some("(dv.mime_type LIKE '%rfc822%' OR dv.mime_type LIKE 'message/%')".to_string())
        }
        "archives" => Some(
            "(dv.mime_type = 'application/zip' OR dv.mime_type = 'application/x-zip' \
             OR dv.mime_type = 'application/x-zip-compressed' OR dv.mime_type = 'application/x-tar' \
             OR dv.mime_type = 'application/gzip' OR dv.mime_type = 'application/x-rar-compressed' \
             OR dv.mime_type = 'application/x-7z-compressed')"
                .to_string(),
        ),
        "other" => Some(
            "(dv.mime_type NOT LIKE 'image/%' AND dv.mime_type != 'application/pdf' \
             AND dv.mime_type NOT LIKE '%word%' AND dv.mime_type NOT LIKE '%spreadsheet%' \
             AND dv.mime_type NOT LIKE '%excel%' AND dv.mime_type NOT LIKE 'text/%' \
             AND dv.mime_type NOT LIKE '%rfc822%' AND dv.mime_type NOT LIKE 'message/%' \
             AND dv.mime_type != 'application/json' AND dv.mime_type != 'application/xml' \
             AND dv.mime_type != 'application/xhtml+xml' \
             AND dv.mime_type NOT LIKE 'application/zip%' AND dv.mime_type NOT LIKE 'application/x-zip%' \
             AND dv.mime_type != 'application/x-tar' AND dv.mime_type != 'application/gzip' \
             AND dv.mime_type != 'application/x-rar-compressed' AND dv.mime_type != 'application/x-7z-compressed')"
                .to_string(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mime_category() {
        assert_eq!(
            mime_type_category("application/pdf"),
            MimeCategory::Documents
        );
        assert_eq!(mime_type_category("text/html"), MimeCategory::Markup);
        assert_eq!(mime_type_category("application/xml"), MimeCategory::Markup);
        assert_eq!(mime_type_category("image/png"), MimeCategory::Images);
        assert_eq!(mime_type_category("text/csv"), MimeCategory::Data);
        assert_eq!(
            mime_type_category("application/zip"),
            MimeCategory::Archives
        );
        assert_eq!(
            mime_type_category("application/octet-stream"),
            MimeCategory::Other
        );
    }

    #[test]
    fn test_mime_icon() {
        assert_eq!(mime_icon("application/pdf"), "[pdf]");
        assert_eq!(mime_icon("image/jpeg"), "[img]");
        assert_eq!(mime_icon("application/msword"), "[doc]");
    }

    #[test]
    fn test_sql_condition() {
        assert!(mime_type_sql_condition("documents").is_some());
        assert!(mime_type_sql_condition("markup").is_some());
        assert!(mime_type_sql_condition("invalid").is_none());
    }

    #[test]
    fn guess_mime_from_filename_common_types() {
        assert_eq!(guess_mime_from_filename("report.pdf"), "application/pdf");
        assert_eq!(guess_mime_from_filename("REPORT.PDF"), "application/pdf");
        assert_eq!(guess_mime_from_filename("file.doc"), "application/msword");
        assert_eq!(
            guess_mime_from_filename("file.docx"),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
        assert_eq!(
            guess_mime_from_filename("data.xls"),
            "application/vnd.ms-excel"
        );
        assert_eq!(
            guess_mime_from_filename("data.xlsx"),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        assert_eq!(
            guess_mime_from_filename("slides.ppt"),
            "application/vnd.ms-powerpoint"
        );
        assert_eq!(
            guess_mime_from_filename("slides.pptx"),
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        );
        assert_eq!(guess_mime_from_filename("notes.txt"), "text/plain");
        assert_eq!(guess_mime_from_filename("page.html"), "text/html");
        assert_eq!(guess_mime_from_filename("page.htm"), "text/html");
        assert_eq!(guess_mime_from_filename("photo.jpg"), "image/jpeg");
        assert_eq!(guess_mime_from_filename("photo.jpeg"), "image/jpeg");
        assert_eq!(guess_mime_from_filename("image.png"), "image/png");
        assert_eq!(guess_mime_from_filename("anim.gif"), "image/gif");
        assert_eq!(guess_mime_from_filename("scan.tif"), "image/tiff");
        assert_eq!(guess_mime_from_filename("scan.tiff"), "image/tiff");
        assert_eq!(guess_mime_from_filename("scan.TIFF"), "image/tiff");
        assert_eq!(guess_mime_from_filename("diagram.bmp"), "image/bmp");
        assert_eq!(
            guess_mime_from_filename("email.msg"),
            "application/vnd.ms-outlook"
        );
        assert_eq!(guess_mime_from_filename("email.eml"), "message/rfc822");
        assert_eq!(guess_mime_from_filename("archive.zip"), "application/zip");
        assert_eq!(
            guess_mime_from_filename("unknown"),
            "application/octet-stream"
        );
        assert_eq!(
            guess_mime_from_filename("file.xyz"),
            "application/octet-stream"
        );
    }

    #[test]
    fn guess_mime_from_url_strips_query_and_fragment() {
        assert_eq!(
            guess_mime_from_url("https://example.com/file.pdf"),
            "application/pdf"
        );
        assert_eq!(
            guess_mime_from_url("https://example.com/file.pdf?download=1"),
            "application/pdf"
        );
        assert_eq!(
            guess_mime_from_url("https://example.com/file.pdf#page=2"),
            "application/pdf"
        );
        assert_eq!(
            guess_mime_from_url("https://example.com/file.pdf?a=1#b=2"),
            "application/pdf"
        );
        assert_eq!(
            guess_mime_from_url("https://example.com/page"),
            "application/octet-stream"
        );
    }

    #[test]
    fn has_document_extension_checks() {
        assert!(has_document_extension("https://example.com/report.pdf"));
        assert!(has_document_extension("https://example.com/REPORT.PDF"));
        assert!(has_document_extension("https://example.com/file.doc"));
        assert!(has_document_extension("https://example.com/file.docx"));
        assert!(has_document_extension("https://example.com/data.xls"));
        assert!(has_document_extension("https://example.com/data.xlsx"));
        assert!(has_document_extension("https://example.com/slides.ppt"));
        assert!(has_document_extension("https://example.com/slides.pptx"));
        assert!(!has_document_extension("https://example.com/image.png"));
        assert!(!has_document_extension("https://example.com/page"));
        assert!(!has_document_extension("https://example.com/documents/"));
    }

    #[test]
    fn has_file_extension_checks() {
        // Documents
        assert!(has_file_extension("https://example.com/report.pdf"));
        assert!(has_file_extension("https://example.com/file.docx"));
        // Images
        assert!(has_file_extension("https://example.com/photo.jpg"));
        assert!(has_file_extension("https://example.com/photo.jpeg"));
        assert!(has_file_extension("https://example.com/image.png"));
        assert!(has_file_extension("https://example.com/anim.gif"));
        assert!(has_file_extension("https://example.com/scan.tif"));
        assert!(has_file_extension("https://example.com/scan.tiff"));
        assert!(has_file_extension("https://example.com/diagram.bmp"));
        // Archives
        assert!(has_file_extension("https://example.com/archive.zip"));
        // Not files
        assert!(!has_file_extension("https://example.com/page"));
        assert!(!has_file_extension("https://example.com/reports/"));
    }

    #[test]
    fn is_document_mimetype_checks() {
        assert!(is_document_mimetype("application/pdf"));
        assert!(is_document_mimetype("application/msword"));
        assert!(is_document_mimetype(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ));
        assert!(is_document_mimetype("application/vnd.ms-excel"));
        assert!(is_document_mimetype(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ));
        assert!(is_document_mimetype("text/html"));
        assert!(is_document_mimetype("application/xhtml+xml"));
        assert!(!is_document_mimetype("image/png"));
        assert!(!is_document_mimetype("application/javascript"));
        assert!(!is_document_mimetype("application/octet-stream"));
    }
}
