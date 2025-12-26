//! MIME type categorization and display utilities.

/// MIME type categories for document classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MimeCategory {
    Documents,
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

    if mime_lower == "application/pdf"
        || mime_lower.contains("word")
        || mime_lower == "application/msword"
        || mime_lower.contains("rfc822")
        || mime_lower.starts_with("message/")
        || (mime_lower.starts_with("text/")
            && mime_lower != "text/csv"
            && mime_lower != "text/html")
    {
        MimeCategory::Documents
    } else if mime_lower.starts_with("image/") {
        MimeCategory::Images
    } else if mime_lower.contains("spreadsheet")
        || mime_lower.contains("excel")
        || mime_lower == "application/vnd.ms-excel"
        || mime_lower == "text/csv"
        || mime_lower == "application/json"
        || mime_lower == "application/xml"
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

/// Generate SQL WHERE clause fragment for filtering by category.
/// The clause refers to `dv.mime_type` (document_versions table alias).
pub fn mime_type_sql_condition(category: &str) -> Option<String> {
    match category.to_lowercase().as_str() {
        "pdf" => Some("dv.mime_type = 'application/pdf'".to_string()),
        "documents" => Some(
            "(dv.mime_type = 'application/pdf' OR dv.mime_type LIKE '%word%' \
             OR dv.mime_type = 'application/msword' OR dv.mime_type LIKE '%rfc822%' \
             OR dv.mime_type LIKE 'message/%' \
             OR (dv.mime_type LIKE 'text/%' AND dv.mime_type != 'text/csv'))"
                .to_string(),
        ),
        "data" => Some(
            "(dv.mime_type LIKE '%spreadsheet%' OR dv.mime_type LIKE '%excel%' \
             OR dv.mime_type = 'application/vnd.ms-excel' OR dv.mime_type = 'text/csv' \
             OR dv.mime_type = 'application/json' OR dv.mime_type = 'application/xml')"
                .to_string(),
        ),
        "images" => Some("dv.mime_type LIKE 'image/%'".to_string()),
        "text" => Some(
            "(dv.mime_type LIKE 'text/%' AND dv.mime_type != 'text/html' \
             AND dv.mime_type != 'text/csv')"
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
        assert!(mime_type_sql_condition("invalid").is_none());
    }
}
