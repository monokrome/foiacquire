//! Askama template structs for the web interface.
//!
//! Each struct corresponds to an HTML template in the templates/ directory.
//! Askama provides compile-time verification that templates are valid.

use askama::Template;

use crate::models::{VirtualFile, VirtualFileStatus};
use crate::repository::diesel_document::BrowseRow;
use crate::repository::parse_datetime;
use crate::utils::{format_size, mime_icon};

/// Helper struct for document rows in listings.
pub struct DocumentRow {
    pub id: String,
    pub title: String,
    pub icon: String,
    pub mime_type: String,
    pub size_str: String,
    pub date_str: String,
    pub timestamp: i64,
    pub source_id: String,
    pub has_synopsis: bool,
    pub synopsis_preview: String,
    pub tags: Vec<TagRef>,
    pub other_tags: Vec<TagRef>,
}

/// Helper struct for tag references.
pub struct TagRef {
    pub name: String,
    pub encoded: String,
}

/// Helper struct for tags with counts.
pub struct TagWithCount {
    pub name: String,
    pub encoded: String,
    pub count: usize,
}

/// Helper struct for active tag display with index.
pub struct ActiveTagDisplay {
    pub name: String,
    pub index: usize,
}

/// Helper struct for version timeline items.
pub struct VersionItem {
    pub path: String,
    pub filename: String,
    pub size_str: String,
    pub date_str: String,
}

/// Helper struct for virtual file display.
#[derive(Clone)]
pub struct VirtualFileRow {
    pub id: String,
    pub filename: String,
    pub icon: String,
    pub mime_type: String,
    pub size_str: String,
    pub status_badge: String,
}

/// Helper struct for type statistics.
pub struct TypeStat {
    pub category: String,
    pub mime_type: String,
    pub count: u64,
}

/// Helper struct for category with count.
#[derive(Clone)]
pub struct CategoryWithCount {
    pub id: String,
    pub name: String,
    pub count: u64,
    pub active: bool,
    pub checked: bool,
}

/// Helper struct for source in dropdown.
pub struct SourceOption {
    pub id: String,
    pub name: String,
    pub count: u64,
    pub selected: bool,
}

/// Helper struct for duplicate groups.
pub struct DuplicateGroup {
    pub hash_prefix: String,
    pub docs: Vec<DuplicateDoc>,
}

/// Helper struct for documents in duplicate groups.
pub struct DuplicateDoc {
    pub id: String,
    pub title: String,
    pub source_id: String,
}

/// Duplicates list page.
#[derive(Template)]
#[template(path = "duplicates.html")]
pub struct DuplicatesTemplate<'a> {
    pub title: &'a str,
    pub duplicates: Vec<DuplicateGroup>,
    pub has_duplicates: bool,
}

/// Tags list page.
#[derive(Template)]
#[template(path = "tags.html")]
pub struct TagsTemplate<'a> {
    pub title: &'a str,
    pub tags: Vec<TagWithCount>,
    pub has_tags: bool,
}

/// Documents filtered by tag.
#[derive(Template)]
#[template(path = "tag_documents.html")]
pub struct TagDocumentsTemplate<'a> {
    pub title: &'a str,
    pub tag: &'a str,
    pub document_count: usize,
    pub documents: Vec<DocumentRow>,
}

/// Types list page.
#[derive(Template)]
#[template(path = "types.html")]
pub struct TypesTemplate<'a> {
    pub title: &'a str,
    pub categories: Vec<CategoryWithCount>,
    pub type_stats: Vec<TypeStat>,
}

/// Documents filtered by type.
#[derive(Template)]
#[template(path = "type_documents.html")]
pub struct TypeDocumentsTemplate<'a> {
    pub title: &'a str,
    pub type_name: &'a str,
    pub document_count: usize,
    pub tabs: Vec<CategoryWithCount>,
    pub has_tabs: bool,
    pub documents: Vec<DocumentRow>,
}

/// Document detail page.
#[derive(Template)]
#[template(path = "document_detail.html")]
pub struct DocumentDetailTemplate<'a> {
    pub title: &'a str,
    pub doc_id: &'a str,
    pub source_id: &'a str,
    pub source_url: &'a str,
    pub versions: Vec<VersionItem>,
    pub has_versions: bool,
    pub other_sources: Vec<String>,
    pub has_other_sources: bool,
    pub has_extracted_text: bool,
    pub extracted_text_val: String,
    pub virtual_files: Vec<VirtualFileRow>,
    pub has_virtual_files: bool,
    pub virtual_files_count: usize,
    pub has_prev: bool,
    pub prev_id_val: String,
    pub prev_title_val: String,
    pub prev_title_truncated: String,
    pub has_next: bool,
    pub next_id_val: String,
    pub next_title_val: String,
    pub next_title_truncated: String,
    pub position: u64,
    pub total: u64,
    pub nav_query_string: String,
    pub has_pages: bool,
    pub page_count_val: u32,
    pub version_id_val: i64,
}

/// Main browse page with filters.
#[derive(Template)]
#[template(path = "browse.html")]
pub struct BrowseTemplate<'a> {
    pub title: &'a str,
    pub documents: Vec<DocumentRow>,
    pub categories: Vec<CategoryWithCount>,
    pub type_stats_empty: bool,
    pub sources: Vec<SourceOption>,
    pub sources_empty: bool,
    pub has_active_source: bool,
    pub active_source_val: String,
    pub all_tags: Vec<TagWithCount>,
    pub active_tags_display: Vec<ActiveTagDisplay>,
    pub has_prev_cursor: bool,
    pub prev_cursor_val: String,
    pub has_next_cursor: bool,
    pub next_cursor_val: String,
    pub start_position: u64,
    pub end_position: u64,
    pub total_count: u64,
    pub per_page: usize,
    pub has_pagination: bool,
    pub nav_query_string: String,
    pub active_tags_json: String,
    pub active_types_json: String,
    pub active_source_js: String,
    pub prev_cursor_js: String,
    pub next_cursor_js: String,
}

/// Error page template.
#[derive(Template)]
#[template(path = "error.html")]
pub struct ErrorTemplate<'a> {
    pub title: &'a str,
    pub message: &'a str,
}

// Helper implementations for converting data to template structs

impl TagRef {
    pub fn new(name: String) -> Self {
        let encoded = urlencoding::encode(&name).to_string();
        Self { name, encoded }
    }
}

impl TagWithCount {
    pub fn new(name: String, count: usize) -> Self {
        let encoded = urlencoding::encode(&name).to_string();
        Self {
            name,
            encoded,
            count,
        }
    }
}

impl VirtualFileRow {
    pub fn from_virtual_file(vf: &VirtualFile) -> Self {
        let status_badge = match vf.status {
            VirtualFileStatus::Pending => r#"<span class="status-badge pending">pending</span>"#,
            VirtualFileStatus::OcrComplete => r#"<span class="status-badge complete">OCR</span>"#,
            VirtualFileStatus::Failed => r#"<span class="status-badge failed">failed</span>"#,
            VirtualFileStatus::Unsupported => r#"<span class="status-badge unsupported">—</span>"#,
        };
        Self {
            id: vf.id.to_string(),
            filename: vf.filename.clone(),
            icon: mime_icon(&vf.mime_type).to_string(),
            mime_type: vf.mime_type.clone(),
            size_str: format_size(vf.file_size),
            status_badge: status_badge.to_string(),
        }
    }
}

impl DocumentRow {
    /// Create a DocumentRow with basic fields, no other_sources info.
    #[allow(clippy::too_many_arguments)] // Template struct initialization
    pub fn new(
        id: String,
        title: String,
        source_id: String,
        mime_type: String,
        size: u64,
        acquired_at: chrono::DateTime<chrono::Utc>,
        synopsis: Option<String>,
        tags: Vec<String>,
    ) -> Self {
        let synopsis_preview = synopsis
            .as_ref()
            .map(|s| {
                let preview: String = s.chars().take(100).collect();
                if s.len() > 100 {
                    format!("{}...", preview)
                } else {
                    preview
                }
            })
            .unwrap_or_default();

        Self {
            id,
            title,
            icon: mime_icon(&mime_type).to_string(),
            mime_type,
            size_str: format_size(size),
            date_str: acquired_at.format("%Y-%m-%d %H:%M").to_string(),
            timestamp: acquired_at.timestamp(),
            source_id,
            has_synopsis: synopsis.is_some(),
            synopsis_preview,
            tags: tags.iter().map(|t| TagRef::new(t.clone())).collect(),
            other_tags: Vec::new(),
        }
    }

    /// Create with other_tags for tag document pages (excludes the current tag).
    pub fn with_other_tags(mut self, current_tag: &str) -> Self {
        self.other_tags = self
            .tags
            .iter()
            .filter(|t| t.name.to_lowercase() != current_tag.to_lowercase())
            .take(5)
            .cloned()
            .collect();
        self
    }

    /// Create from an optimized BrowseRow (used for fast browse queries).
    pub fn from_browse_row(row: BrowseRow) -> Self {
        let display_name = row.original_filename.unwrap_or(row.title);
        let tags: Vec<String> = row
            .tags
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let acquired_at = parse_datetime(&row.acquired_at);

        let synopsis_preview = row
            .synopsis
            .as_ref()
            .map(|s| {
                let preview: String = s.chars().take(100).collect();
                if s.len() > 100 {
                    format!("{}...", preview)
                } else {
                    preview
                }
            })
            .unwrap_or_default();

        Self {
            id: row.id,
            title: display_name,
            icon: mime_icon(&row.mime_type).to_string(),
            mime_type: row.mime_type,
            size_str: format_size(row.file_size as u64),
            date_str: acquired_at.format("%Y-%m-%d %H:%M").to_string(),
            timestamp: acquired_at.timestamp(),
            source_id: row.source_id,
            has_synopsis: row.synopsis.is_some(),
            synopsis_preview,
            tags: tags.iter().map(|t| TagRef::new(t.clone())).collect(),
            other_tags: Vec::new(),
        }
    }
}

impl Clone for TagRef {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            encoded: self.encoded.clone(),
        }
    }
}
