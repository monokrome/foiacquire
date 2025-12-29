//! Askama template structs for the web interface.
//!
//! Each struct corresponds to an HTML template in the templates/ directory.
//!
//! These templates are ready to use but the handlers haven't been migrated yet.
//! Once handlers are updated, remove the dead_code allow.

#![allow(dead_code)]

use askama::Template;

/// Helper struct for source data in templates.
pub struct SourceRow {
    pub id: String,
    pub name: String,
    pub doc_count: u64,
    pub last_scraped_str: String,
}

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
    pub other_sources_count: usize,
    pub other_sources_list: String,
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

/// Sources list page.
#[derive(Template)]
#[template(path = "sources.html")]
pub struct SourcesTemplate<'a> {
    pub title: &'a str,
    pub sources: Vec<SourceRow>,
}

/// Documents list for a source.
#[derive(Template)]
#[template(path = "documents.html")]
pub struct DocumentsTemplate<'a> {
    pub title: &'a str,
    pub source_name: &'a str,
    pub documents: Vec<DocumentRow>,
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
