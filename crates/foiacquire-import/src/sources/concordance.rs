//! Concordance DAT/OPT load file importer.
//!
//! Supports the standard e-discovery load file format used by Concordance,
//! Relativity, and other document review platforms. This format is commonly
//! used for FOIA document releases and investigative journalism collections.
//!
//! ## Format
//!
//! - **DAT file**: Document metadata with þ (0xFE) delimiter and 0x14 text qualifier
//!   - Header: field names (e.g., "Begin Bates", "End Bates")
//!   - Rows: document metadata defining Bates number ranges
//!
//! - **OPT file**: Image load file (standard CSV)
//!   - Maps Bates numbers to actual file paths
//!   - Format: `BatesID,Volume,ImagePath,FirstPage,Field5,Field6,PageCount`

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use console::style;

use crate::{
    guess_mime_type, runner::FileStorageMode, ImportConfig, ImportProgress, ImportSource,
    ImportStats,
};
use foiacquire::models::{Document, DocumentVersion};
use foiacquire::repository::extract_filename_parts;
use foiacquire::storage::content_storage_path_with_name;

/// Concordance DAT field delimiter (þ, thorn character).
/// In UTF-8 this is encoded as 0xC3 0xBE.
const FIELD_DELIMITER_UTF8: &[u8] = &[0xC3, 0xBE];
/// Single-byte delimiter for Latin-1 encoded files.
const FIELD_DELIMITER_LATIN1: u8 = 0xFE;

/// Concordance DAT text qualifier (DC4 control character).
const TEXT_QUALIFIER: u8 = 0x14;

/// A document defined by a Bates range in the DAT file.
#[derive(Debug, Clone)]
struct ConcordanceDocument {
    /// Beginning Bates number.
    begin_bates: String,
    /// Ending Bates number.
    end_bates: String,
    /// Additional metadata fields from DAT.
    metadata: HashMap<String, String>,
}

/// A page mapping from the OPT file.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct OptPage {
    /// Bates ID for this page.
    bates_id: String,
    /// Volume identifier.
    volume: String,
    /// Path to the image file.
    image_path: String,
    /// Whether this is the first page of a document.
    is_first_page: bool,
    /// Page count (if specified).
    page_count: Option<u32>,
}

/// How to handle multi-page documents.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default)]
pub enum MultiPageMode {
    /// Import only the first page's file.
    #[default]
    First,
    /// Import all pages as separate documents.
    All,
}

/// Concordance DAT/OPT import source.
#[allow(dead_code)]
pub struct ConcordanceImportSource {
    /// Path to the volume directory or DAT file.
    source_path: PathBuf,
    /// Path to the DAT file.
    dat_path: PathBuf,
    /// Path to the OPT file.
    opt_path: PathBuf,
    /// Base directory for resolving file paths.
    base_path: PathBuf,
    /// Field names from DAT header.
    dat_fields: Vec<String>,
    /// Parsed documents from DAT.
    documents: Vec<ConcordanceDocument>,
    /// Page mappings from OPT (Bates ID -> page info).
    pages: HashMap<String, OptPage>,
    /// Multi-page handling mode.
    multi_page_mode: MultiPageMode,
    /// URL prefix for constructing canonical URLs from filenames.
    url_prefix: Option<String>,
    /// Settings for database access.
    settings: foiacquire::config::Settings,
}

impl ConcordanceImportSource {
    /// Create a new Concordance import source.
    ///
    /// `path` can be:
    /// - A volume directory containing DATA/*.DAT and IMAGES/
    /// - A direct path to a .DAT file
    pub fn new(
        path: PathBuf,
        multi_page_mode: MultiPageMode,
        url_prefix: Option<String>,
        settings: foiacquire::config::Settings,
    ) -> anyhow::Result<Self> {
        let (dat_path, opt_path, base_path) = Self::resolve_paths(&path)?;

        // Parse DAT file
        let (dat_fields, documents) = Self::parse_dat(&dat_path)?;
        tracing::info!("Parsed {} documents from DAT file", documents.len());

        // Parse OPT file
        let pages = Self::parse_opt(&opt_path)?;
        tracing::info!("Parsed {} page mappings from OPT file", pages.len());

        Ok(Self {
            source_path: path,
            dat_path,
            opt_path,
            base_path,
            dat_fields,
            documents,
            pages,
            multi_page_mode,
            url_prefix,
            settings,
        })
    }

    /// Resolve DAT, OPT, and base paths from input path.
    fn resolve_paths(path: &Path) -> anyhow::Result<(PathBuf, PathBuf, PathBuf)> {
        if path.is_file() {
            // Direct path to DAT file
            let dat_path = path.to_path_buf();
            let opt_path = dat_path.with_extension("OPT");
            let base_path = path
                .parent()
                .and_then(|p| p.parent())
                .unwrap_or(path)
                .to_path_buf();
            return Ok((dat_path, opt_path, base_path));
        }

        // Volume directory structure
        let data_dir = path.join("DATA");
        if !data_dir.exists() {
            anyhow::bail!(
                "DATA directory not found in {}. Expected structure: VOLxxxxx/DATA/*.DAT",
                path.display()
            );
        }

        // Find DAT file
        let dat_path = std::fs::read_dir(&data_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|e| e.eq_ignore_ascii_case("dat")))
            .ok_or_else(|| anyhow::anyhow!("No .DAT file found in {}", data_dir.display()))?;

        // Find OPT file (same name as DAT)
        let opt_path = dat_path.with_extension("OPT");
        if !opt_path.exists() {
            anyhow::bail!("OPT file not found: {}", opt_path.display());
        }

        Ok((dat_path, opt_path, path.to_path_buf()))
    }

    /// Parse DAT file with þ delimiter and 0x14 text qualifier.
    fn parse_dat(path: &Path) -> anyhow::Result<(Vec<String>, Vec<ConcordanceDocument>)> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut lines = reader.split(b'\n');
        let mut documents = Vec::new();

        // Parse header
        let header_line = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Empty DAT file"))??;
        let fields = Self::parse_dat_row(&header_line);

        if fields.is_empty() {
            anyhow::bail!("No fields found in DAT header");
        }

        // Find field indices
        let begin_idx = fields.iter().position(|f| {
            f.eq_ignore_ascii_case("Begin Bates") || f.eq_ignore_ascii_case("BEGDOC")
        });
        let end_idx = fields
            .iter()
            .position(|f| f.eq_ignore_ascii_case("End Bates") || f.eq_ignore_ascii_case("ENDDOC"));

        // Parse data rows
        for line_result in lines {
            let line = line_result?;
            if line.is_empty() || line.iter().all(|&b| b.is_ascii_whitespace()) {
                continue;
            }

            let values = Self::parse_dat_row(&line);
            if values.is_empty() {
                continue;
            }

            // Build metadata map
            let metadata: HashMap<String, String> = fields
                .iter()
                .zip(values.iter())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            // Extract Bates numbers
            let begin_bates = begin_idx
                .and_then(|i| values.get(i))
                .cloned()
                .or_else(|| values.first().cloned())
                .unwrap_or_default();

            let end_bates = end_idx
                .and_then(|i| values.get(i))
                .cloned()
                .unwrap_or_else(|| begin_bates.clone());

            if begin_bates.is_empty() {
                continue;
            }

            documents.push(ConcordanceDocument {
                begin_bates,
                end_bates,
                metadata,
            });
        }

        Ok((fields, documents))
    }

    /// Parse a single DAT row with þ delimiter and 0x14 text qualifier.
    ///
    /// Format: þfield1þ0x14þfield2þ0x14þfield3þ
    /// - þ marks field boundaries (can be UTF-8 0xC3 0xBE or Latin-1 0xFE)
    /// - 0x14 separates fields
    fn parse_dat_row(line: &[u8]) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current = Vec::new();
        let mut i = 0;

        while i < line.len() {
            // Handle CRLF at end of line
            if line[i] == b'\r' || line[i] == b'\n' {
                i += 1;
                continue;
            }

            // Check for UTF-8 encoded þ (0xC3 0xBE)
            if i + 1 < line.len()
                && line[i] == FIELD_DELIMITER_UTF8[0]
                && line[i + 1] == FIELD_DELIMITER_UTF8[1]
            {
                // þ marks the boundary of a field value
                if !current.is_empty() {
                    let s = String::from_utf8_lossy(&current).trim().to_string();
                    if !s.is_empty() {
                        fields.push(s);
                    }
                    current.clear();
                }
                i += 2; // Skip both bytes of UTF-8 þ
                continue;
            }

            // Check for Latin-1 encoded þ (0xFE)
            if line[i] == FIELD_DELIMITER_LATIN1 {
                if !current.is_empty() {
                    let s = String::from_utf8_lossy(&current).trim().to_string();
                    if !s.is_empty() {
                        fields.push(s);
                    }
                    current.clear();
                }
                i += 1;
                continue;
            }

            // Check for text qualifier / field separator (0x14)
            if line[i] == TEXT_QUALIFIER {
                // 0x14 is a field separator between fields, skip it
                i += 1;
                continue;
            }

            current.push(line[i]);
            i += 1;
        }

        // Don't forget the last field
        if !current.is_empty() {
            let s = String::from_utf8_lossy(&current).trim().to_string();
            if !s.is_empty() {
                fields.push(s);
            }
        }

        fields
    }

    /// Parse OPT file (standard CSV format).
    fn parse_opt(path: &Path) -> anyhow::Result<HashMap<String, OptPage>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut pages = HashMap::new();

        for line_result in reader.lines() {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() < 3 {
                continue;
            }

            let bates_id = parts[0].trim().to_string();
            let volume = parts.get(1).map(|s| s.trim()).unwrap_or("").to_string();
            let image_path = parts.get(2).map(|s| s.trim()).unwrap_or("").to_string();
            let is_first_page = parts.get(3).map(|s| s.trim() == "Y").unwrap_or(false);
            let page_count = parts.get(6).and_then(|s| s.trim().parse::<u32>().ok());

            if bates_id.is_empty() || image_path.is_empty() {
                continue;
            }

            pages.insert(
                bates_id.clone(),
                OptPage {
                    bates_id,
                    volume,
                    image_path,
                    is_first_page,
                    page_count,
                },
            );
        }

        Ok(pages)
    }

    /// Resolve a file path from OPT, trying multiple strategies.
    fn resolve_file_path(&self, opt_page: &OptPage) -> Option<PathBuf> {
        // Convert Windows backslashes to forward slashes
        let normalized_path = opt_page.image_path.replace('\\', "/");
        let path = Path::new(&normalized_path);

        // Try multiple resolution strategies
        let candidates = [
            // Relative to base (volume root)
            self.base_path.join(path),
            // Relative to DATA directory parent
            self.dat_path
                .parent()
                .unwrap_or(&self.base_path)
                .parent()
                .unwrap_or(&self.base_path)
                .join(path),
            // Strip leading component if it matches volume
            path.strip_prefix(&opt_page.volume)
                .map(|p| self.base_path.join(p))
                .unwrap_or_else(|_| self.base_path.join(path)),
            // Just the filename in IMAGES
            self.base_path
                .join("IMAGES")
                .join(path.file_name().unwrap_or_default()),
        ];

        for candidate in &candidates {
            if candidate.exists() {
                return Some(candidate.clone());
            }
        }

        // Return first attempt even if it doesn't exist
        Some(candidates[0].clone())
    }

    /// Generate URL for a document.
    fn generate_url(&self, doc: &ConcordanceDocument) -> String {
        if let Some(prefix) = &self.url_prefix {
            if let Some(page) = self.pages.get(&doc.begin_bates) {
                let normalized = page.image_path.replace('\\', "/");
                let filename = Path::new(&normalized)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&doc.begin_bates);
                return format!("{}/{}", prefix.trim_end_matches('/'), filename);
            }
        }
        format!("concordance://{}..{}", doc.begin_bates, doc.end_bates)
    }
}

#[async_trait::async_trait]
impl ImportSource for ConcordanceImportSource {
    fn format_id(&self) -> &'static str {
        "concordance"
    }

    fn display_name(&self) -> &str {
        "Concordance DAT/OPT"
    }

    fn source_path(&self) -> &Path {
        &self.source_path
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn total_count(&self) -> Option<u64> {
        Some(self.documents.len() as u64)
    }

    async fn run_import(
        &mut self,
        config: &ImportConfig,
        start_position: u64,
    ) -> anyhow::Result<(ImportProgress, ImportStats)> {
        let mut stats = ImportStats::default();
        let mut position = start_position;

        // Get database context
        let ctx = self.settings.create_db_context()?;
        let doc_repo = ctx.documents();

        // Get source ID
        let source_id = config
            .source_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Source ID is required for Concordance import"))?;

        // Skip to resume position
        let docs_to_process = self.documents.iter().skip(start_position as usize);

        for doc in docs_to_process {
            // Check limits
            if config.limit > 0 && stats.imported >= config.limit {
                break;
            }
            if config.scan_limit > 0 && stats.scanned >= config.scan_limit {
                break;
            }

            stats.scanned += 1;

            // Generate URL for dedup check
            let url = self.generate_url(doc);

            // Check for duplicate
            if config.existing_urls.contains(&url) {
                stats.skipped += 1;
                position += 1;
                continue;
            }

            // Find the file for this document
            let opt_page = match self.pages.get(&doc.begin_bates) {
                Some(p) => p,
                None => {
                    tracing::debug!("No OPT mapping for Bates number: {}", doc.begin_bates);
                    stats.missing_files += 1;
                    position += 1;
                    continue;
                }
            };

            let file_path = match self.resolve_file_path(opt_page) {
                Some(p) => p,
                None => {
                    tracing::debug!("Could not resolve path for: {}", opt_page.image_path);
                    stats.missing_files += 1;
                    position += 1;
                    continue;
                }
            };

            if !file_path.exists() {
                tracing::debug!(
                    "File not found: {} (Bates: {})",
                    file_path.display(),
                    doc.begin_bates
                );
                stats.missing_files += 1;
                position += 1;
                continue;
            }

            // Generate title
            let title = format!(
                "{} - {}",
                doc.begin_bates,
                if doc.begin_bates != doc.end_bates {
                    &doc.end_bates
                } else {
                    file_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("Document")
                }
            );

            // For dry run, we just need basic info
            if config.dry_run {
                let file_size = file_path.metadata().map(|m| m.len()).unwrap_or(0);
                let mime_type = guess_mime_type(&file_path);
                println!(
                    "  {} [{}] {} ({}, {} bytes)",
                    style("+").green(),
                    source_id,
                    url,
                    mime_type,
                    file_size
                );
                stats.imported += 1;
                position += 1;
                continue;
            }

            // Build metadata
            let metadata = serde_json::json!({
                "import_source": "concordance",
                "begin_bates": doc.begin_bates,
                "end_bates": doc.end_bates,
                "volume": opt_page.volume,
                "original_path": opt_page.image_path,
                "dat_fields": doc.metadata,
            });

            // Handle different storage modes
            let save_result: anyhow::Result<bool> = match config.storage_mode {
                FileStorageMode::Copy => {
                    // Read content and use standard save helper
                    let content = match std::fs::read(&file_path) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("Failed to read {}: {}", file_path.display(), e);
                            stats.errors += 1;
                            position += 1;
                            continue;
                        }
                    };

                    let mime_type = infer::get(&content)
                        .map(|t| t.mime_type().to_string())
                        .unwrap_or_else(|| guess_mime_type(&file_path));

                    let content_hash = DocumentVersion::compute_hash(&content);
                    let (basename, extension) = extract_filename_parts(&url, &title, &mime_type);
                    let dest_path = content_storage_path_with_name(
                        &config.documents_dir,
                        &content_hash,
                        &basename,
                        &extension,
                    );
                    if let Some(parent) = dest_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(&dest_path, &content)?;

                    let version = DocumentVersion::new_with_metadata(
                        &content,
                        dest_path,
                        mime_type,
                        Some(url.clone()),
                        file_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .map(|s| s.to_string()),
                        None,
                    );

                    let existing = doc_repo.get_by_url(&url).await?;
                    if let Some(mut doc) = existing.into_iter().next() {
                        if doc.add_version(version) {
                            doc_repo.save(&doc).await?;
                        }
                    } else {
                        let mut doc = Document::new(
                            uuid::Uuid::new_v4().to_string(),
                            source_id.to_string(),
                            title,
                            url.clone(),
                            version,
                            metadata.clone(),
                        );
                        doc.tags = config.tags.clone();
                        doc_repo.save(&doc).await?;
                    }

                    Ok(true)
                }
                FileStorageMode::Move | FileStorageMode::HardLink => {
                    // For move/link, we compute hash without loading entire file into memory
                    // (for large files this matters)
                    let content = match std::fs::read(&file_path) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("Failed to read {}: {}", file_path.display(), e);
                            stats.errors += 1;
                            position += 1;
                            continue;
                        }
                    };

                    let content_hash = DocumentVersion::compute_hash(&content);
                    let mime_type = infer::get(&content)
                        .map(|t| t.mime_type().to_string())
                        .unwrap_or_else(|| guess_mime_type(&file_path));

                    let (basename, extension) = extract_filename_parts(&url, &title, &mime_type);
                    let dest_path = content_storage_path_with_name(
                        &config.documents_dir,
                        &content_hash,
                        &basename,
                        &extension,
                    );

                    // Create parent directory
                    if let Some(parent) = dest_path.parent() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            tracing::warn!("Failed to create directory: {}", e);
                            stats.errors += 1;
                            position += 1;
                            continue;
                        }
                    }

                    // Move or link the file
                    let file_op_result = match config.storage_mode {
                        FileStorageMode::Move => std::fs::rename(&file_path, &dest_path),
                        FileStorageMode::HardLink => std::fs::hard_link(&file_path, &dest_path),
                        _ => unreachable!(),
                    };

                    if let Err(e) = file_op_result {
                        // If hard link fails (cross-device), fall back to copy
                        if config.storage_mode == FileStorageMode::HardLink {
                            tracing::debug!("Hard link failed ({}), falling back to copy", e);
                            if let Err(e) = std::fs::copy(&file_path, &dest_path) {
                                tracing::warn!("Failed to copy {}: {}", file_path.display(), e);
                                stats.errors += 1;
                                position += 1;
                                continue;
                            }
                        } else {
                            tracing::warn!("Failed to move {}: {}", file_path.display(), e);
                            stats.errors += 1;
                            position += 1;
                            continue;
                        }
                    }

                    // Create document version
                    let version = DocumentVersion::new_with_metadata(
                        &content,
                        dest_path,
                        mime_type,
                        Some(url.clone()),
                        file_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .map(|s| s.to_string()),
                        None,
                    );

                    // Save document
                    let existing = doc_repo.get_by_url(&url).await?;
                    if let Some(mut doc) = existing.into_iter().next() {
                        if doc.add_version(version) {
                            doc_repo.save(&doc).await?;
                        }
                    } else {
                        let mut doc = Document::new(
                            uuid::Uuid::new_v4().to_string(),
                            source_id.to_string(),
                            title,
                            url.clone(),
                            version,
                            metadata,
                        );
                        doc.tags = config.tags.clone();
                        doc_repo.save(&doc).await?;
                    }

                    Ok(true)
                }
            };

            match save_result {
                Ok(_) => {
                    stats.imported += 1;
                    stats.imported_urls.push(url);
                }
                Err(e) => {
                    tracing::warn!("Failed to save {}: {}", url, e);
                    stats.errors += 1;
                }
            }

            position += 1;

            // Checkpoint
            if config.enable_resume
                && config.checkpoint_interval > 0
                && stats.scanned % config.checkpoint_interval == 0
            {
                let progress = ImportProgress {
                    position,
                    done: false,
                    error: None,
                };
                let _ = self.save_progress(&progress);
            }
        }

        let progress = ImportProgress {
            position,
            done: position >= self.documents.len() as u64,
            error: None,
        };

        Ok((progress, stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dat_row_simple() {
        // þABCþþDEFþ
        let line = [
            0xFE, b'A', b'B', b'C', 0xFE, 0x14, 0xFE, b'D', b'E', b'F', 0xFE,
        ];
        let fields = ConcordanceImportSource::parse_dat_row(&line);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], "ABC");
        assert_eq!(fields[1], "DEF");
    }

    #[test]
    fn test_parse_dat_row_utf8() {
        // UTF-8 encoded þ (0xC3 0xBE) as used in real Concordance files
        // Format: þField1þ\x14þField2þ
        let line = [
            0xC3, 0xBE, b'A', b'B', b'C', 0xC3, 0xBE, 0x14, 0xC3, 0xBE, b'D', b'E', b'F', 0xC3,
            0xBE,
        ];
        let fields = ConcordanceImportSource::parse_dat_row(&line);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], "ABC");
        assert_eq!(fields[1], "DEF");
    }

    #[test]
    fn test_generate_url_with_prefix() {
        let mut pages = HashMap::new();
        pages.insert(
            "EFTA00000001".to_string(),
            OptPage {
                bates_id: "EFTA00000001".to_string(),
                volume: "VOL00001".to_string(),
                image_path: r"IMAGES\0001\EFTA00000001.pdf".to_string(),
                is_first_page: true,
                page_count: Some(1),
            },
        );

        let source = ConcordanceImportSource {
            source_path: PathBuf::from("/tmp"),
            dat_path: PathBuf::from("/tmp/test.DAT"),
            opt_path: PathBuf::from("/tmp/test.OPT"),
            base_path: PathBuf::from("/tmp"),
            dat_fields: vec!["Begin Bates".into(), "End Bates".into()],
            documents: Vec::new(),
            pages,
            multi_page_mode: MultiPageMode::First,
            url_prefix: Some("https://www.justice.gov/epstein/files/DataSet%201".into()),
            settings: foiacquire::config::Settings::default(),
        };

        let doc = ConcordanceDocument {
            begin_bates: "EFTA00000001".to_string(),
            end_bates: "EFTA00000001".to_string(),
            metadata: HashMap::new(),
        };

        assert_eq!(
            source.generate_url(&doc),
            "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf"
        );
    }

    #[test]
    fn test_generate_url_without_prefix() {
        let source = ConcordanceImportSource {
            source_path: PathBuf::from("/tmp"),
            dat_path: PathBuf::from("/tmp/test.DAT"),
            opt_path: PathBuf::from("/tmp/test.OPT"),
            base_path: PathBuf::from("/tmp"),
            dat_fields: vec!["Begin Bates".into(), "End Bates".into()],
            documents: Vec::new(),
            pages: HashMap::new(),
            multi_page_mode: MultiPageMode::First,
            url_prefix: None,
            settings: foiacquire::config::Settings::default(),
        };

        let doc = ConcordanceDocument {
            begin_bates: "EFTA00000001".to_string(),
            end_bates: "EFTA00000005".to_string(),
            metadata: HashMap::new(),
        };

        assert_eq!(
            source.generate_url(&doc),
            "concordance://EFTA00000001..EFTA00000005"
        );
    }

    #[test]
    fn test_generate_url_prefix_with_trailing_slash() {
        let mut pages = HashMap::new();
        pages.insert(
            "DOC001".to_string(),
            OptPage {
                bates_id: "DOC001".to_string(),
                volume: "VOL001".to_string(),
                image_path: r"IMAGES\DOC001.tif".to_string(),
                is_first_page: true,
                page_count: None,
            },
        );

        let source = ConcordanceImportSource {
            source_path: PathBuf::from("/tmp"),
            dat_path: PathBuf::from("/tmp/test.DAT"),
            opt_path: PathBuf::from("/tmp/test.OPT"),
            base_path: PathBuf::from("/tmp"),
            dat_fields: Vec::new(),
            documents: Vec::new(),
            pages,
            multi_page_mode: MultiPageMode::First,
            url_prefix: Some("https://example.com/files/".into()),
            settings: foiacquire::config::Settings::default(),
        };

        let doc = ConcordanceDocument {
            begin_bates: "DOC001".to_string(),
            end_bates: "DOC001".to_string(),
            metadata: HashMap::new(),
        };

        // Should not produce double slash
        assert_eq!(
            source.generate_url(&doc),
            "https://example.com/files/DOC001.tif"
        );
    }
}
