//! WARC archive file importer.
//!
//! Supports importing documents from WARC (Web ARChive) files, commonly
//! produced by web scrapers like wget, HTTrack, and archive.org.
//!
//! Features:
//! - Gzip and uncompressed WARC files
//! - Byte-offset resume for uncompressed files
//! - Source auto-detection from URL patterns
//! - HTTP response parsing and content extraction

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use console::style;
use warc::{WarcHeader, WarcReader};

use crate::{ImportConfig, ImportProgress, ImportSource, ImportStats};
use foiacquire::config::Settings;
use foiacquire::storage::{save_document_async, DocumentInput};

/// A BufReader wrapper that tracks total bytes consumed.
/// Uses Arc<AtomicU64> so position can be read even after reader is consumed.
struct PositionTrackingReader<R> {
    inner: BufReader<R>,
    position: Arc<AtomicU64>,
}

impl<R: Read> PositionTrackingReader<R> {
    fn new(inner: R, start_position: u64) -> Self {
        Self {
            inner: BufReader::with_capacity(1024 * 1024, inner),
            position: Arc::new(AtomicU64::new(start_position)),
        }
    }

    fn position_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.position)
    }
}

impl<R: Read> Read for PositionTrackingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.position.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

impl<R: Read> BufRead for PositionTrackingReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.position.fetch_add(amt as u64, Ordering::Relaxed);
        self.inner.consume(amt)
    }
}

/// HTTP response headers extracted from WARC body.
struct HttpResponseHeaders {
    status_ok: bool,
    content_type: Option<String>,
}

/// Parse HTTP response from WARC body bytes.
/// Returns (headers, body content) if successful.
fn parse_http_response(data: &[u8]) -> Option<(HttpResponseHeaders, &[u8])> {
    // Find header/body separator (double CRLF)
    let separator = b"\r\n\r\n";
    let sep_pos = data.windows(separator.len()).position(|w| w == separator)?;

    let header_bytes = &data[..sep_pos];
    let body = &data[sep_pos + separator.len()..];

    // Parse status line and headers
    let header_str = std::str::from_utf8(header_bytes).ok()?;
    let mut lines = header_str.lines();

    // Parse status line: "HTTP/1.1 200 OK"
    let status_line = lines.next()?;
    let status_ok = status_line.contains(" 200 ") || status_line.contains(" 206 ");

    // Parse headers
    let mut content_type = None;
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_lowercase();
            let value = value.trim();
            if key == "content-type" {
                // Extract just the MIME type, not charset etc.
                content_type = Some(value.split(';').next().unwrap_or(value).trim().to_string());
            }
        }
    }

    Some((
        HttpResponseHeaders {
            status_ok,
            content_type,
        },
        body,
    ))
}

/// Guess MIME type from URL extension.
pub fn guess_mime_type_from_url(url: &str) -> String {
    let path = url.split('?').next().unwrap_or(url);
    if path.ends_with(".pdf") || path.ends_with(".PDF") {
        "application/pdf".to_string()
    } else if path.ends_with(".html") || path.ends_with(".htm") {
        "text/html".to_string()
    } else if path.ends_with(".txt") {
        "text/plain".to_string()
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        "image/jpeg".to_string()
    } else if path.ends_with(".png") {
        "image/png".to_string()
    } else if path.ends_with(".gif") {
        "image/gif".to_string()
    } else if path.ends_with(".doc") {
        "application/msword".to_string()
    } else if path.ends_with(".docx") {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document".to_string()
    } else {
        "application/octet-stream".to_string()
    }
}

/// WARC archive import source.
pub struct WarcImportSource {
    /// Path to the WARC file.
    warc_path: PathBuf,
    /// Whether this is a gzip-compressed file.
    is_gzip: bool,
    /// Source map for URL -> source_id auto-detection.
    source_map: HashMap<String, String>,
    /// Explicit source ID (overrides auto-detection).
    explicit_source_id: Option<String>,
    /// Filter regex for URLs.
    filter_regex: Option<regex::Regex>,
    /// Settings for database access.
    settings: Settings,
}

impl WarcImportSource {
    /// Create a new WARC import source.
    ///
    /// - `warc_path`: Path to the .warc or .warc.gz file
    /// - `source_id`: Explicit source ID (if None, auto-detects from URL patterns)
    /// - `filter`: Optional regex pattern to filter URLs
    /// - `settings`: Application settings for database access
    pub async fn new(
        warc_path: PathBuf,
        source_id: Option<String>,
        filter: Option<&str>,
        settings: Settings,
    ) -> anyhow::Result<Self> {
        // Detect if gzipped
        let is_gzip = warc_path.extension().is_some_and(|ext| ext == "gz")
            || warc_path.to_string_lossy().contains(".warc.gz");

        // Build source map for auto-detection
        let ctx = settings.create_db_context()?;
        let source_repo = ctx.sources();
        let all_sources = source_repo.get_all().await?;

        let source_map: HashMap<String, String> = all_sources
            .iter()
            .map(|s| (s.base_url.clone(), s.id.clone()))
            .collect();

        // Verify explicit source exists if provided
        if let Some(ref sid) = source_id {
            if source_repo.get(sid).await?.is_none() {
                anyhow::bail!(
                    "Source '{}' not found. Use 'source list' to see available sources.",
                    sid
                );
            }
        }

        // Compile filter regex
        let filter_regex = if let Some(pattern) = filter {
            Some(regex::Regex::new(pattern)?)
        } else {
            None
        };

        Ok(Self {
            warc_path,
            is_gzip,
            source_map,
            explicit_source_id: source_id,
            filter_regex,
            settings,
        })
    }

    /// Find source ID for a URL using auto-detection.
    fn find_source_for_url(&self, url: &str) -> Option<String> {
        // If explicitly provided, use that
        if let Some(ref sid) = self.explicit_source_id {
            return Some(sid.clone());
        }
        // Otherwise, match against source base_urls
        for (base_url, sid) in &self.source_map {
            if url.starts_with(base_url) {
                return Some(sid.clone());
            }
        }
        None
    }

    /// Process WARC records from a reader.
    async fn process_records<R: BufRead>(
        &self,
        reader: WarcReader<R>,
        position_tracker: Option<Arc<AtomicU64>>,
        can_checkpoint: bool,
        config: &ImportConfig,
        start_position: u64,
    ) -> anyhow::Result<(ImportProgress, ImportStats)> {
        let mut stats = ImportStats::default();
        let mut position = start_position;

        let ctx = self.settings.create_db_context()?;
        let doc_repo = ctx.documents();
        let documents_dir = &config.documents_dir;

        // Create mutable copy of existing_urls for session dedup
        let mut existing_urls = config.existing_urls.clone();

        for record_result in reader.iter_records() {
            // Check import limit
            if config.limit > 0 && stats.imported >= config.limit {
                break;
            }

            // Check scan limit
            if config.scan_limit > 0 && stats.scanned >= config.scan_limit {
                break;
            }

            stats.scanned += 1;

            // Checkpoint at intervals (uncompressed files only)
            if can_checkpoint
                && config.enable_resume
                && !config.dry_run
                && config.checkpoint_interval > 0
                && stats.scanned % config.checkpoint_interval == 0
            {
                if let Some(ref tracker) = position_tracker {
                    position = tracker.load(Ordering::Relaxed);
                    let progress = ImportProgress {
                        position,
                        done: false,
                        error: None,
                    };
                    let _ = self.save_progress(&progress);
                }
            }

            let record = match record_result {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!("Skipping malformed record: {}", e);
                    continue;
                }
            };

            // Only process response records
            let warc_type = record.header(WarcHeader::WarcType);
            if warc_type.as_deref() != Some("response") {
                continue;
            }

            // Get target URI
            let target_uri = match record.header(WarcHeader::TargetURI) {
                Some(uri) => uri.to_string(),
                None => continue,
            };

            // Apply filter
            if let Some(ref regex) = self.filter_regex {
                if !regex.is_match(&target_uri) {
                    stats.filtered += 1;
                    continue;
                }
            }

            // Get body content
            let body = record.body();
            if body.is_empty() {
                continue;
            }

            // Parse HTTP response from body
            let (headers, content) = match parse_http_response(body) {
                Some(parsed) => parsed,
                None => {
                    tracing::debug!("Could not parse HTTP response for {}", target_uri);
                    continue;
                }
            };

            // Skip non-success responses
            if !headers.status_ok {
                continue;
            }

            // Skip empty content
            if content.is_empty() {
                continue;
            }

            // Auto-detect source from URL
            let source_id = match self.find_source_for_url(&target_uri) {
                Some(sid) => sid,
                None => {
                    stats.no_source += 1;
                    tracing::debug!("No matching source for URL: {}", target_uri);
                    continue;
                }
            };

            // Check if document already exists (O(1) HashSet lookup)
            if existing_urls.contains(&target_uri) {
                stats.skipped += 1;
                continue;
            }

            // Extract title from URL
            let title = foiacquire::scrapers::extract_title_from_url(&target_uri);

            // Determine MIME type
            let mime_type = headers
                .content_type
                .clone()
                .unwrap_or_else(|| guess_mime_type_from_url(&target_uri));

            if config.dry_run {
                println!(
                    "  {} [{}] {} ({}, {} bytes)",
                    style("+").green(),
                    source_id,
                    target_uri,
                    mime_type,
                    content.len()
                );
                stats.imported += 1;
            } else {
                let input = DocumentInput {
                    url: target_uri.clone(),
                    title,
                    mime_type,
                    metadata: serde_json::json!({}),
                    original_filename: None,
                    server_date: None,
                };

                match save_document_async(&doc_repo, content, &input, &source_id, documents_dir)
                    .await
                {
                    Ok(_) => {
                        // Add to URL cache to avoid re-importing in same session
                        existing_urls.insert(target_uri);
                        stats.imported += 1;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to import {}: {}", target_uri, e);
                        stats.errors += 1;
                    }
                }
            }
        }

        // Get final position
        if let Some(ref tracker) = position_tracker {
            position = tracker.load(Ordering::Relaxed);
        }

        let progress = ImportProgress {
            position,
            done: true,
            error: None,
        };

        Ok((progress, stats))
    }
}

#[async_trait::async_trait]
impl ImportSource for WarcImportSource {
    fn format_id(&self) -> &'static str {
        "warc"
    }

    fn display_name(&self) -> &str {
        if self.is_gzip {
            "WARC archive (gzip)"
        } else {
            "WARC archive"
        }
    }

    fn source_path(&self) -> &Path {
        &self.warc_path
    }

    fn supports_resume(&self) -> bool {
        // Only uncompressed WARC files support byte-offset resume
        !self.is_gzip
    }

    fn load_progress(&self) -> Option<ImportProgress> {
        let path = self.progress_path();
        let content = std::fs::read_to_string(&path).ok()?;
        let content = content.trim();

        // Handle legacy format: "done", "offset:12345", or "error:message"
        if content == "done" {
            return Some(ImportProgress {
                position: 0,
                done: true,
                error: None,
            });
        }

        if let Some(error_msg) = content.strip_prefix("error:") {
            return Some(ImportProgress {
                position: 0,
                done: false,
                error: Some(error_msg.to_string()),
            });
        }

        if let Some(offset_str) = content.strip_prefix("offset:") {
            if let Ok(offset) = offset_str.parse::<u64>() {
                return Some(ImportProgress {
                    position: offset,
                    done: false,
                    error: None,
                });
            }
        }

        // Try JSON format
        serde_json::from_str(content).ok()
    }

    fn save_progress(&self, progress: &ImportProgress) -> std::io::Result<()> {
        let path = self.progress_path();
        // Use legacy format for compatibility
        let content = if progress.done {
            "done".to_string()
        } else if let Some(ref err) = progress.error {
            format!("error:{}", err)
        } else {
            format!("offset:{}", progress.position)
        };
        std::fs::write(&path, content)
    }

    async fn run_import(
        &mut self,
        config: &ImportConfig,
        start_position: u64,
    ) -> anyhow::Result<(ImportProgress, ImportStats)> {
        if !self.warc_path.exists() {
            return Err(anyhow::anyhow!(
                "File not found: {}",
                self.warc_path.display()
            ));
        }

        if self.is_gzip {
            // Gzip files: no seeking, no checkpointing
            // Note: start_position is ignored for gzip
            if start_position > 0 {
                tracing::warn!(
                    "Cannot resume gzip WARC from position {}, starting from beginning",
                    start_position
                );
            }

            match WarcReader::from_path_gzip(&self.warc_path) {
                Ok(reader) => self.process_records(reader, None, false, config, 0).await,
                Err(e) => {
                    let progress = ImportProgress {
                        position: 0,
                        done: false,
                        error: Some(e.to_string()),
                    };
                    if config.enable_resume && !config.dry_run {
                        let _ = self.save_progress(&progress);
                    }
                    Err(anyhow::anyhow!("Failed to open WARC file: {}", e))
                }
            }
        } else {
            // Uncompressed files: seek support and byte-offset checkpointing
            let file_result = (|| -> std::io::Result<_> {
                let mut file = std::fs::File::open(&self.warc_path)?;
                if start_position > 0 {
                    file.seek(SeekFrom::Start(start_position))?;
                }
                Ok(file)
            })();

            match file_result {
                Ok(file) => {
                    let tracking_reader = PositionTrackingReader::new(file, start_position);
                    let tracker = tracking_reader.position_handle();
                    let reader = WarcReader::new(tracking_reader);
                    self.process_records(reader, Some(tracker), true, config, start_position)
                        .await
                }
                Err(e) => {
                    let progress = ImportProgress {
                        position: start_position,
                        done: false,
                        error: Some(e.to_string()),
                    };
                    if config.enable_resume && !config.dry_run {
                        let _ = self.save_progress(&progress);
                    }
                    Err(anyhow::anyhow!("Failed to open WARC file: {}", e))
                }
            }
        }
    }
}
