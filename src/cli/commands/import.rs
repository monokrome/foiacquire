//! WARC file import command.

use std::path::PathBuf;
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use crate::config::Settings;

/// Import documents from WARC archive files.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_import(
    settings: &Settings,
    files: &[PathBuf],
    source_id: Option<&str>,
    filter: Option<&str>,
    limit: usize,
    scan_limit: usize,
    dry_run: bool,
    resume: bool,
    checkpoint_interval: usize,
) -> anyhow::Result<()> {
    use std::collections::{HashMap, HashSet};
    use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use warc::{WarcHeader, WarcReader};

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

    let documents_dir = settings.documents_dir.clone();
    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();
    let source_repo = ctx.sources();

    // Pre-load all existing URLs into a HashSet for O(1) duplicate detection.
    // This is much faster than querying the DB for each WARC record.
    println!(
        "{} Loading existing URLs for duplicate detection...",
        style("→").cyan()
    );
    let mut existing_urls: HashSet<String> = doc_repo.get_all_urls_set().await.unwrap_or_default();
    println!("  {} existing URLs loaded", existing_urls.len());

    // Load all sources for URL matching
    let all_sources = source_repo.get_all().await?;

    // Build URL prefix -> source_id map for auto-detection
    let source_map: HashMap<String, String> = all_sources
        .iter()
        .map(|s| (s.base_url.clone(), s.id.clone()))
        .collect();

    // If source_id provided, verify it exists
    if let Some(sid) = source_id {
        if source_repo.get(sid).await?.is_none() {
            println!(
                "{} Source '{}' not found. Use 'source list' to see available sources.",
                style("✗").red(),
                sid
            );
            return Ok(());
        }
    }

    // Helper to find source from URL
    let find_source_for_url = |url: &str| -> Option<String> {
        // If explicitly provided, use that
        if let Some(sid) = source_id {
            return Some(sid.to_string());
        }
        // Otherwise, match against source base_urls
        for (base_url, sid) in &source_map {
            if url.starts_with(base_url) {
                return Some(sid.clone());
            }
        }
        None
    };

    // Compile filter regex if provided
    let filter_regex = if let Some(pattern) = filter {
        Some(regex::Regex::new(pattern)?)
    } else {
        None
    };

    if dry_run {
        println!(
            "{} Dry run mode - no changes will be made",
            style("!").yellow()
        );
    }

    let mut total_imported = 0;
    let mut total_skipped = 0;
    let mut total_filtered = 0;
    let mut total_no_source = 0;
    let mut total_errors = 0;
    let mut total_scanned = 0usize;

    for warc_path in files {
        println!(
            "\n{} Processing: {}",
            style("→").cyan(),
            warc_path.display()
        );

        if !warc_path.exists() {
            println!(
                "{} File not found: {}",
                style("✗").red(),
                warc_path.display()
            );
            total_errors += 1;
            continue;
        }

        // Check for progress sidecar file when --resume is enabled
        let progress_path = warc_path.with_extension(
            warc_path
                .extension()
                .map(|e| format!("{}.progress", e.to_string_lossy()))
                .unwrap_or_else(|| "progress".to_string()),
        );

        // Detect if gzipped (needed before parsing progress)
        let is_gzip = warc_path.extension().is_some_and(|ext| ext == "gz")
            || warc_path.to_string_lossy().contains(".warc.gz");

        // Read previous progress if resuming
        // Format: "done", "offset:12345" (byte offset for uncompressed), or "error:message"
        let mut resume_byte_offset: u64 = 0;
        let mut file_fully_processed = false;

        if resume && progress_path.exists() {
            if let Ok(progress_str) = std::fs::read_to_string(&progress_path) {
                let progress_str = progress_str.trim();
                if progress_str == "done" {
                    println!("  {} Already fully processed, skipping", style("✓").green());
                    file_fully_processed = true;
                } else if let Some(error_msg) = progress_str.strip_prefix("error:") {
                    println!(
                        "  {} Previous attempt failed: {}",
                        style("!").yellow(),
                        error_msg
                    );
                    println!("  {} Retrying from start", style("→").cyan());
                } else if let Some(offset_str) = progress_str.strip_prefix("offset:") {
                    if is_gzip {
                        // Can't seek in gzip, ignore offset and start over
                        println!(
                            "  {} Gzip file - cannot resume from offset, starting over",
                            style("!").yellow()
                        );
                    } else if let Ok(offset) = offset_str.parse::<u64>() {
                        resume_byte_offset = offset;
                        println!(
                            "  {} Resuming from byte offset {}",
                            style("→").cyan(),
                            resume_byte_offset
                        );
                    }
                }
            }
        }

        if file_fully_processed {
            continue;
        }

        // Progress bar
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap(),
        );
        pb.enable_steady_tick(Duration::from_millis(100));

        let mut file_imported = 0;
        let mut file_skipped = 0;
        let mut file_filtered = 0;
        let mut file_no_source = 0;
        let mut file_completed = true; // Track if we processed entire file
        let mut file_records_processed: usize = 0;

        // Process WARC records - macro to avoid code duplication
        // $position_tracker: Option<Arc<AtomicU64>> for byte offset tracking
        // $can_checkpoint: bool - whether this file type supports checkpointing
        macro_rules! process_warc {
            ($reader:expr, $position_tracker:expr, $can_checkpoint:expr) => {
                for record_result in $reader.iter_records() {
                    file_records_processed += 1;

                    // Check import limit
                    if limit > 0 && total_imported >= limit {
                        pb.finish_with_message(format!(
                            "Import limit reached ({} documents)",
                            limit
                        ));
                        file_completed = false;
                        break;
                    }

                    // Check scan limit
                    if scan_limit > 0 && total_scanned >= scan_limit {
                        pb.finish_with_message(format!(
                            "Scan limit reached ({} records)",
                            scan_limit
                        ));
                        file_completed = false;
                        break;
                    }

                    total_scanned += 1;

                    // Write checkpoint at intervals (uncompressed files only)
                    if $can_checkpoint
                        && resume
                        && !dry_run
                        && checkpoint_interval > 0
                        && file_records_processed % checkpoint_interval == 0
                    {
                        if let Some(ref tracker) = $position_tracker {
                            let offset = tracker.load(Ordering::Relaxed);
                            let _ = std::fs::write(&progress_path, format!("offset:{}", offset));
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
                    if let Some(ref regex) = filter_regex {
                        if !regex.is_match(&target_uri) {
                            file_filtered += 1;
                            continue;
                        }
                    }

                    pb.set_message(format!(
                        "Processing: {}",
                        &target_uri[..target_uri.len().min(60)]
                    ));

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
                    let detected_source = find_source_for_url(&target_uri);
                    let effective_source_id = match &detected_source {
                        Some(sid) => sid.as_str(),
                        None => {
                            file_no_source += 1;
                            tracing::debug!("No matching source for URL: {}", target_uri);
                            continue;
                        }
                    };

                    // Check if document already exists (O(1) HashSet lookup)
                    if existing_urls.contains(&target_uri) {
                        file_skipped += 1;
                        continue;
                    }

                    // Extract title from URL
                    let title = crate::scrapers::extract_title_from_url(&target_uri);

                    // Determine MIME type
                    let mime_type = headers
                        .content_type
                        .clone()
                        .unwrap_or_else(|| guess_mime_type(&target_uri));

                    if dry_run {
                        println!(
                            "  {} [{}] {} ({}, {} bytes)",
                            style("+").green(),
                            effective_source_id,
                            target_uri,
                            mime_type,
                            content.len()
                        );
                        file_imported += 1;
                        total_imported += 1;
                    } else {
                        // Create ScraperResult for helper
                        let result = crate::scrapers::ScraperResult::new(
                            target_uri.clone(),
                            title,
                            content.to_vec(),
                            mime_type,
                        );

                        // Save using async helper
                        match crate::cli::helpers::save_scraped_document_async(
                            &doc_repo,
                            content,
                            &result,
                            effective_source_id,
                            &documents_dir,
                        )
                        .await
                        {
                            Ok(_) => {
                                // Add to URL cache to avoid re-importing in same session
                                existing_urls.insert(target_uri.clone());
                                file_imported += 1;
                                total_imported += 1;
                            }
                            Err(e) => {
                                tracing::warn!("Failed to import {}: {}", target_uri, e);
                                total_errors += 1;
                            }
                        }
                    }
                }
            };
        }

        // Open and process WARC file
        // Track final position for checkpoint (uncompressed only)
        let mut final_position: Option<u64> = None;

        if is_gzip {
            // Gzip files: no seeking, no checkpointing
            match WarcReader::from_path_gzip(warc_path) {
                Ok(reader) => process_warc!(reader, None::<Arc<AtomicU64>>, false),
                Err(e) => {
                    println!("{} Failed to open WARC file: {}", style("✗").red(), e);
                    total_errors += 1;
                    if resume && !dry_run {
                        let _ = std::fs::write(&progress_path, format!("error:{}", e));
                    }
                    continue;
                }
            }
        } else {
            // Uncompressed files: seek support and byte-offset checkpointing
            let file_result = (|| -> std::io::Result<_> {
                let mut file = std::fs::File::open(warc_path)?;
                if resume_byte_offset > 0 {
                    file.seek(SeekFrom::Start(resume_byte_offset))?;
                }
                Ok(file)
            })();

            match file_result {
                Ok(file) => {
                    let tracking_reader = PositionTrackingReader::new(file, resume_byte_offset);
                    let tracker = tracking_reader.position_handle();
                    let reader = WarcReader::new(tracking_reader);
                    process_warc!(reader, Some(tracker.clone()), true);
                    final_position = Some(tracker.load(Ordering::Relaxed));
                }
                Err(e) => {
                    println!("{} Failed to open WARC file: {}", style("✗").red(), e);
                    total_errors += 1;
                    if resume && !dry_run {
                        let _ = std::fs::write(&progress_path, format!("error:{}", e));
                    }
                    continue;
                }
            }
        }

        pb.finish_and_clear();

        println!(
            "  {} imported, {} skipped (existing), {} filtered, {} no source",
            style(file_imported).green(),
            style(file_skipped).yellow(),
            style(file_filtered).dim(),
            style(file_no_source).dim()
        );

        // Write progress file when --resume is enabled
        if resume && !dry_run {
            let progress_content = if file_completed {
                Some("done".to_string())
            } else {
                // Uncompressed file: save byte offset for true resume
                // Gzip file: can't resume mid-file, don't write checkpoint
                final_position.map(|offset| format!("offset:{}", offset))
            };
            if let Some(content) = progress_content {
                if let Err(e) = std::fs::write(&progress_path, content) {
                    tracing::warn!("Failed to write progress file: {}", e);
                }
            }
        }

        total_skipped += file_skipped;
        total_filtered += file_filtered;
        total_no_source += file_no_source;
    }

    // Summary
    println!("\n{} Import complete:", style("✓").green());
    println!("  Records scanned:    {}", style(total_scanned).dim());
    println!("  Documents imported: {}", style(total_imported).green());
    println!("  Documents skipped:  {}", style(total_skipped).yellow());
    println!("  Records filtered:   {}", style(total_filtered).dim());
    if total_no_source > 0 {
        println!(
            "  No matching source: {} (use --source to specify)",
            style(total_no_source).yellow()
        );
    }
    if total_errors > 0 {
        println!("  Errors:             {}", style(total_errors).red());
    }

    Ok(())
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

/// HTTP response headers extracted from WARC body.
struct HttpResponseHeaders {
    status_ok: bool,
    content_type: Option<String>,
}

/// Guess MIME type from URL extension.
fn guess_mime_type(url: &str) -> String {
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
