//! Import commands for WARC files, URL lists, and stdin content.

use std::io::Read;
use std::path::PathBuf;

use console::style;

use foiacquire::config::Settings;
use foiacquire::models::{CrawlUrl, DiscoveryMethod};
use foiacquire_import::{
    guess_mime_type_from_url, FileStorageMode, ImportRunner, ImportStats, WarcImportSource,
};

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
    settings.ensure_directories()?;

    if dry_run {
        println!(
            "{} Dry run mode - no changes will be made",
            style("!").yellow()
        );
    }

    // Create the import runner
    let runner = ImportRunner::new(settings);

    // Create shared config with existing URLs loaded
    let mut config = runner
        .create_config(
            source_id.map(|s| s.to_string()),
            limit,
            dry_run,
            resume,
            FileStorageMode::Copy, // WARC always copies (content is embedded)
        )
        .await?;

    // Set additional config options
    config.scan_limit = scan_limit;
    config.checkpoint_interval = checkpoint_interval;

    // Track aggregate stats across all files
    let mut total_stats = ImportStats::default();
    let mut total_errors = 0usize;

    for warc_path in files {
        if !warc_path.exists() {
            println!(
                "{} File not found: {}",
                style("✗").red(),
                warc_path.display()
            );
            total_errors += 1;
            continue;
        }

        // Create import source for this file
        let mut source = match WarcImportSource::new(
            warc_path.clone(),
            source_id.map(|s| s.to_string()),
            filter,
            settings.clone(),
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "{} Failed to initialize WARC source {}: {}",
                    style("✗").red(),
                    warc_path.display(),
                    e
                );
                total_errors += 1;
                continue;
            }
        };

        // Run import for this file
        match runner.run(&mut source, &config).await {
            Ok(stats) => {
                total_stats.merge(&stats);

                // Check if we hit import limit
                if limit > 0 && total_stats.imported >= limit {
                    println!(
                        "{} Import limit reached ({} documents)",
                        style("→").cyan(),
                        limit
                    );
                    break;
                }

                // Check if we hit scan limit
                if scan_limit > 0 && total_stats.scanned >= scan_limit {
                    println!(
                        "{} Scan limit reached ({} records)",
                        style("→").cyan(),
                        scan_limit
                    );
                    break;
                }
            }
            Err(e) => {
                println!(
                    "{} Error processing {}: {}",
                    style("✗").red(),
                    warc_path.display(),
                    e
                );
                total_errors += 1;
            }
        }
    }

    // Print aggregate summary if multiple files
    if files.len() > 1 {
        println!("\n{} Total across all files:", style("✓").green());
        println!("  Records scanned:    {}", style(total_stats.scanned).dim());
        println!(
            "  Documents imported: {}",
            style(total_stats.imported).green()
        );
        println!(
            "  Documents skipped:  {}",
            style(total_stats.skipped).yellow()
        );
        if total_stats.filtered > 0 {
            println!(
                "  Records filtered:   {}",
                style(total_stats.filtered).dim()
            );
        }
        if total_stats.no_source > 0 {
            println!(
                "  No matching source: {} (use --source to specify)",
                style(total_stats.no_source).yellow()
            );
        }
    }

    if total_errors > 0 {
        anyhow::bail!("{} error(s) during import", total_errors);
    }

    Ok(())
}

/// Import URLs from a file to add to the crawl queue.
///
/// Each line in the file should contain a single URL. Empty lines and lines
/// starting with # are ignored.
pub async fn cmd_import_urls(
    settings: &Settings,
    file: &PathBuf,
    source_id: &str,
    _method: &str,
    skip_invalid: bool,
) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use url::Url;

    settings.ensure_directories()?;
    let ctx = settings.create_db_context()?;
    let crawl_repo = ctx.crawl();

    // Read URLs from file
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    let mut added = 0usize;
    let mut skipped = 0usize;
    let mut invalid = 0usize;
    let mut line_num = 0usize;

    println!(
        "{} Importing URLs from file for source '{}'...",
        style("→").cyan(),
        source_id
    );

    for line in reader.lines() {
        line_num += 1;
        let line = line?;
        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Validate URL
        if Url::parse(trimmed).is_err() {
            if skip_invalid {
                invalid += 1;
                continue;
            } else {
                anyhow::bail!("Invalid URL at line {}: {}", line_num, trimmed);
            }
        }

        // Create crawl URL entry
        let crawl_url = CrawlUrl::new(
            trimmed.to_string(),
            source_id.to_string(),
            DiscoveryMethod::Manual,
            None,
            0,
        );

        match crawl_repo.add_url(&crawl_url).await {
            Ok(true) => added += 1,
            Ok(false) => skipped += 1, // Already exists
            Err(e) => {
                if skip_invalid {
                    invalid += 1;
                    tracing::warn!("Failed to add URL at line {}: {}", line_num, e);
                } else {
                    return Err(e.into());
                }
            }
        }
    }

    println!(
        "{} Import complete: {} added, {} already existed, {} invalid",
        style("✓").green(),
        added,
        skipped,
        invalid
    );

    Ok(())
}

/// Import document content from stdin.
///
/// Reads content from stdin and saves it as a document with the specified URL.
pub async fn cmd_import_stdin(
    settings: &Settings,
    url: &str,
    source_id: &str,
    content_type: Option<&str>,
    filename: Option<&str>,
) -> anyhow::Result<()> {
    use chrono::Utc;
    use url::Url;

    use foiacquire::models::{Document, DocumentVersion, Source, SourceType};
    use foiacquire::repository::extract_filename_parts;
    use foiacquire::storage::compute_storage_path_with_dedup;

    settings.ensure_directories()?;
    let ctx = settings.create_db_context()?;
    let source_repo = ctx.sources();
    let doc_repo = ctx.documents();

    // Validate URL
    let parsed_url = Url::parse(url)?;

    // Read content from stdin
    let mut content = Vec::new();
    std::io::stdin().read_to_end(&mut content)?;

    if content.is_empty() {
        anyhow::bail!("No content received from stdin");
    }

    println!(
        "{} Importing {} bytes from stdin for URL: {}",
        style("→").cyan(),
        content.len(),
        url
    );

    // Detect content type if not specified
    let mime_type = content_type
        .map(|s| s.to_string())
        .or_else(|| infer::get(&content).map(|t| t.mime_type().to_string()))
        .unwrap_or_else(|| guess_mime_type_from_url(url));

    // Extract filename from URL if not specified
    let original_filename = filename.map(|s| s.to_string()).or_else(|| {
        parsed_url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    });

    // Ensure source exists
    let source = match source_repo.get(source_id).await? {
        Some(s) => s,
        None => {
            println!("  {} Creating source '{}'...", style("→").dim(), source_id);
            let new_source = Source {
                id: source_id.to_string(),
                name: source_id.to_string(),
                source_type: SourceType::Custom,
                base_url: format!(
                    "{}://{}",
                    parsed_url.scheme(),
                    parsed_url.host_str().unwrap_or("unknown")
                ),
                metadata: serde_json::json!({}),
                created_at: Utc::now(),
                last_scraped: None,
            };
            source_repo.save(&new_source).await?;
            new_source
        }
    };

    // Compute content hash and storage path
    let content_hash = DocumentVersion::compute_hash(&content);
    let title = original_filename
        .clone()
        .unwrap_or_else(|| "document".to_string());
    let (basename, extension) = extract_filename_parts(url, &title, &mime_type);
    let (relative_path, dedup_index) = compute_storage_path_with_dedup(
        &settings.documents_dir,
        &content_hash,
        &basename,
        &extension,
        &content,
    );

    // Save content to file
    let content_path = settings.documents_dir.join(&relative_path);
    if let Some(parent) = content_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&content_path, &content)?;

    // Create document version
    let mut version = DocumentVersion::new_with_metadata(
        &content,
        mime_type.clone(),
        Some(url.to_string()),
        original_filename.clone(),
        None, // No server date for stdin import
    );
    version.dedup_index = dedup_index;

    // Check for existing document at this URL
    let existing = doc_repo.get_by_url(url).await?;

    let (doc_id, is_new) = if let Some(mut doc) = existing.into_iter().next() {
        let added = doc.add_version(version);
        if added {
            doc_repo.save(&doc).await?;
        }
        (doc.id.clone(), false)
    } else {
        let title = original_filename.unwrap_or_else(|| "Imported document".to_string());
        let doc = Document::new(
            uuid::Uuid::new_v4().to_string(),
            source.id.clone(),
            title,
            url.to_string(),
            version,
            serde_json::json!({ "discovery_method": "stdin-import" }),
        );
        let doc_id = doc.id.clone();
        doc_repo.save(&doc).await?;
        (doc_id, true)
    };

    if is_new {
        println!(
            "{} Imported document: {} ({}, {} bytes)",
            style("✓").green(),
            doc_id,
            mime_type,
            content.len()
        );
    } else {
        println!(
            "{} Added version to existing document: {} ({}, {} bytes)",
            style("✓").green(),
            doc_id,
            mime_type,
            content.len()
        );
    }

    Ok(())
}

/// Import documents from Concordance DAT/OPT load files.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_import_concordance(
    settings: &Settings,
    path: &std::path::Path,
    source_id: &str,
    url_prefix: Option<&str>,
    verify: bool,
    tags: &[String],
    limit: usize,
    dry_run: bool,
    resume: bool,
    move_files: bool,
    link_files: bool,
) -> anyhow::Result<()> {
    use foiacquire_import::{
        ConcordanceImportSource, FileStorageMode, ImportRunner, MultiPageMode,
    };

    settings.ensure_directories()?;

    // Determine storage mode
    let storage_mode = if move_files {
        FileStorageMode::Move
    } else if link_files {
        FileStorageMode::HardLink
    } else {
        // Auto-detect: use hard links if on same filesystem
        ImportRunner::detect_storage_mode(path, &settings.documents_dir)
    };

    // Log the storage mode being used
    match storage_mode {
        FileStorageMode::Copy => {
            println!(
                "{} Storage mode: copy (different filesystem or default)",
                style("→").cyan()
            );
        }
        FileStorageMode::Move => {
            println!(
                "{} Storage mode: move (originals will be deleted)",
                style("!").yellow()
            );
        }
        FileStorageMode::HardLink => {
            println!(
                "{} Storage mode: hard link (same filesystem detected)",
                style("→").cyan()
            );
        }
    }

    // Create import source
    let mut source = ConcordanceImportSource::new(
        path.to_path_buf(),
        MultiPageMode::First,
        url_prefix.map(|s| s.to_string()),
        settings.clone(),
    )?;

    // Create config with existing URLs loaded
    let runner = ImportRunner::new(settings);
    let mut config = runner
        .create_config(
            Some(source_id.to_string()),
            limit,
            dry_run,
            resume,
            storage_mode,
        )
        .await?;
    config.verify = verify;
    config.tags = tags.to_vec();

    // Run import
    let stats = runner.run(&mut source, &config).await?;

    if stats.errors > 0 {
        anyhow::bail!("{} error(s) during import", stats.errors);
    }

    Ok(())
}
