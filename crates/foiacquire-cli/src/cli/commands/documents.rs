//! Document management commands.

use std::path::Path;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use foiacquire::config::Settings;
use foiacquire::models::Document;
use foiacquire::repository::DieselDocumentRepository;

use super::helpers::{format_bytes, mime_short, truncate};

/// Statistics from processing containers.
struct ProcessingStats {
    containers_processed: usize,
    files_discovered: usize,
    files_extracted: usize,
}

impl ProcessingStats {
    fn new() -> Self {
        Self {
            containers_processed: 0,
            files_discovered: 0,
            files_extracted: 0,
        }
    }
}

/// Extract and optionally OCR a virtual file from an archive or email.
fn extract_and_ocr_from_archive(
    file_path: &Path,
    entry_path: &str,
    entry_mime: &str,
    run_ocr: bool,
    text_extractor: &foiacquire_analysis::ocr::TextExtractor,
) -> (Option<String>, foiacquire::models::VirtualFileStatus) {
    use foiacquire::models::VirtualFileStatus;
    use foiacquire_analysis::ocr::ArchiveExtractor;

    if !run_ocr {
        return (None, VirtualFileStatus::Pending);
    }

    match ArchiveExtractor::extract_file(file_path, entry_path) {
        Ok(extracted) => match text_extractor.extract(&extracted.file_path, entry_mime) {
            Ok(result) => (Some(result.text), VirtualFileStatus::OcrComplete),
            Err(e) => {
                tracing::debug!("OCR failed for {}: {}", entry_path, e);
                (None, VirtualFileStatus::Failed)
            }
        },
        Err(e) => {
            tracing::debug!("Failed to extract {}: {}", entry_path, e);
            (None, VirtualFileStatus::Failed)
        }
    }
}

/// Extract and optionally OCR an email attachment.
fn extract_and_ocr_from_email(
    file_path: &Path,
    attachment_name: &str,
    attachment_mime: &str,
    run_ocr: bool,
    text_extractor: &foiacquire_analysis::ocr::TextExtractor,
) -> (Option<String>, foiacquire::models::VirtualFileStatus) {
    use foiacquire::models::VirtualFileStatus;
    use foiacquire_analysis::ocr::EmailExtractor;

    if !run_ocr {
        return (None, VirtualFileStatus::Pending);
    }

    match EmailExtractor::extract_attachment(file_path, attachment_name) {
        Ok(extracted) => match text_extractor.extract(&extracted.file_path, attachment_mime) {
            Ok(result) => (Some(result.text), VirtualFileStatus::OcrComplete),
            Err(e) => {
                tracing::debug!("OCR failed for {}: {}", attachment_name, e);
                (None, VirtualFileStatus::Failed)
            }
        },
        Err(e) => {
            tracing::debug!("Failed to extract {}: {}", attachment_name, e);
            (None, VirtualFileStatus::Failed)
        }
    }
}

/// Process a single archive document.
async fn process_archive(
    doc: &Document,
    doc_repo: &DieselDocumentRepository,
    run_ocr: bool,
    text_extractor: &foiacquire_analysis::ocr::TextExtractor,
    documents_dir: &Path,
) -> Option<(usize, usize)> {
    use foiacquire::models::{VirtualFile, VirtualFileStatus};
    use foiacquire_analysis::ocr::ArchiveExtractor;

    let version = doc.current_version()?;
    let version_id = doc_repo.get_current_version_id(&doc.id).await.ok()??;
    let file_path = version.resolve_path(documents_dir, &doc.source_url, &doc.title);

    let entries = match ArchiveExtractor::list_zip_contents(&file_path) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("Failed to read archive {}: {}", doc.title, e);
            return None;
        }
    };

    let files_discovered = entries.len();
    let mut files_extracted = 0;

    for entry in entries {
        let (text, status) = if entry.is_extractable() {
            let result = extract_and_ocr_from_archive(
                &file_path,
                &entry.path,
                &entry.mime_type,
                run_ocr,
                text_extractor,
            );
            if result.0.is_some() {
                files_extracted += 1;
            }
            result
        } else {
            (None, VirtualFileStatus::Unsupported)
        };

        let mut vf = VirtualFile::new(
            doc.id.clone(),
            version_id,
            entry.path.clone(),
            entry.filename.clone(),
            entry.mime_type.clone(),
            entry.size,
        );
        vf.extracted_text = text;
        vf.status = status;

        if let Err(e) = doc_repo.insert_virtual_file(&vf).await {
            tracing::warn!("Failed to save virtual file {}: {}", entry.path, e);
        }
    }

    Some((files_discovered, files_extracted))
}

/// Process a single email document.
async fn process_email(
    doc: &Document,
    doc_repo: &DieselDocumentRepository,
    run_ocr: bool,
    text_extractor: &foiacquire_analysis::ocr::TextExtractor,
    documents_dir: &Path,
) -> Option<(usize, usize)> {
    use foiacquire::models::{VirtualFile, VirtualFileStatus};
    use foiacquire_analysis::ocr::EmailExtractor;

    let version = doc.current_version()?;
    let version_id = doc_repo.get_current_version_id(&doc.id).await.ok()??;
    let file_path = version.resolve_path(documents_dir, &doc.source_url, &doc.title);

    let parsed = match EmailExtractor::parse_email(&file_path) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("Failed to parse email {}: {}", doc.title, e);
            return None;
        }
    };

    let files_discovered = parsed.attachments.len();
    let mut files_extracted = 0;

    for attachment in &parsed.attachments {
        let (text, status) = if attachment.is_extractable() {
            let result = extract_and_ocr_from_email(
                &file_path,
                &attachment.filename,
                &attachment.mime_type,
                run_ocr,
                text_extractor,
            );
            if result.0.is_some() {
                files_extracted += 1;
            }
            result
        } else {
            (None, VirtualFileStatus::Unsupported)
        };

        let mut vf = VirtualFile::new(
            doc.id.clone(),
            version_id,
            attachment.filename.clone(),
            attachment.filename.clone(),
            attachment.mime_type.clone(),
            attachment.size,
        );
        vf.extracted_text = text;
        vf.status = status;

        if let Err(e) = doc_repo.insert_virtual_file(&vf).await {
            tracing::warn!("Failed to save virtual file {}: {}", attachment.filename, e);
        }
    }

    // Mark emails with no attachments as processed
    if parsed.attachments.is_empty() {
        let placeholder = VirtualFile::new(
            doc.id.clone(),
            version_id,
            "_email_body".to_string(),
            "_email_body".to_string(),
            "text/plain".to_string(),
            parsed
                .body_text
                .as_ref()
                .map(|s| s.len() as u64)
                .unwrap_or(0),
        );
        let _ = doc_repo.insert_virtual_file(&placeholder).await;
    }

    Some((files_discovered, files_extracted))
}

/// Process archive/email containers.
pub async fn cmd_archive(
    settings: &Settings,
    source_id: Option<&str>,
    limit: usize,
    run_ocr: bool,
) -> anyhow::Result<()> {
    use foiacquire_analysis::ocr::TextExtractor;

    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();

    let archive_count = doc_repo.count_unprocessed_archives(source_id).await?;
    let email_count = doc_repo.count_unprocessed_emails(source_id).await?;
    let total_count = archive_count + email_count;

    if total_count == 0 {
        println!("{} No containers need processing", style("!").yellow());
        return Ok(());
    }

    let effective_limit = if limit > 0 {
        limit.min(total_count as usize)
    } else {
        total_count as usize
    };

    println!(
        "{} Processing up to {} containers ({} archives, {} emails)",
        style("→").cyan(),
        effective_limit,
        archive_count,
        email_count
    );

    let pb = ProgressBar::new(effective_limit as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}")
            .unwrap()
            .progress_chars("█▓░"),
    );

    let mut stats = ProcessingStats::new();
    let text_extractor = TextExtractor::new();

    // Process zip archives first
    let archive_limit = effective_limit.min(archive_count as usize);
    if archive_limit > 0 {
        for doc in doc_repo
            .get_unprocessed_archives(source_id, archive_limit)
            .await?
        {
            pb.set_message(truncate(&doc.title, 40));
            if let Some((discovered, extracted)) = process_archive(
                &doc,
                &doc_repo,
                run_ocr,
                &text_extractor,
                &settings.documents_dir,
            )
            .await
            {
                stats.files_discovered += discovered;
                stats.files_extracted += extracted;
                stats.containers_processed += 1;
            }
            pb.inc(1);
        }
    }

    // Process emails with remaining limit
    let remaining_limit = effective_limit.saturating_sub(stats.containers_processed);
    if remaining_limit > 0 && email_count > 0 {
        for doc in doc_repo
            .get_unprocessed_emails(source_id, remaining_limit)
            .await?
        {
            pb.set_message(truncate(&doc.title, 40));
            if let Some((discovered, extracted)) = process_email(
                &doc,
                &doc_repo,
                run_ocr,
                &text_extractor,
                &settings.documents_dir,
            )
            .await
            {
                stats.files_discovered += discovered;
                stats.files_extracted += extracted;
                stats.containers_processed += 1;
            }
            pb.inc(1);
        }
    }

    pb.finish_and_clear();

    println!("{} Container processing complete:", style("✓").green());
    println!("  {} containers processed", stats.containers_processed);
    println!("  {} files discovered", stats.files_discovered);
    if run_ocr {
        println!("  {} files extracted and OCR'd", stats.files_extracted);
    }

    Ok(())
}

/// List documents in the repository.
pub async fn cmd_ls(
    settings: &Settings,
    source_id: Option<&str>,
    tag: Option<&str>,
    type_filter: Option<&str>,
    limit: usize,
    format: &str,
) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();

    // Get documents based on filters
    let documents: Vec<Document> = if let Some(tag_name) = tag {
        // Filter by tag
        doc_repo.get_by_tag(tag_name, source_id).await?
    } else if let Some(type_name) = type_filter {
        // Filter by type
        doc_repo
            .get_by_type_category(type_name, source_id, limit)
            .await?
    } else if let Some(sid) = source_id {
        // Filter by source
        doc_repo.get_by_source(sid).await?
    } else {
        // Get all
        doc_repo.get_all().await?
    };

    // Apply limit
    let documents: Vec<_> = documents.into_iter().take(limit).collect();

    if documents.is_empty() {
        println!("{} No documents found", style("!").yellow());
        return Ok(());
    }

    match format {
        "json" => {
            // JSON output
            let output: Vec<_> = documents
                .iter()
                .map(|doc| {
                    let version = doc.current_version();
                    serde_json::json!({
                        "id": doc.id,
                        "title": doc.title,
                        "source_id": doc.source_id,
                        "source_url": doc.source_url,
                        "synopsis": doc.synopsis,
                        "tags": doc.tags,
                        "status": doc.status.as_str(),
                        "mime_type": version.map(|v| v.mime_type.as_str()),
                        "file_size": version.map(|v| v.file_size),
                        "file_path": version.and_then(|v| v.file_path.as_ref().map(|p| p.to_string_lossy().to_string())),
                        "created_at": doc.created_at.to_rfc3339(),
                        "updated_at": doc.updated_at.to_rfc3339(),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        "ids" => {
            // Just IDs (for piping)
            for doc in &documents {
                println!("{}", doc.id);
            }
        }
        _ => {
            // Table format (default)
            println!(
                "\n{:<36}  {:<30}  {:<10}  {:<10}  Status",
                "ID", "Title", "Type", "Size"
            );
            println!("{}", "-".repeat(100));

            for doc in &documents {
                let version = doc.current_version();
                let mime = version.map(|v| mime_short(&v.mime_type)).unwrap_or("???");
                let size = version
                    .map(|v| format_bytes(v.file_size))
                    .unwrap_or_else(|| "-".to_string());
                let status = doc.status.as_str();

                println!(
                    "{:<36}  {:<30}  {:<10}  {:<10}  {}",
                    &doc.id[..36.min(doc.id.len())],
                    truncate(&doc.title, 30),
                    mime,
                    size,
                    status
                );
            }

            println!("\n{} documents", documents.len());
        }
    }

    Ok(())
}

/// Show document info/metadata.
pub async fn cmd_info(settings: &Settings, doc_id: &str) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();

    // Try to find document by ID
    let doc = match doc_repo.get(doc_id).await? {
        Some(d) => d,
        None => {
            // Try to find by partial ID or title search
            let all_docs = doc_repo.get_all().await?;
            let matches: Vec<_> = all_docs
                .into_iter()
                .filter(|d| {
                    d.id.starts_with(doc_id)
                        || d.title.to_lowercase().contains(&doc_id.to_lowercase())
                })
                .collect();

            match matches.len() {
                0 => {
                    println!("{} Document not found: {}", style("✗").red(), doc_id);
                    return Ok(());
                }
                1 => matches.into_iter().next().unwrap(),
                _ => {
                    println!("{} Multiple matches found:", style("!").yellow());
                    for d in &matches {
                        println!("  {} - {}", &d.id[..8], truncate(&d.title, 50));
                    }
                    return Ok(());
                }
            }
        }
    };

    // Display document info
    println!("\n{}", style("Document Info").bold());
    println!("{}", "=".repeat(60));
    println!("{:<18} {}", "ID:", doc.id);
    println!("{:<18} {}", "Title:", doc.title);
    println!("{:<18} {}", "Source:", doc.source_id);
    println!("{:<18} {}", "URL:", doc.source_url);
    println!("{:<18} {}", "Status:", doc.status.as_str());
    println!(
        "{:<18} {}",
        "Created:",
        doc.created_at.format("%Y-%m-%d %H:%M:%S")
    );
    println!(
        "{:<18} {}",
        "Updated:",
        doc.updated_at.format("%Y-%m-%d %H:%M:%S")
    );

    if let Some(synopsis) = &doc.synopsis {
        println!("\n{}", style("Synopsis").bold());
        println!("{}", "-".repeat(60));
        println!("{}", synopsis);
    }

    if !doc.tags.is_empty() {
        println!("\n{:<18} {}", "Tags:", doc.tags.join(", "));
    }

    if let Some(version) = doc.current_version() {
        println!("\n{}", style("Current Version").bold());
        println!("{}", "-".repeat(60));
        let resolved_path =
            version.resolve_path(&settings.documents_dir, &doc.source_url, &doc.title);
        println!("{:<18} {}", "File:", resolved_path.display());
        println!("{:<18} {}", "MIME Type:", version.mime_type);
        println!("{:<18} {}", "Size:", format_bytes(version.file_size));
        println!("{:<18} {}", "Content Hash:", &version.content_hash[..16]);
        println!(
            "{:<18} {}",
            "Acquired:",
            version.acquired_at.format("%Y-%m-%d %H:%M:%S")
        );
        if let Some(filename) = &version.original_filename {
            println!("{:<18} {}", "Original Name:", filename);
        }
        if let Some(date) = &version.server_date {
            println!(
                "{:<18} {}",
                "Server Date:",
                date.format("%Y-%m-%d %H:%M:%S")
            );
        }
    }

    if doc.versions.len() > 1 {
        println!(
            "\n{} ({} versions total)",
            style("Version History").bold(),
            doc.versions.len()
        );
        println!("{}", "-".repeat(60));
        for (i, version) in doc.versions.iter().enumerate() {
            println!(
                "  {}. {} - {} ({})",
                i + 1,
                version.acquired_at.format("%Y-%m-%d"),
                &version.content_hash[..8],
                format_bytes(version.file_size)
            );
        }
    }

    if doc.extracted_text.is_some() {
        let text_len = doc.extracted_text.as_ref().map(|t| t.len()).unwrap_or(0);
        println!("\n{:<18} {} chars", "Extracted Text:", text_len);
    }

    Ok(())
}

/// Output document content to stdout.
pub async fn cmd_read(settings: &Settings, doc_id: &str, text_only: bool) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();

    // Find document
    let doc = match doc_repo.get(doc_id).await? {
        Some(d) => d,
        None => {
            // Try partial match
            let all_docs = doc_repo.get_all().await?;
            let matches: Vec<_> = all_docs
                .into_iter()
                .filter(|d| d.id.starts_with(doc_id))
                .collect();

            match matches.len() {
                0 => {
                    eprintln!("Document not found: {}", doc_id);
                    std::process::exit(1);
                }
                1 => matches.into_iter().next().unwrap(),
                _ => {
                    eprintln!("Multiple matches found for '{}', be more specific:", doc_id);
                    for d in &matches {
                        eprintln!("  {} - {}", &d.id[..8], truncate(&d.title, 50));
                    }
                    std::process::exit(1);
                }
            }
        }
    };

    if text_only {
        // Output extracted text
        match &doc.extracted_text {
            Some(text) => {
                print!("{}", text);
            }
            None => {
                eprintln!("No extracted text available for this document");
                eprintln!("Run 'foiacquire ocr' to extract text from documents");
                std::process::exit(1);
            }
        }
    } else {
        // Output binary file content
        let version = doc
            .current_version()
            .ok_or_else(|| anyhow::anyhow!("Document has no file version"))?;

        let resolved = version.resolve_path(&settings.documents_dir, &doc.source_url, &doc.title);
        let content = std::fs::read(&resolved)?;

        use std::io::Write;
        std::io::stdout().write_all(&content)?;
    }

    Ok(())
}

/// Search documents by content or metadata.
pub async fn cmd_search(
    settings: &Settings,
    query: &str,
    source_id: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();

    let query_lower = query.to_lowercase();

    // Get all documents and filter
    let documents: Vec<Document> = if let Some(sid) = source_id {
        doc_repo.get_by_source(sid).await?
    } else {
        doc_repo.get_all().await?
    };

    // Search in title, synopsis, tags, and extracted text
    let matches: Vec<_> = documents
        .into_iter()
        .filter(|doc| {
            // Check title
            if doc.title.to_lowercase().contains(&query_lower) {
                return true;
            }
            // Check synopsis
            if let Some(synopsis) = &doc.synopsis {
                if synopsis.to_lowercase().contains(&query_lower) {
                    return true;
                }
            }
            // Check tags
            if doc
                .tags
                .iter()
                .any(|t| t.to_lowercase().contains(&query_lower))
            {
                return true;
            }
            // Check extracted text
            if let Some(text) = &doc.extracted_text {
                if text.to_lowercase().contains(&query_lower) {
                    return true;
                }
            }
            false
        })
        .take(limit)
        .collect();

    if matches.is_empty() {
        println!(
            "{} No documents found matching '{}'",
            style("!").yellow(),
            query
        );
        return Ok(());
    }

    println!("\n{} results for '{}'\n", matches.len(), query);

    for doc in &matches {
        let version = doc.current_version();
        let mime = version.map(|v| mime_short(&v.mime_type)).unwrap_or("???");

        println!(
            "{} {} [{}]",
            style(&doc.id[..8]).cyan(),
            style(&doc.title).bold(),
            mime
        );

        // Show context of match
        if let Some(synopsis) = &doc.synopsis {
            if synopsis.to_lowercase().contains(&query_lower) {
                println!("  Synopsis: {}", truncate(synopsis, 80));
            }
        }

        if !doc.tags.is_empty() {
            let matching_tags: Vec<_> = doc
                .tags
                .iter()
                .filter(|t| t.to_lowercase().contains(&query_lower))
                .collect();
            if !matching_tags.is_empty() {
                println!(
                    "  Tags: {}",
                    matching_tags
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }

        // Show snippet from extracted text if match found there
        if let Some(text) = &doc.extracted_text {
            if let Some(pos) = text.to_lowercase().find(&query_lower) {
                let start = pos.saturating_sub(40);
                let end = (pos + query.len() + 40).min(text.len());
                let snippet: String = text[start..end].chars().collect();
                let snippet = snippet.replace('\n', " ");
                println!("  ...{}...", truncate(&snippet, 80));
            }
        }

        println!();
    }

    Ok(())
}
