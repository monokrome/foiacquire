//! Main document analysis/daemon command.

use std::sync::Arc;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use foiacquire::config::{Config, Settings};
use foiacquire_analysis::ocr::TextExtractor;

use crate::cli::commands::scrape::ReloadMode;

/// Analyze documents: detect MIME types, extract text, and run OCR.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_analyze(
    settings: &Settings,
    source_id: Option<&str>,
    doc_id: Option<&str>,
    method: Option<&str>,
    workers: usize,
    limit: usize,
    mime_type: Option<&str>,
    daemon: bool,
    interval: u64,
    reload: ReloadMode,
) -> anyhow::Result<()> {
    // Parse methods from comma-separated string (e.g., "ocr,whisper")
    let methods: Vec<String> = method
        .map(|m| m.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_else(|| vec!["ocr".to_string()]);
    use foiacquire_analysis::ocr::FallbackOcrBackend;
    use foiacquire_analysis::services::{AnalysisEvent, AnalysisService};
    use tokio::sync::mpsc;

    // Load config early so we can check the right backends
    let config = Config::load().await;

    // Phase 1: Check PDF processing tools (always required)
    let pdf_tools = TextExtractor::check_pdf_tools();
    let missing_pdf: Vec<_> = pdf_tools.iter().filter(|(_, avail)| !avail).collect();

    if !missing_pdf.is_empty() {
        println!("{} Required PDF tools are missing:", style("✗").red());
        for (tool, _) in &missing_pdf {
            println!("  - {}", tool);
        }
        println!();
        println!("Install poppler-utils, then run: foiacquire ocr-check");
        return Err(anyhow::anyhow!(
            "Missing required PDF tools. Run 'foiacquire ocr-check' for install instructions."
        ));
    }

    // Phase 2: Check that at least one configured OCR backend is available
    let configured_names: Vec<&str> = config
        .analysis
        .ocr
        .backends
        .iter()
        .flat_map(|entry| entry.backends())
        .collect();

    let any_backend_available = configured_names
        .iter()
        .any(|name| FallbackOcrBackend::check_backend_available(name));

    if !any_backend_available {
        println!(
            "{} No configured OCR backends are available:",
            style("✗").red()
        );
        for name in &configured_names {
            println!("  - {} (not available)", name);
        }
        println!();
        println!("Run 'foiacquire ocr-check' for setup instructions.");
        return Err(anyhow::anyhow!(
            "No configured OCR backends available. Run 'foiacquire ocr-check' for details."
        ));
    }

    // Set up config watcher for stop-process and inplace modes
    // Try file watching first, fall back to DB polling if no config file
    let mut config_watcher =
        if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            prefer::watch("foiacquire").await.ok()
        } else {
            None
        };

    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();
    let config_history = ctx.config_history();
    let mut current_config_hash = config.hash();

    let service = AnalysisService::with_ocr_config(doc_repo, config.analysis.ocr.clone());

    // If specific doc_id provided, process just that document (no daemon mode)
    if let Some(id) = doc_id {
        println!("{} Processing single document: {}", style("→").cyan(), id);
        let (event_tx, _event_rx) = mpsc::channel::<AnalysisEvent>(100);
        return service.process_single(id, event_tx).await;
    }

    if daemon {
        println!(
            "{} Running in daemon mode (interval: {}s, reload: {:?})",
            style("→").cyan(),
            interval,
            reload
        );
    }

    loop {
        // Check if there's work to do
        let (docs_count, pages_count) = service
            .count_needing_processing(source_id, mime_type)
            .await?;
        if docs_count == 0 && pages_count == 0 {
            if daemon {
                println!(
                    "{} No documents need OCR processing, sleeping for {}s...",
                    style("→").dim(),
                    interval
                );
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                continue;
            } else {
                println!("{} No documents need OCR processing", style("!").yellow());
                return Ok(());
            }
        }

        // Create event channel for progress tracking
        let (event_tx, mut event_rx) = mpsc::channel::<AnalysisEvent>(100);

        // State for progress bar
        let pb = Arc::new(tokio::sync::Mutex::new(None::<ProgressBar>));
        let pb_clone = pb.clone();

        // Spawn event handler for UI
        let event_handler = tokio::spawn(async move {
            let mut phase1_succeeded = 0;
            let mut phase1_failed = 0;
            let mut phase1_pages = 0;
            let mut phase2_improved = 0;
            let mut phase2_skipped = 0;
            let mut phase2_failed = 0;
            let mut docs_finalized_incremental = 0;

            while let Some(event) = event_rx.recv().await {
                match event {
                    AnalysisEvent::Phase1Started { total_documents } => {
                        println!(
                            "{} Phase 1: Extracting text from {} documents",
                            style("→").cyan(),
                            total_documents
                        );
                        let progress = ProgressBar::new(total_documents as u64);
                        progress.set_style(
                            ProgressStyle::default_bar()
                                .template(
                                    "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                                )
                                .unwrap()
                                .progress_chars("█▓░"),
                        );
                        progress.set_message("Extracting text...");
                        *pb_clone.lock().await = Some(progress);
                    }
                    AnalysisEvent::DocumentCompleted {
                        pages_extracted, ..
                    } => {
                        phase1_succeeded += 1;
                        phase1_pages += pages_extracted;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.inc(1);
                        }
                    }
                    AnalysisEvent::DocumentFailed { document_id, error } => {
                        phase1_failed += 1;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.suspend(|| {
                                eprintln!(
                                    "  {} Document {} failed: {}",
                                    style("✗").red(),
                                    document_id,
                                    error
                                );
                            });
                            progress.inc(1);
                        } else {
                            eprintln!(
                                "  {} Document {} failed: {}",
                                style("✗").red(),
                                document_id,
                                error
                            );
                        }
                    }
                    AnalysisEvent::Phase1Complete {
                        skipped_missing, ..
                    } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.finish_and_clear();
                        }
                        *pb_clone.lock().await = None;
                        println!(
                            "{} Phase 1 complete: {} documents processed, {} pages extracted",
                            style("✓").green(),
                            phase1_succeeded,
                            phase1_pages
                        );
                        if phase1_failed > 0 {
                            println!(
                                "  {} {} documents failed",
                                style("!").yellow(),
                                phase1_failed
                            );
                        }
                        if skipped_missing > 0 {
                            println!(
                                "  {} Skipped {} documents with missing files (will retry when they appear)",
                                style("!").yellow(),
                                skipped_missing
                            );
                        }
                    }
                    AnalysisEvent::Phase2Started { total_pages } => {
                        println!(
                            "{} Phase 2: Running OCR on {} pages",
                            style("→").cyan(),
                            total_pages
                        );
                        let progress = ProgressBar::new(total_pages as u64);
                        progress.set_style(
                            ProgressStyle::default_bar()
                                .template(
                                    "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                                )
                                .unwrap()
                                .progress_chars("█▓░"),
                        );
                        progress.set_message("Running OCR...");
                        *pb_clone.lock().await = Some(progress);
                    }
                    AnalysisEvent::PageOcrCompleted { improved, .. } => {
                        if improved {
                            phase2_improved += 1;
                        } else {
                            phase2_skipped += 1;
                        }
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.inc(1);
                        }
                    }
                    AnalysisEvent::PageOcrFailed {
                        document_id,
                        page_number,
                        error,
                    } => {
                        phase2_failed += 1;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.suspend(|| {
                                eprintln!(
                                    "  {} Page {} of {} failed: {}",
                                    style("✗").red(),
                                    page_number,
                                    document_id,
                                    error
                                );
                            });
                            progress.inc(1);
                        } else {
                            eprintln!(
                                "  {} Page {} of {} failed: {}",
                                style("✗").red(),
                                page_number,
                                document_id,
                                error
                            );
                        }
                    }
                    AnalysisEvent::DocumentFinalized { .. } => {
                        docs_finalized_incremental += 1;
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.set_message(format!(
                                "{} docs complete",
                                docs_finalized_incremental
                            ));
                        }
                    }
                    AnalysisEvent::Phase2Complete { .. } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.finish_and_clear();
                        }
                        *pb_clone.lock().await = None;
                        let mut msg = format!(
                            "{} Phase 2 complete: {} pages improved by OCR, {} kept PDF text",
                            style("✓").green(),
                            phase2_improved,
                            phase2_skipped
                        );
                        if phase2_failed > 0 {
                            msg.push_str(&format!(", {} failed", phase2_failed));
                        }
                        if docs_finalized_incremental > 0 {
                            msg.push_str(&format!(
                                ", {} documents finalized",
                                docs_finalized_incremental
                            ));
                        }
                        println!("{}", msg);
                    }
                    _ => {}
                }
            }
        });

        // Run service
        let _result = service
            .process(source_id, &methods, workers, limit, mime_type, event_tx)
            .await?;

        // Wait for event handler to finish
        if let Err(e) = event_handler.await {
            tracing::warn!("Event handler task failed: {}", e);
        }

        if !daemon {
            break;
        }

        // Sleep with config watching for stop-process and inplace modes
        println!(
            "{} Sleeping for {}s before next check...",
            style("→").dim(),
            interval
        );

        if let Some(ref mut watcher) = config_watcher {
            // File-based config watching
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(interval)) => {}
                result = watcher.recv() => {
                    if result.is_some() {
                        match reload {
                            ReloadMode::StopProcess => {
                                println!(
                                    "{} Config file changed, exiting for restart...",
                                    style("↻").cyan()
                                );
                                return Ok(());
                            }
                            ReloadMode::Inplace => {
                                println!(
                                    "{} Config file changed, continuing...",
                                    style("↻").cyan()
                                );
                                // OCR doesn't use config, so just continue
                            }
                            ReloadMode::NextRun => {}
                        }
                    }
                }
            }
        } else if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            // DB-based config polling (no config file available)
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

            // Check if config changed in DB
            if let Ok(Some(latest_hash)) = config_history.get_latest_hash().await {
                if latest_hash != current_config_hash {
                    match reload {
                        ReloadMode::StopProcess => {
                            println!(
                                "{} Config changed in database, exiting for restart...",
                                style("↻").cyan()
                            );
                            return Ok(());
                        }
                        ReloadMode::Inplace => {
                            println!(
                                "{} Config changed in database, continuing...",
                                style("↻").cyan()
                            );
                            current_config_hash = latest_hash;
                        }
                        ReloadMode::NextRun => {}
                    }
                }
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        }
    }

    Ok(())
}
