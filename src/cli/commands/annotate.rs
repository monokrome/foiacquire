//! Annotation and date detection commands.

use std::sync::Arc;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use crate::config::{Config, Settings};

use super::helpers::truncate;
use super::scrape::ReloadMode;

/// Annotate documents using LLM.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_annotate(
    settings: &Settings,
    source_id: Option<&str>,
    doc_id: Option<&str>,
    limit: usize,
    endpoint: Option<String>,
    model: Option<String>,
    daemon: bool,
    interval: u64,
    reload: ReloadMode,
) -> anyhow::Result<()> {
    use crate::services::{AnnotationEvent, AnnotationService};
    use tokio::sync::mpsc;

    let ctx = settings.create_db_context();
    let doc_repo = ctx.documents();

    // Set up config watcher for stop-process and inplace modes
    // Try file watching first, fall back to DB polling if no config file
    let mut config_watcher =
        if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            prefer::watch("foiacquire").await.ok()
        } else {
            None
        };

    // Initial config load
    let config = Config::load().await;
    let mut current_config_hash = config.hash();
    let mut llm_config = config.llm.clone();
    if let Some(ref ep) = endpoint {
        llm_config.endpoint = ep.clone();
    }
    if let Some(ref m) = model {
        llm_config.model = m.clone();
    }

    if !llm_config.enabled {
        println!(
            "{} LLM annotation is disabled in configuration",
            style("!").yellow()
        );
        println!("  Set llm.enabled = true in your foiacquire.json config");
        return Ok(());
    }

    // Create initial service
    let mut service = AnnotationService::new(doc_repo.clone(), llm_config.clone());
    let config_history = ctx.config_history();

    // Check if LLM service is available
    if !service.is_available().await {
        println!(
            "{} LLM service not available at {}",
            style("✗").red(),
            llm_config.endpoint
        );
        println!("  Make sure Ollama is running: ollama serve");
        return Ok(());
    }

    println!(
        "{} Connected to LLM at {} (model: {})",
        style("✓").green(),
        llm_config.endpoint,
        llm_config.model
    );

    // If specific doc_id provided, process just that document (no daemon mode)
    if let Some(id) = doc_id {
        println!("{} Processing single document: {}", style("→").cyan(), id);
        let (event_tx, _event_rx) = mpsc::channel::<AnnotationEvent>(100);
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
        // For next-run and inplace modes, reload config at start of each cycle
        if daemon && matches!(reload, ReloadMode::NextRun | ReloadMode::Inplace) {
            let fresh_config = Config::load().await;
            let mut new_llm_config = fresh_config.llm.clone();
            if let Some(ref ep) = endpoint {
                new_llm_config.endpoint = ep.clone();
            }
            if let Some(ref m) = model {
                new_llm_config.model = m.clone();
            }

            if new_llm_config.endpoint != llm_config.endpoint
                || new_llm_config.model != llm_config.model
                || new_llm_config.enabled != llm_config.enabled
            {
                println!(
                    "{} Config reloaded (model: {})",
                    style("↻").cyan(),
                    new_llm_config.model
                );
                llm_config = new_llm_config;
                current_config_hash = fresh_config.hash();
                service = AnnotationService::new(doc_repo.clone(), llm_config.clone());
            }
        }

        // Check if there's work to do
        let total_count = service.count_needing_annotation(source_id).await?;

        if total_count == 0 {
            if daemon {
                println!(
                    "{} No documents need annotation, sleeping for {}s...",
                    style("→").dim(),
                    interval
                );
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                continue;
            } else {
                println!("{} No documents need annotation", style("!").yellow());
                println!(
                    "  Documents need OCR complete status with extracted text to be annotated"
                );
                return Ok(());
            }
        }

        let effective_limit = if limit > 0 {
            limit
        } else {
            total_count as usize
        };

        println!(
            "{} Annotating up to {} documents (running sequentially to manage memory)",
            style("→").cyan(),
            effective_limit
        );

        // Create event channel for progress tracking
        let (event_tx, mut event_rx) = mpsc::channel::<AnnotationEvent>(100);

        // State for progress bar
        let pb = Arc::new(tokio::sync::Mutex::new(None::<ProgressBar>));
        let pb_clone = pb.clone();

        // Spawn event handler for UI
        let event_handler = tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    AnnotationEvent::Started { total_documents } => {
                        let progress = ProgressBar::new(total_documents as u64);
                        progress.set_style(
                            ProgressStyle::default_bar()
                                .template(
                                    "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}",
                                )
                                .unwrap()
                                .progress_chars("█▓░"),
                        );
                        progress.set_message("Annotating...");
                        *pb_clone.lock().await = Some(progress);
                    }
                    AnnotationEvent::DocumentStarted { title, .. } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.set_message(truncate(&title, 40));
                        }
                    }
                    AnnotationEvent::DocumentCompleted { .. }
                    | AnnotationEvent::DocumentSkipped { .. } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.inc(1);
                        }
                    }
                    AnnotationEvent::DocumentFailed { error, .. } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.println(format!("{} {}", style("✗").red(), error));
                            progress.inc(1);
                        }
                    }
                    AnnotationEvent::Complete {
                        succeeded,
                        failed,
                        remaining,
                    } => {
                        if let Some(ref progress) = *pb_clone.lock().await {
                            progress.finish_and_clear();
                        }
                        *pb_clone.lock().await = None;

                        println!(
                            "{} Annotation complete: {} succeeded, {} failed",
                            style("✓").green(),
                            succeeded,
                            failed
                        );

                        if remaining > 0 {
                            println!(
                                "  {} {} documents still need annotation",
                                style("→").dim(),
                                remaining
                            );
                        }
                    }
                }
            }
        });

        // Run service
        let _result = service.annotate(source_id, limit, event_tx).await?;

        // Wait for event handler to finish
        let _ = event_handler.await;

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
                                    "{} Config file changed, reloading...",
                                    style("↻").cyan()
                                );
                                // Config will be reloaded at start of next iteration
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
                                "{} Config changed in database, reloading...",
                                style("↻").cyan()
                            );
                            current_config_hash = latest_hash;
                            // Config will be reloaded at start of next iteration
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

/// Detect and estimate publication dates for documents.
pub async fn cmd_detect_dates(
    settings: &Settings,
    source_id: Option<&str>,
    limit: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    use crate::services::date_detection::{detect_date, DateConfidence};

    let ctx = settings.create_db_context();
    let doc_repo = ctx.documents();

    // Count documents needing date estimation
    let total_count = doc_repo
        .count_documents_needing_date_estimation(source_id)
        .await?;

    if total_count == 0 {
        println!("{} No documents need date estimation", style("!").yellow());
        println!("  All documents already have estimated_date or manual_date set");
        return Ok(());
    }

    let effective_limit = if limit > 0 {
        limit
    } else {
        total_count as usize
    };

    if dry_run {
        println!(
            "{} Dry run - showing what would be detected for up to {} documents",
            style("→").cyan(),
            effective_limit
        );
    } else {
        println!(
            "{} Detecting dates for up to {} documents",
            style("→").cyan(),
            effective_limit
        );
    }

    // Fetch documents needing estimation
    let documents = doc_repo
        .get_documents_needing_date_estimation(source_id, effective_limit)
        .await?;

    let pb = ProgressBar::new(documents.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {wide_msg}")
            .unwrap()
            .progress_chars("█▓░"),
    );
    pb.set_message("Analyzing...");

    let mut detected = 0u64;
    let mut no_date = 0u64;

    for doc in documents {
        pb.set_message(truncate(&doc.id, 36));

        // Extract date detection inputs from document
        let version = doc.current_version();
        let filename = version.and_then(|v| v.original_filename.clone());
        let server_date = version.and_then(|v| v.server_date);
        let acquired_at = version.map(|v| v.acquired_at).unwrap_or(doc.created_at);
        let source_url = Some(doc.source_url.clone());

        // Run date detection
        let estimate = detect_date(
            server_date,
            acquired_at,
            filename.as_deref(),
            source_url.as_deref(),
        );
        let doc_id = &doc.id;

        if let Some(est) = estimate {
            detected += 1;

            if dry_run {
                let confidence_str = match est.confidence {
                    DateConfidence::High => style("high").green(),
                    DateConfidence::Medium => style("medium").yellow(),
                    DateConfidence::Low => style("low").red(),
                };
                pb.println(format!(
                    "  {} {} → {} ({}, {})",
                    style("✓").green(),
                    &doc_id[..8],
                    est.date.format("%Y-%m-%d"),
                    confidence_str,
                    est.source.as_str()
                ));
            } else {
                // Update database with detected date
                doc_repo
                    .update_estimated_date(
                        doc_id,
                        est.date,
                        est.confidence.as_str(),
                        est.source.as_str(),
                    )
                    .await?;
                // Record that we processed this document
                doc_repo
                    .record_annotation(
                        doc_id,
                        "date_detection",
                        1,
                        Some(&format!("detected:{}", est.source.as_str())),
                        None,
                    )
                    .await?;
            }
        } else {
            no_date += 1;
            if !dry_run {
                // Record that we tried but found no date
                doc_repo
                    .record_annotation(doc_id, "date_detection", 1, Some("no_date"), None)
                    .await?;
            }
        }

        pb.inc(1);
    }

    pb.finish_and_clear();

    println!(
        "{} Date detection complete: {} detected, {} no date found",
        style("✓").green(),
        detected,
        no_date
    );

    if dry_run && detected > 0 {
        println!(
            "  {} Run without --dry-run to update database",
            style("→").dim()
        );
    }

    // Use saturating subtraction to avoid underflow
    // (can happen if count query and get query have slightly different criteria)
    let processed = detected + no_date;
    if processed < total_count {
        let remaining = total_count - processed;
        println!(
            "  {} {} documents still need date estimation",
            style("→").dim(),
            remaining
        );
    }

    Ok(())
}
