//! Annotation and date detection commands.

use std::sync::Arc;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::mpsc;

use foiacquire::config::{Config, Settings};
use foiacquire_annotate::services::annotation::{
    AnnotationEvent, AnnotationManager, Annotator, DateAnnotator, LlmAnnotator, NerAnnotator,
};

use super::daemon::{ConfigWatcher, DaemonAction, ReloadMode};
use super::helpers::truncate;

/// Spawn a task that drives a progress bar from annotation events.
///
/// Returns a `JoinHandle` the caller should `.await` after the batch completes.
fn spawn_progress_handler(
    mut event_rx: mpsc::Receiver<AnnotationEvent>,
    action_label: &str,
) -> tokio::task::JoinHandle<()> {
    let label = action_label.to_string();
    let pb = Arc::new(tokio::sync::Mutex::new(None::<ProgressBar>));
    let pb_clone = pb.clone();

    tokio::spawn(async move {
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
                    progress.set_message(format!("{}...", label));
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
                AnnotationEvent::DocumentFailed { document_id, error } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.println(format!(
                            "{} Document {} failed: {}",
                            style("✗").red(),
                            &document_id[..8.min(document_id.len())],
                            error
                        ));
                        progress.inc(1);
                    }
                }
                AnnotationEvent::Complete {
                    succeeded,
                    failed,
                    remaining,
                    ..
                } => {
                    if let Some(ref progress) = *pb_clone.lock().await {
                        progress.finish_and_clear();
                    }
                    *pb_clone.lock().await = None;

                    println!(
                        "{} {} complete: {} succeeded, {} failed",
                        style("✓").green(),
                        label,
                        succeeded,
                        failed
                    );

                    if remaining > 0 {
                        println!(
                            "  {} {} documents still need {}",
                            style("→").dim(),
                            remaining,
                            label.to_lowercase()
                        );
                    }
                }
            }
        }
    })
}

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
    let repos = settings.repositories()?;
    let manager = AnnotationManager::new(repos.documents.clone());

    // Initial config load
    let config = Config::load().await;
    let config_history = repos.config_history;

    let mut config_watcher =
        ConfigWatcher::new(daemon, reload, config_history, config.hash()).await;
    let mut llm_config = config.llm.clone();
    if let Some(ref ep) = endpoint {
        llm_config.set_endpoint(ep.clone());
    }
    if let Some(ref m) = model {
        llm_config.set_model(m.clone());
    }

    if !llm_config.enabled() {
        println!(
            "{} LLM annotation is disabled in configuration",
            style("!").yellow()
        );
        println!("  Set llm.enabled = true in your foiacquire.json config");
        return Ok(());
    }

    let mut annotator = LlmAnnotator::new(llm_config.clone());

    println!(
        "{} Using {} at {} (model: {})",
        style("✓").green(),
        llm_config.provider_name(),
        llm_config.endpoint(),
        llm_config.model()
    );

    if !annotator.is_available().await {
        println!(
            "{} {}",
            style("✗").red(),
            annotator.llm_config().availability_hint()
        );
        return Ok(());
    }

    // Single document mode
    if let Some(id) = doc_id {
        println!("{} Processing single document: {}", style("→").cyan(), id);
        let (event_tx, _event_rx) = mpsc::channel::<AnnotationEvent>(100);
        return manager.process_single(&annotator, id, event_tx).await;
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
        // Reload config in daemon mode
        if daemon && matches!(reload, ReloadMode::NextRun | ReloadMode::Inplace) {
            let fresh_config = Config::load().await;
            let mut new_llm_config = fresh_config.llm.clone();
            if let Some(ref ep) = endpoint {
                new_llm_config.set_endpoint(ep.clone());
            }
            if let Some(ref m) = model {
                new_llm_config.set_model(m.clone());
            }

            if new_llm_config.endpoint() != llm_config.endpoint()
                || new_llm_config.model() != llm_config.model()
                || new_llm_config.enabled() != llm_config.enabled()
            {
                println!(
                    "{} Config reloaded (model: {})",
                    style("↻").cyan(),
                    new_llm_config.model()
                );
                llm_config = new_llm_config;
                config_watcher.update_hash(fresh_config.hash());
                annotator = LlmAnnotator::new(llm_config.clone());
            }
        }

        let total_count = manager.count_needing(&annotator, source_id).await?;

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

        let (event_tx, event_rx) = mpsc::channel::<AnnotationEvent>(100);
        let event_handler = spawn_progress_handler(event_rx, "Annotation");

        let _result = manager
            .run_batch(&annotator, source_id, limit, event_tx)
            .await?;

        if let Err(e) = event_handler.await {
            tracing::warn!("Event handler task failed: {}", e);
        }

        if !daemon {
            break;
        }

        match config_watcher.sleep_or_reload(interval, "reloading").await {
            DaemonAction::Exit => return Ok(()),
            DaemonAction::Continue | DaemonAction::Reload => {}
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
    let repos = settings.repositories()?;

    let annotator = DateAnnotator::new(dry_run);
    let manager = AnnotationManager::new(repos.documents);

    let total_count = manager.count_needing(&annotator, source_id).await?;

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

    let (event_tx, event_rx) = mpsc::channel::<AnnotationEvent>(100);
    let event_handler = spawn_progress_handler(event_rx, "Date detection");

    let result = manager
        .run_batch(&annotator, source_id, limit, event_tx)
        .await?;

    if let Err(e) = event_handler.await {
        tracing::warn!("Event handler task failed: {}", e);
    }

    if dry_run && result.succeeded > 0 {
        println!(
            "  {} Run without --dry-run to update database",
            style("→").dim()
        );
    }

    Ok(())
}

/// Extract named entities from documents.
pub async fn cmd_extract_entities(
    settings: &Settings,
    source_id: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    let repos = settings.repositories()?;

    let annotator = NerAnnotator::new();
    let manager = AnnotationManager::new(repos.documents);

    let total_count = manager.count_needing(&annotator, source_id).await?;

    if total_count == 0 {
        println!(
            "{} No documents need entity extraction",
            style("!").yellow()
        );
        println!("  Documents need OCR complete status with extracted text");
        return Ok(());
    }

    let effective_limit = if limit > 0 {
        limit
    } else {
        total_count as usize
    };

    println!(
        "{} Extracting entities from up to {} documents",
        style("→").cyan(),
        effective_limit
    );

    let (event_tx, event_rx) = mpsc::channel::<AnnotationEvent>(100);
    let event_handler = spawn_progress_handler(event_rx, "Entity extraction");

    let _result = manager
        .run_batch(&annotator, source_id, limit, event_tx)
        .await?;

    if let Err(e) = event_handler.await {
        tracing::warn!("Event handler task failed: {}", e);
    }

    Ok(())
}

/// Reset annotations for documents, allowing them to be re-annotated.
pub async fn cmd_annotate_reset(
    settings: &Settings,
    source_id: Option<&str>,
    confirm: bool,
) -> anyhow::Result<()> {
    let repos = settings.repositories()?;
    let doc_repo = repos.documents;

    let count = doc_repo.count_annotated(source_id).await?;

    if count == 0 {
        println!(
            "{} No annotated documents found to reset",
            style("!").yellow()
        );
        return Ok(());
    }

    let scope = source_id.unwrap_or("all sources");
    println!(
        "{} Found {} annotated documents in {}",
        style("→").cyan(),
        count,
        scope
    );

    if !confirm {
        print!(
            "Reset annotations for {} documents? This will clear synopses and tags. [y/N] ",
            count
        );
        use std::io::Write;
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("{} Cancelled", style("!").yellow());
            return Ok(());
        }
    }

    let reset_count = doc_repo.reset_annotations(source_id).await?;

    println!(
        "{} Reset {} documents - they will be re-annotated on next run",
        style("✓").green(),
        reset_count
    );

    Ok(())
}
