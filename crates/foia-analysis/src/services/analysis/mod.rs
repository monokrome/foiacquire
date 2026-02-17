//! Document analysis service.
//!
//! Handles MIME detection, text extraction, and OCR processing.
//! Separated from UI concerns - emits events for progress tracking.

mod processing;
pub mod stages;
mod types;

use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::analysis::AnalysisManager;
use foia::repository::DieselDocumentRepository;
use foia::work_queue::{ExecutionStrategy, PipelineEvent, PipelineRunner};

pub use processing::{extract_document_text_per_page, ocr_document_page_with_config};
pub use stages::{OcrStage, TextExtractionStage};
pub use types::{AnalysisEvent, AnalysisResult};

use foia::config::OcrConfig;

/// Service for document analysis (MIME detection, text extraction, OCR).
/// Default retry interval for failed analyses (hours).
const DEFAULT_RETRY_INTERVAL_HOURS: u32 = 12;

pub struct AnalysisService {
    doc_repo: DieselDocumentRepository,
    analysis_manager: AnalysisManager,
    ocr_config: OcrConfig,
    documents_dir: PathBuf,
    retry_interval_hours: u32,
}

impl AnalysisService {
    /// Create a new analysis service with default OCR config.
    #[allow(dead_code)]
    pub fn new(doc_repo: DieselDocumentRepository, documents_dir: PathBuf) -> Self {
        Self {
            doc_repo,
            analysis_manager: AnalysisManager::with_defaults(),
            ocr_config: OcrConfig::default(),
            documents_dir,
            retry_interval_hours: DEFAULT_RETRY_INTERVAL_HOURS,
        }
    }

    /// Create a new analysis service with custom OCR config.
    pub fn with_ocr_config(
        doc_repo: DieselDocumentRepository,
        ocr_config: OcrConfig,
        documents_dir: PathBuf,
    ) -> Self {
        Self {
            doc_repo,
            analysis_manager: AnalysisManager::with_defaults(),
            ocr_config,
            documents_dir,
            retry_interval_hours: DEFAULT_RETRY_INTERVAL_HOURS,
        }
    }

    /// Set the retry interval for failed analyses.
    pub fn with_retry_interval(mut self, hours: u32) -> Self {
        self.retry_interval_hours = hours;
        self
    }

    /// Get count of documents needing analysis.
    pub async fn count_needing_processing(
        &self,
        source_id: Option<&str>,
        mime_type: Option<&str>,
    ) -> anyhow::Result<(u64, u64)> {
        let docs = self
            .doc_repo
            .count_needing_analysis("ocr", source_id, mime_type, self.retry_interval_hours)
            .await?;
        let pages = self.doc_repo.count_pages_needing_ocr().await?;
        Ok((docs, pages))
    }

    /// Analyze documents: detect MIME types, extract text, and run analysis.
    ///
    /// The `methods` parameter specifies which analysis methods to run (e.g., ["ocr", "whisper"]).
    /// If empty, defaults to ["ocr"].
    #[allow(clippy::too_many_arguments)]
    pub async fn process(
        &self,
        source_id: Option<&str>,
        methods: &[String],
        workers: usize,
        limit: usize,
        mime_type: Option<&str>,
        chunk_size: Option<usize>,
        strategy: ExecutionStrategy,
        event_tx: mpsc::Sender<AnalysisEvent>,
    ) -> anyhow::Result<AnalysisResult> {
        // Use default methods if none specified
        let methods = if methods.is_empty() {
            vec!["ocr".to_string()]
        } else {
            methods.to_vec()
        };

        // Log available backends for requested methods
        tracing::debug!("Analysis methods requested: {:?}", methods);
        for method in &methods {
            if let Some(backend) = self.analysis_manager.get(method) {
                tracing::debug!(
                    "  {} -> {} (available: {})",
                    method,
                    backend.backend_id(),
                    backend.is_available()
                );
            } else {
                tracing::warn!("  {} -> no backend registered", method);
            }
        }

        // Check if any page-level (OCR) methods are requested
        let has_ocr_methods = methods.iter().any(|m| m == "ocr" || m.starts_with("ocr:"));

        // Pre-pipeline setup
        tracing::debug!("Finalizing pending documents...");
        let pending_finalized = self.doc_repo.finalize_pending_documents().await?;
        tracing::debug!("Finalized {} pending documents", pending_finalized);

        self.migrate_legacy_file_paths().await;

        if !has_ocr_methods {
            return Ok(AnalysisResult::default());
        }

        for method in &methods {
            self.backfill_analysis_completions(method).await;
        }

        // Build pipeline stages
        let effective_chunk = chunk_size.unwrap_or(4096);

        let text_stage = TextExtractionStage::new(
            self.doc_repo.clone(),
            self.documents_dir.clone(),
            source_id,
            mime_type,
            self.retry_interval_hours,
            workers,
        );

        let ocr_stage = OcrStage::new(
            self.doc_repo.clone(),
            self.ocr_config.clone(),
            self.documents_dir.clone(),
            workers,
        );

        let mut runner = PipelineRunner::new(effective_chunk, limit);
        runner.add_stage(Box::new(text_stage));
        runner.add_stage(Box::new(ocr_stage));

        // Bridge PipelineEvent -> AnalysisEvent
        let (pipe_tx, pipe_rx) = mpsc::channel::<PipelineEvent>(100);
        let bridge = tokio::spawn(bridge_pipeline_to_analysis_events(pipe_rx, event_tx));

        runner.run(strategy, pipe_tx).await?;

        // Wait for bridge to finish
        let result = bridge.await?;
        Ok(result)
    }

    /// Backfill completion rows for already-processed documents.
    ///
    /// Documents with status 'indexed' or 'ocr_complete' have already been
    /// through the analysis pipeline but may lack a `document_analysis_results`
    /// row (e.g., they were processed before per-method tracking was added).
    /// Without the row, `count_needing_analysis` treats them as unprocessed
    /// and Phase 0 re-scans every file on every run.
    async fn backfill_analysis_completions(&self, analysis_type: &str) {
        match self
            .doc_repo
            .backfill_analysis_completions(analysis_type)
            .await
        {
            Ok(count) if count > 0 => {
                tracing::info!(
                    "Backfilled {count} analysis completion rows for '{analysis_type}'"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("Failed to backfill analysis completions: {e}");
            }
        }
    }

    /// Migrate legacy file_path values to deterministic paths.
    ///
    /// Versions with an explicit `file_path` that resolves to the same location
    /// as the deterministic `compute_storage_path` don't need the stored path.
    /// Clear them in batches so `resolve_path` uses the computed path instead.
    async fn migrate_legacy_file_paths(&self) {
        // Count how many versions have legacy file_path
        let total = match self.doc_repo.count_legacy_file_paths().await {
            Ok(n) => n,
            Err(_) => return,
        };

        if total == 0 {
            return;
        }

        tracing::info!("Checking path resolution for {total} files...");

        const BATCH_SIZE: usize = 1000;
        let mut cursor: i64 = 0;
        let mut cleared: u64 = 0;
        let mut checked: u64 = 0;

        loop {
            let batch = match self
                .doc_repo
                .get_legacy_file_path_versions(cursor, BATCH_SIZE)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("Failed to query legacy file_path versions: {e}");
                    break;
                }
            };

            if batch.is_empty() {
                break;
            }

            let mut to_clear: Vec<i32> = Vec::new();

            for (version, source_url, title) in &batch {
                cursor = version.id as i64;
                checked += 1;

                let stored = version.resolve_path(&self.documents_dir, source_url, title);
                let computed = self
                    .documents_dir
                    .join(version.compute_storage_path(source_url, title));

                if stored == computed {
                    to_clear.push(version.id as i32);
                }
            }

            if !to_clear.is_empty() {
                match self
                    .doc_repo
                    .clear_version_file_paths_batch(&to_clear)
                    .await
                {
                    Ok(n) => cleared += n as u64,
                    Err(e) => tracing::warn!("Failed to clear legacy file_paths: {e}"),
                }
            }

            if batch.len() < BATCH_SIZE {
                break;
            }
        }

        tracing::info!("Done: {checked} checked, {cleared} updated");
    }

    /// Process a single document by ID.
    pub async fn process_single(
        &self,
        doc_id: &str,
        _event_tx: mpsc::Sender<AnalysisEvent>,
    ) -> anyhow::Result<()> {
        // Get the document
        let doc = self
            .doc_repo
            .get(doc_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Document not found: {}", doc_id))?;

        println!("  {} Processing: {}", console::style("→").cyan(), doc.title);

        // Extract text per-page (run in blocking context for CPU-intensive work)
        let doc_repo = self.doc_repo.clone();
        let doc_clone = doc.clone();
        let doc_id_owned = doc_id.to_string();
        let documents_dir = self.documents_dir.clone();

        let pages = tokio::task::spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            extract_document_text_per_page(&doc_clone, &doc_repo, &handle, &documents_dir)
        })
        .await??;

        println!(
            "  {} Extracted {} pages",
            console::style("✓").green(),
            pages
        );

        // Finalize the document
        self.doc_repo.finalize_document(&doc_id_owned).await?;
        println!("  {} Document finalized", console::style("✓").green());

        Ok(())
    }
}

/// Bridge generic `PipelineEvent`s to domain-specific `AnalysisEvent`s.
///
/// Maps stage names ("Text extraction" / "OCR") to the existing phase-based
/// event variants so the CLI event handler works unchanged.
async fn bridge_pipeline_to_analysis_events(
    mut pipe_rx: mpsc::Receiver<PipelineEvent>,
    event_tx: mpsc::Sender<AnalysisEvent>,
) -> AnalysisResult {
    let mut result = AnalysisResult::default();

    while let Some(event) = pipe_rx.recv().await {
        match event {
            PipelineEvent::StageStarted { ref stage, total_items } => {
                if stage == "Text extraction" {
                    let _ = event_tx
                        .send(AnalysisEvent::Phase1Started {
                            total_documents: total_items as usize,
                        })
                        .await;
                } else if stage == "OCR" {
                    let _ = event_tx
                        .send(AnalysisEvent::Phase2Started {
                            total_pages: total_items as usize,
                        })
                        .await;
                }
            }
            PipelineEvent::ItemStarted { ref stage, ref item_id, ref label } => {
                if stage == "Text extraction" {
                    let _ = event_tx
                        .send(AnalysisEvent::DocumentStarted {
                            document_id: item_id.clone(),
                            title: label.clone(),
                        })
                        .await;
                } else if stage == "OCR" {
                    // Parse page number from item_id "docid:pN"
                    let page_number = item_id
                        .rsplit(":p")
                        .next()
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);
                    let document_id = item_id
                        .rsplit_once(":p")
                        .map(|(d, _)| d.to_string())
                        .unwrap_or_else(|| item_id.clone());
                    let _ = event_tx
                        .send(AnalysisEvent::PageOcrStarted {
                            document_id,
                            page_number,
                        })
                        .await;
                }
            }
            PipelineEvent::ItemCompleted { ref stage, ref item_id, ref detail } => {
                if stage == "Text extraction" {
                    let pages = detail
                        .as_deref()
                        .and_then(|d| d.split(' ').next())
                        .and_then(|n| n.parse::<usize>().ok())
                        .unwrap_or(0);
                    result.phase1_succeeded += 1;
                    result.pages_created += pages;
                    let _ = event_tx
                        .send(AnalysisEvent::DocumentCompleted {
                            document_id: item_id.clone(),
                            pages_extracted: pages,
                        })
                        .await;
                } else if stage == "OCR" {
                    let page_number = item_id
                        .rsplit(":p")
                        .next()
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);
                    let document_id = item_id
                        .rsplit_once(":p")
                        .map(|(d, _)| d.to_string())
                        .unwrap_or_else(|| item_id.clone());
                    let finalized = detail.as_deref() == Some("document finalized");
                    // The ChunkResult from the stage counts "skipped" for pages
                    // that didn't improve, but the event still uses "improved" bool
                    let _ = event_tx
                        .send(AnalysisEvent::PageOcrCompleted {
                            document_id: document_id.clone(),
                            page_number,
                            improved: true,
                        })
                        .await;
                    result.phase2_improved += 1;
                    if finalized {
                        let _ = event_tx
                            .send(AnalysisEvent::DocumentFinalized { document_id })
                            .await;
                    }
                }
            }
            PipelineEvent::ItemSkipped { ref stage, ref item_id } => {
                if stage == "Text extraction" {
                    result.phase1_skipped_missing += 1;
                    let _ = event_tx
                        .send(AnalysisEvent::DocumentSkipped {
                            document_id: item_id.clone(),
                        })
                        .await;
                } else if stage == "OCR" {
                    // OCR "skipped" means text wasn't improved
                    let page_number = item_id
                        .rsplit(":p")
                        .next()
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);
                    let document_id = item_id
                        .rsplit_once(":p")
                        .map(|(d, _)| d.to_string())
                        .unwrap_or_else(|| item_id.clone());
                    result.phase2_skipped += 1;
                    let _ = event_tx
                        .send(AnalysisEvent::PageOcrCompleted {
                            document_id,
                            page_number,
                            improved: false,
                        })
                        .await;
                }
            }
            PipelineEvent::ItemFailed { ref stage, ref item_id, ref error } => {
                if stage == "Text extraction" {
                    result.phase1_failed += 1;
                    let _ = event_tx
                        .send(AnalysisEvent::DocumentFailed {
                            document_id: item_id.clone(),
                            error: error.clone(),
                        })
                        .await;
                } else if stage == "OCR" {
                    let page_number = item_id
                        .rsplit(":p")
                        .next()
                        .and_then(|s| s.parse::<u32>().ok())
                        .unwrap_or(0);
                    let document_id = item_id
                        .rsplit_once(":p")
                        .map(|(d, _)| d.to_string())
                        .unwrap_or_else(|| item_id.clone());
                    result.phase2_failed += 1;
                    let _ = event_tx
                        .send(AnalysisEvent::PageOcrFailed {
                            document_id,
                            page_number,
                            error: error.clone(),
                        })
                        .await;
                }
            }
            PipelineEvent::StageCompleted { ref stage, succeeded, failed, skipped, .. } => {
                if stage == "Text extraction" {
                    let _ = event_tx
                        .send(AnalysisEvent::Phase1Complete {
                            succeeded,
                            failed,
                            pages_created: result.pages_created,
                            skipped_missing: skipped,
                        })
                        .await;
                } else if stage == "OCR" {
                    let _ = event_tx
                        .send(AnalysisEvent::Phase2Complete {
                            improved: succeeded,
                            skipped,
                            failed,
                        })
                        .await;
                }
            }
        }
    }

    result
}
