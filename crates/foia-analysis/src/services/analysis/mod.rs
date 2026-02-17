//! Document analysis service.
//!
//! Handles MIME detection, text extraction, and OCR processing.
//! Separated from UI concerns - emits events for progress tracking.

mod processing;
mod types;

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::analysis::AnalysisManager;
use foia::repository::DieselDocumentRepository;

pub use processing::{extract_document_text_per_page, ocr_document_page_with_config};
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
    pub async fn process(
        &self,
        source_id: Option<&str>,
        methods: &[String],
        workers: usize,
        limit: usize,
        mime_type: Option<&str>,
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
        let mut result = AnalysisResult {
            mime_checked: 0,
            mime_fixed: 0,
            phase1_succeeded: 0,
            phase1_failed: 0,
            phase1_skipped_missing: 0,
            pages_created: 0,
            phase2_improved: 0,
            phase2_skipped: 0,
            phase2_failed: 0,
        };

        // First, finalize any documents that have all pages complete but weren't finalized
        // (this handles documents processed before incremental finalization was added)
        tracing::debug!("Finalizing pending documents...");
        let pending_finalized = self.doc_repo.finalize_pending_documents().await?;
        tracing::debug!("Finalized {} pending documents", pending_finalized);

        // Migrate legacy file_path values to deterministic paths
        self.migrate_legacy_file_paths().await;

        // Only run OCR phases if OCR methods are requested
        if has_ocr_methods {
            // Backfill completion rows for already-processed documents
            // so they aren't re-scanned by Phase 0
            for method in &methods {
                self.backfill_analysis_completions(method).await;
            }

            // ==================== PHASE 0: MIME Detection ====================
            tracing::debug!("Starting Phase 0: MIME detection");
            self.process_phase0_mime(source_id, limit, mime_type, &event_tx, &mut result)
                .await?;
            tracing::debug!(
                "Phase 0 complete: checked={}, fixed={}",
                result.mime_checked,
                result.mime_fixed
            );

            // ==================== PHASE 1: Text Extraction ====================
            tracing::debug!("Starting Phase 1: Text extraction");
            self.process_phase1(source_id, workers, limit, mime_type, &event_tx, &mut result)
                .await?;
            tracing::debug!(
                "Phase 1 complete: succeeded={}, failed={}, pages={}",
                result.phase1_succeeded,
                result.phase1_failed,
                result.pages_created
            );

            // ==================== PHASE 2: OCR All Pages ====================
            tracing::debug!("Starting Phase 2: OCR all pages");
            self.process_phase2(workers, &event_tx, &mut result).await?;
            tracing::debug!(
                "Phase 2 complete: improved={}, skipped={}, failed={}",
                result.phase2_improved,
                result.phase2_skipped,
                result.phase2_failed
            );
        }

        // TODO: Add document-level analysis phase for methods like Whisper
        // This would process audio/video files using the analysis backends
        // that have granularity == AnalysisGranularity::Document

        // Documents are finalized incrementally as their last page completes.
        // No separate Phase 3 needed.

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

    /// Phase 0: Detect and fix MIME types based on file content.
    async fn process_phase0_mime(
        &self,
        source_id: Option<&str>,
        limit: usize,
        mime_type: Option<&str>,
        event_tx: &mpsc::Sender<AnalysisEvent>,
        result: &mut AnalysisResult,
    ) -> anyhow::Result<()> {
        // Get documents needing analysis - we check MIME before processing
        let total_count = self
            .doc_repo
            .count_needing_analysis("ocr", source_id, mime_type, self.retry_interval_hours)
            .await?;

        if total_count == 0 {
            return Ok(());
        }

        let max_to_check = if limit > 0 {
            limit
        } else {
            total_count as usize
        };

        let _ = event_tx
            .send(AnalysisEvent::MimeCheckStarted {
                total_documents: max_to_check,
            })
            .await;

        let mut checked = 0;
        let mut fixed = 0;
        let mut cursor: Option<String> = None;
        let batch_size = 200;

        loop {
            if checked >= max_to_check {
                break;
            }

            let remaining = max_to_check - checked;
            let docs = self
                .doc_repo
                .get_needing_analysis(
                    "ocr",
                    remaining.min(batch_size),
                    source_id,
                    mime_type,
                    cursor.as_deref(),
                    self.retry_interval_hours,
                )
                .await?;

            if docs.is_empty() {
                break;
            }

            if let Some(last) = docs.last() {
                cursor = Some(last.id.clone());
            }

            for doc in docs {
                checked += 1;
                let _ = event_tx
                    .send(AnalysisEvent::MimeChecked {
                        document_id: doc.id.clone(),
                    })
                    .await;

                if let Some(version) = doc.versions.last() {
                    let path =
                        version.resolve_path(&self.documents_dir, &doc.source_url, &doc.title);
                    if path.exists() {
                        if let Some((detected_mime, old_mime)) =
                            self.detect_mime_mismatch(&path, &version.mime_type)
                        {
                            if self
                                .doc_repo
                                .update_version_mime_type(version.id, &detected_mime)
                                .await
                                .is_ok()
                            {
                                fixed += 1;
                                let _ = event_tx
                                    .send(AnalysisEvent::MimeFixed {
                                        document_id: doc.id.clone(),
                                        old_mime,
                                        new_mime: detected_mime,
                                    })
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        result.mime_checked = checked;
        result.mime_fixed = fixed;

        let _ = event_tx
            .send(AnalysisEvent::MimeCheckComplete { checked, fixed })
            .await;

        Ok(())
    }

    /// Detect MIME type from file content and check if it differs from stored type.
    /// Returns Some((detected_mime, old_mime)) if they differ, None if they match.
    fn detect_mime_mismatch(
        &self,
        path: &std::path::Path,
        stored_mime: &str,
    ) -> Option<(String, String)> {
        // Read first 8KB for magic byte detection
        let mut file = File::open(path).ok()?;
        let mut buffer = [0u8; 8192];
        let bytes_read = file.read(&mut buffer).ok()?;

        if bytes_read == 0 {
            return None;
        }

        // Use infer to detect MIME type from content
        let detected = infer::get(&buffer[..bytes_read])?;
        let detected_mime = detected.mime_type();

        // Normalize stored MIME for comparison (strip charset, etc.)
        let stored_normalized = stored_mime
            .split(';')
            .next()
            .unwrap_or(stored_mime)
            .trim()
            .to_lowercase();

        // Check if they differ meaningfully
        if detected_mime != stored_normalized {
            // Don't "fix" generic types to specific ones if the stored type is reasonable
            // e.g., don't change "application/octet-stream" -> detected
            if stored_normalized == "application/octet-stream"
                || stored_normalized == "binary/octet-stream"
            {
                return Some((detected_mime.to_string(), stored_normalized));
            }

            // Check for mismatched types (e.g., stored as text/html but actually PDF)
            let stored_base = stored_normalized.split('/').next().unwrap_or("");
            let detected_base = detected_mime.split('/').next().unwrap_or("");

            if stored_base != detected_base {
                // Different type families - definitely fix
                return Some((detected_mime.to_string(), stored_normalized));
            }
        }

        None
    }

    async fn process_phase1(
        &self,
        source_id: Option<&str>,
        workers: usize,
        limit: usize,
        mime_type: Option<&str>,
        event_tx: &mpsc::Sender<AnalysisEvent>,
        result: &mut AnalysisResult,
    ) -> anyhow::Result<()> {
        let total_count = self
            .doc_repo
            .count_needing_analysis("ocr", source_id, mime_type, self.retry_interval_hours)
            .await?;

        if total_count == 0 {
            return Ok(());
        }

        let max_to_process = if limit > 0 { limit } else { usize::MAX };

        let _ = event_tx
            .send(AnalysisEvent::Phase1Started {
                total_documents: if limit > 0 {
                    limit
                } else {
                    total_count as usize
                },
            })
            .await;

        let succeeded = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let skipped_missing = Arc::new(AtomicUsize::new(0));
        let pages_created = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(AtomicUsize::new(0));

        let batch_size = workers * 4;
        let mut cursor: Option<String> = None;

        loop {
            let current_processed = processed.load(Ordering::Relaxed);
            if current_processed >= max_to_process {
                break;
            }

            let remaining = max_to_process - current_processed;
            let batch_limit = remaining.min(batch_size);

            let docs = self
                .doc_repo
                .get_needing_analysis(
                    "ocr",
                    batch_limit,
                    source_id,
                    mime_type,
                    cursor.as_deref(),
                    self.retry_interval_hours,
                )
                .await?;

            if docs.is_empty() {
                break;
            }

            // Advance cursor past this batch regardless of skip/process
            if let Some(last) = docs.last() {
                cursor = Some(last.id.clone());
            }

            let mut handles = Vec::with_capacity(docs.len().min(workers));

            for doc in docs {
                // Skip documents whose files aren't on disk yet
                let file_available = doc.current_version().is_some_and(|v| {
                    let path = v.resolve_path(&self.documents_dir, &doc.source_url, &doc.title);
                    match std::fs::metadata(&path) {
                        Ok(_) => true,
                        Err(e) => {
                            tracing::debug!(
                                "File unavailable for {}: {} (path: {})",
                                doc.title,
                                e,
                                path.display()
                            );
                            false
                        }
                    }
                });
                if !file_available {
                    if doc.current_version().is_none() {
                        tracing::debug!("Skipping {}: no version record", doc.title);
                    }
                    skipped_missing.fetch_add(1, Ordering::Relaxed);
                    let _ = event_tx
                        .send(AnalysisEvent::DocumentSkipped {
                            document_id: doc.id.clone(),
                        })
                        .await;
                    continue;
                }

                // Claim the document so other workers skip it
                if let Some(v) = doc.current_version() {
                    let _ = self
                        .doc_repo
                        .claim_analysis(&doc.id, v.id as i32, "ocr")
                        .await;
                }

                let doc_repo = self.doc_repo.clone();
                let documents_dir = self.documents_dir.clone();
                let processed = processed.clone();
                let succeeded = succeeded.clone();
                let failed = failed.clone();
                let pages_created = pages_created.clone();
                let event_tx = event_tx.clone();

                let handle = tokio::task::spawn_blocking(move || {
                    let doc_id = doc.id.clone();
                    let title = doc.title.clone();

                    let _ = futures::executor::block_on(event_tx.send(
                        AnalysisEvent::DocumentStarted {
                            document_id: doc_id.clone(),
                            title,
                        },
                    ));

                    let handle = tokio::runtime::Handle::current();

                    match extract_document_text_per_page(&doc, &doc_repo, &handle, &documents_dir) {
                        Ok(page_count) => {
                            pages_created.fetch_add(page_count, Ordering::Relaxed);
                            succeeded.fetch_add(1, Ordering::Relaxed);
                            let _ = futures::executor::block_on(event_tx.send(
                                AnalysisEvent::DocumentCompleted {
                                    document_id: doc_id,
                                    pages_extracted: page_count,
                                },
                            ));
                        }
                        Err(e) => {
                            let err_str = e.to_string();
                            if !err_str.contains("Unsupported file type") {
                                tracing::warn!("Text extraction failed for {}: {}", doc.title, e);
                                failed.fetch_add(1, Ordering::Relaxed);
                                let _ = futures::executor::block_on(event_tx.send(
                                    AnalysisEvent::DocumentFailed {
                                        document_id: doc_id,
                                        error: err_str,
                                    },
                                ));
                            }
                        }
                    }

                    processed.fetch_add(1, Ordering::Relaxed);
                });

                handles.push(handle);

                if handles.len() >= workers {
                    for h in handles.drain(..) {
                        if let Err(e) = h.await {
                            tracing::error!("Analysis worker panicked: {}", e);
                        }
                    }
                }
            }

            for h in handles {
                if let Err(e) = h.await {
                    tracing::error!("Analysis worker panicked: {}", e);
                }
            }
        }

        result.phase1_succeeded = succeeded.load(Ordering::Relaxed);
        result.phase1_failed = failed.load(Ordering::Relaxed);
        result.phase1_skipped_missing = skipped_missing.load(Ordering::Relaxed);
        result.pages_created = pages_created.load(Ordering::Relaxed);

        let _ = event_tx
            .send(AnalysisEvent::Phase1Complete {
                succeeded: result.phase1_succeeded,
                failed: result.phase1_failed,
                pages_created: result.pages_created,
                skipped_missing: result.phase1_skipped_missing,
            })
            .await;

        Ok(())
    }

    async fn process_phase2(
        &self,
        workers: usize,
        event_tx: &mpsc::Sender<AnalysisEvent>,
        result: &mut AnalysisResult,
    ) -> anyhow::Result<()> {
        let _ = event_tx
            .send(AnalysisEvent::Phase2Started {
                total_pages: 0, // Unknown total, process until none remain
            })
            .await;

        let processed = Arc::new(AtomicUsize::new(0));
        let ocr_improved = Arc::new(AtomicUsize::new(0));
        let ocr_skipped = Arc::new(AtomicUsize::new(0));
        let ocr_failed = Arc::new(AtomicUsize::new(0));

        let batch_size = workers * 2;

        loop {
            let batch_limit = batch_size;
            let pages = self.doc_repo.get_all_pages_needing_ocr(batch_limit).await?;

            if pages.is_empty() {
                break; // No more pages to process
            }

            let mut handles = Vec::with_capacity(pages.len().min(workers));

            for page in pages {
                let doc_repo = self.doc_repo.clone();
                let ocr_config = self.ocr_config.clone();
                let documents_dir = self.documents_dir.clone();
                let processed = processed.clone();
                let ocr_improved = ocr_improved.clone();
                let ocr_skipped = ocr_skipped.clone();
                let ocr_failed = ocr_failed.clone();
                let event_tx = event_tx.clone();

                let handle = tokio::task::spawn_blocking(move || {
                    let doc_id = page.document_id.clone();
                    let page_num = page.page_number;

                    let _ =
                        futures::executor::block_on(event_tx.send(AnalysisEvent::PageOcrStarted {
                            document_id: doc_id.clone(),
                            page_number: page_num,
                        }));

                    // Get tokio runtime handle to run async code in blocking context
                    let handle = tokio::runtime::Handle::current();

                    match ocr_document_page_with_config(
                        &page,
                        &doc_repo,
                        &handle,
                        &ocr_config,
                        &documents_dir,
                    ) {
                        Ok(ocr_result) => {
                            if ocr_result.improved {
                                ocr_improved.fetch_add(1, Ordering::Relaxed);
                            } else {
                                ocr_skipped.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = futures::executor::block_on(event_tx.send(
                                AnalysisEvent::PageOcrCompleted {
                                    document_id: doc_id.clone(),
                                    page_number: page_num,
                                    improved: ocr_result.improved,
                                },
                            ));

                            // Emit event when document is finalized during incremental processing
                            if ocr_result.document_finalized {
                                let _ = futures::executor::block_on(event_tx.send(
                                    AnalysisEvent::DocumentFinalized {
                                        document_id: doc_id,
                                    },
                                ));
                            }
                        }
                        Err(e) => {
                            tracing::debug!("OCR failed for page {}: {}", page.page_number, e);
                            ocr_failed.fetch_add(1, Ordering::Relaxed);
                            let _ = futures::executor::block_on(event_tx.send(
                                AnalysisEvent::PageOcrFailed {
                                    document_id: doc_id,
                                    page_number: page_num,
                                    error: e.to_string(),
                                },
                            ));
                        }
                    }

                    processed.fetch_add(1, Ordering::Relaxed);
                });

                handles.push(handle);

                if handles.len() >= workers {
                    for h in handles.drain(..) {
                        if let Err(e) = h.await {
                            tracing::error!("Analysis worker panicked: {}", e);
                        }
                    }
                }
            }

            for h in handles {
                if let Err(e) = h.await {
                    tracing::error!("Analysis worker panicked: {}", e);
                }
            }
        }

        result.phase2_improved = ocr_improved.load(Ordering::Relaxed);
        result.phase2_skipped = ocr_skipped.load(Ordering::Relaxed);
        result.phase2_failed = ocr_failed.load(Ordering::Relaxed);

        let _ = event_tx
            .send(AnalysisEvent::Phase2Complete {
                improved: result.phase2_improved,
                skipped: result.phase2_skipped,
                failed: result.phase2_failed,
            })
            .await;

        Ok(())
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
