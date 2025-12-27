//! Document analysis service.
//!
//! Handles MIME detection, text extraction, and OCR processing.
//! Separated from UI concerns - emits events for progress tracking.

mod processing;
mod types;

use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::repository::DieselDocumentRepository;

pub use processing::{extract_document_text_per_page, ocr_document_page};
pub use types::{OcrEvent, OcrResult};

/// Service for OCR processing.
pub struct OcrService {
    doc_repo: DieselDocumentRepository,
}

impl OcrService {
    /// Create a new OCR service.
    pub fn new(doc_repo: DieselDocumentRepository) -> Self {
        Self { doc_repo }
    }

    /// Get count of documents needing OCR processing.
    pub async fn count_needing_processing(&self, source_id: Option<&str>) -> anyhow::Result<(u64, u64)> {
        let docs = self.doc_repo.count_needing_ocr(source_id).await?;
        let pages = self.doc_repo.count_pages_needing_ocr().await?;
        Ok((docs, pages))
    }

    /// Analyze documents: detect MIME types, extract text, and run OCR.
    pub async fn process(
        &self,
        source_id: Option<&str>,
        workers: usize,
        limit: usize,
        event_tx: mpsc::Sender<OcrEvent>,
    ) -> anyhow::Result<OcrResult> {
        let mut result = OcrResult {
            mime_checked: 0,
            mime_fixed: 0,
            phase1_succeeded: 0,
            phase1_failed: 0,
            pages_created: 0,
            phase2_improved: 0,
            phase2_skipped: 0,
            phase2_failed: 0,
        };

        // First, finalize any documents that have all pages complete but weren't finalized
        // (this handles documents processed before incremental finalization was added)
        let pending_finalized = self.doc_repo.finalize_pending_documents().await?;
        if pending_finalized > 0 {
            tracing::info!(
                "Finalized {} documents that had all pages complete",
                pending_finalized
            );
        }

        // ==================== PHASE 0: MIME Detection ====================
        self.process_phase0_mime(source_id, limit, &event_tx, &mut result)
            .await?;

        // ==================== PHASE 1: Text Extraction ====================
        self.process_phase1(source_id, workers, limit, &event_tx, &mut result)
            .await?;

        // ==================== PHASE 2: OCR All Pages ====================
        self.process_phase2(workers, &event_tx, &mut result).await?;

        // Documents are finalized incrementally as their last page completes.
        // No separate Phase 3 needed.

        Ok(result)
    }

    /// Phase 0: Detect and fix MIME types based on file content.
    async fn process_phase0_mime(
        &self,
        source_id: Option<&str>,
        limit: usize,
        event_tx: &mpsc::Sender<OcrEvent>,
        result: &mut OcrResult,
    ) -> anyhow::Result<()> {
        // Get documents needing OCR (same as Phase 1) - we check MIME before processing
        let total_count = self.doc_repo.count_needing_ocr(source_id).await?;

        if total_count == 0 {
            return Ok(());
        }

        let effective_limit = if limit > 0 {
            limit.min(total_count as usize)
        } else {
            total_count as usize
        };

        let _ = event_tx
            .send(OcrEvent::MimeCheckStarted {
                total_documents: effective_limit,
            })
            .await;

        let docs = self.doc_repo.get_needing_ocr(effective_limit).await?;
        let mut checked = 0;
        let mut fixed = 0;

        for doc in docs {
            checked += 1;

            // Get the current version's file path and MIME type
            if let Some(version) = doc.versions.last() {
                let path = &version.file_path;
                if path.exists() {
                    if let Some((detected_mime, old_mime)) =
                        self.detect_mime_mismatch(path, &version.mime_type)
                    {
                        // Update the MIME type in database
                        if self
                            .doc_repo
                            .update_version_mime_type(version.id, &detected_mime)
                            .await
                            .is_ok()
                        {
                            fixed += 1;
                            let _ = event_tx
                                .send(OcrEvent::MimeFixed {
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

        result.mime_checked = checked;
        result.mime_fixed = fixed;

        let _ = event_tx
            .send(OcrEvent::MimeCheckComplete { checked, fixed })
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
        event_tx: &mpsc::Sender<OcrEvent>,
        result: &mut OcrResult,
    ) -> anyhow::Result<()> {
        let total_count = self.doc_repo.count_needing_ocr(source_id).await?;

        if total_count == 0 {
            return Ok(());
        }

        let effective_limit = if limit > 0 {
            limit
        } else {
            total_count as usize
        };

        let _ = event_tx
            .send(OcrEvent::Phase1Started {
                total_documents: effective_limit.min(total_count as usize),
            })
            .await;

        let succeeded = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let pages_created = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(AtomicUsize::new(0));

        let batch_size = workers * 4;
        let mut offset = 0;

        while offset < effective_limit {
            let batch_limit = (effective_limit - offset).min(batch_size);
            let docs = self.doc_repo.get_needing_ocr(batch_limit).await?;

            if docs.is_empty() {
                break;
            }

            let mut handles = Vec::with_capacity(docs.len().min(workers));

            for doc in docs {
                let current = processed.load(Ordering::Relaxed);
                if current >= effective_limit {
                    break;
                }

                let doc_repo = self.doc_repo.clone();
                let processed = processed.clone();
                let succeeded = succeeded.clone();
                let failed = failed.clone();
                let pages_created = pages_created.clone();
                let event_tx = event_tx.clone();

                let handle = tokio::task::spawn_blocking(move || {
                    let doc_id = doc.id.clone();
                    let title = doc.title.clone();

                    // Send start event (blocking send since we're in spawn_blocking)
                    let _ = futures::executor::block_on(event_tx.send(OcrEvent::DocumentStarted {
                        document_id: doc_id.clone(),
                        title,
                    }));

                    // Get tokio runtime handle to run async code in blocking context
                    let handle = tokio::runtime::Handle::current();

                    match extract_document_text_per_page(&doc, &doc_repo, &handle) {
                        Ok(page_count) => {
                            pages_created.fetch_add(page_count, Ordering::Relaxed);
                            succeeded.fetch_add(1, Ordering::Relaxed);
                            let _ = futures::executor::block_on(event_tx.send(
                                OcrEvent::DocumentCompleted {
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
                                    OcrEvent::DocumentFailed {
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
                        let _ = h.await;
                    }
                }
            }

            for h in handles {
                let _ = h.await;
            }

            offset += batch_limit;
        }

        result.phase1_succeeded = succeeded.load(Ordering::Relaxed);
        result.phase1_failed = failed.load(Ordering::Relaxed);
        result.pages_created = pages_created.load(Ordering::Relaxed);

        let _ = event_tx
            .send(OcrEvent::Phase1Complete {
                succeeded: result.phase1_succeeded,
                failed: result.phase1_failed,
                pages_created: result.pages_created,
            })
            .await;

        Ok(())
    }

    async fn process_phase2(
        &self,
        workers: usize,
        event_tx: &mpsc::Sender<OcrEvent>,
        result: &mut OcrResult,
    ) -> anyhow::Result<()> {
        let pages_needing_ocr = self.doc_repo.count_pages_needing_ocr().await?;

        if pages_needing_ocr == 0 {
            return Ok(());
        }

        let effective_limit = pages_needing_ocr as usize;

        let _ = event_tx
            .send(OcrEvent::Phase2Started {
                total_pages: effective_limit,
            })
            .await;

        let processed = Arc::new(AtomicUsize::new(0));
        let ocr_improved = Arc::new(AtomicUsize::new(0));
        let ocr_skipped = Arc::new(AtomicUsize::new(0));
        let ocr_failed = Arc::new(AtomicUsize::new(0));

        let batch_size = workers * 2;
        let mut offset = 0;

        while offset < effective_limit {
            let batch_limit = (effective_limit - offset).min(batch_size);
            let pages = self.doc_repo.get_pages_needing_ocr("", 0, batch_limit).await?;

            if pages.is_empty() {
                break;
            }

            let mut handles = Vec::with_capacity(pages.len().min(workers));

            for page in pages {
                let current = processed.load(Ordering::Relaxed);
                if current >= effective_limit {
                    break;
                }

                let doc_repo = self.doc_repo.clone();
                let processed = processed.clone();
                let ocr_improved = ocr_improved.clone();
                let ocr_skipped = ocr_skipped.clone();
                let ocr_failed = ocr_failed.clone();
                let event_tx = event_tx.clone();

                let handle = tokio::task::spawn_blocking(move || {
                    let doc_id = page.document_id.clone();
                    let page_num = page.page_number;

                    let _ = futures::executor::block_on(event_tx.send(OcrEvent::PageOcrStarted {
                        document_id: doc_id.clone(),
                        page_number: page_num,
                    }));

                    // Get tokio runtime handle to run async code in blocking context
                    let handle = tokio::runtime::Handle::current();

                    match ocr_document_page(&page, &doc_repo, &handle) {
                        Ok(ocr_result) => {
                            if ocr_result.improved {
                                ocr_improved.fetch_add(1, Ordering::Relaxed);
                            } else {
                                ocr_skipped.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = futures::executor::block_on(event_tx.send(
                                OcrEvent::PageOcrCompleted {
                                    document_id: doc_id.clone(),
                                    page_number: page_num,
                                    improved: ocr_result.improved,
                                },
                            ));

                            // Emit event when document is finalized during incremental processing
                            if ocr_result.document_finalized {
                                let _ = futures::executor::block_on(event_tx.send(
                                    OcrEvent::DocumentFinalized {
                                        document_id: doc_id,
                                    },
                                ));
                            }
                        }
                        Err(e) => {
                            tracing::debug!("OCR failed for page {}: {}", page.page_number, e);
                            ocr_failed.fetch_add(1, Ordering::Relaxed);
                            let _ = futures::executor::block_on(event_tx.send(
                                OcrEvent::PageOcrFailed {
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
                        let _ = h.await;
                    }
                }
            }

            for h in handles {
                let _ = h.await;
            }

            offset += batch_limit;
        }

        result.phase2_improved = ocr_improved.load(Ordering::Relaxed);
        result.phase2_skipped = ocr_skipped.load(Ordering::Relaxed);
        result.phase2_failed = ocr_failed.load(Ordering::Relaxed);

        let _ = event_tx
            .send(OcrEvent::Phase2Complete {
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
        _event_tx: mpsc::Sender<OcrEvent>,
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

        let pages = tokio::task::spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            extract_document_text_per_page(&doc_clone, &doc_repo, &handle)
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
