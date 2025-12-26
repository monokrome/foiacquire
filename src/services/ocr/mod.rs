//! OCR processing service.
//!
//! Handles document text extraction and OCR processing.
//! Separated from UI concerns - emits events for progress tracking.

mod processing;
mod types;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::repository::DocumentRepository;

pub use processing::{extract_document_text_per_page, ocr_document_page};
pub use types::{OcrEvent, OcrResult};

/// Service for OCR processing.
pub struct OcrService {
    doc_repo: Arc<DocumentRepository>,
}

impl OcrService {
    /// Create a new OCR service.
    pub fn new(doc_repo: Arc<DocumentRepository>) -> Self {
        Self { doc_repo }
    }

    /// Get count of documents needing OCR processing.
    pub fn count_needing_processing(&self, source_id: Option<&str>) -> anyhow::Result<(u64, u64)> {
        let docs = self.doc_repo.count_needing_ocr(source_id)?;
        let pages = self.doc_repo.count_pages_needing_ocr()?;
        Ok((docs, pages))
    }

    /// Process documents with OCR.
    pub async fn process(
        &self,
        source_id: Option<&str>,
        workers: usize,
        limit: usize,
        event_tx: mpsc::Sender<OcrEvent>,
    ) -> anyhow::Result<OcrResult> {
        let mut result = OcrResult {
            phase1_succeeded: 0,
            phase1_failed: 0,
            pages_created: 0,
            phase2_improved: 0,
            phase2_skipped: 0,
            phase2_failed: 0,
        };

        // First, finalize any documents that have all pages complete but weren't finalized
        // (this handles documents processed before incremental finalization was added)
        let pending_finalized = self.doc_repo.finalize_pending_documents(source_id)?;
        if pending_finalized > 0 {
            tracing::info!(
                "Finalized {} documents that had all pages complete",
                pending_finalized
            );
        }

        // ==================== PHASE 1: Text Extraction ====================
        self.process_phase1(source_id, workers, limit, &event_tx, &mut result)
            .await?;

        // ==================== PHASE 2: OCR All Pages ====================
        self.process_phase2(workers, &event_tx, &mut result).await?;

        // Documents are finalized incrementally as their last page completes.
        // No separate Phase 3 needed.

        Ok(result)
    }

    async fn process_phase1(
        &self,
        source_id: Option<&str>,
        workers: usize,
        limit: usize,
        event_tx: &mpsc::Sender<OcrEvent>,
        result: &mut OcrResult,
    ) -> anyhow::Result<()> {
        let total_count = self.doc_repo.count_needing_ocr(source_id)?;

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
            let docs = self.doc_repo.get_needing_ocr(source_id, batch_limit)?;

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

                    match extract_document_text_per_page(&doc, &doc_repo) {
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
        let pages_needing_ocr = self.doc_repo.count_pages_needing_ocr()?;

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
            let pages = self.doc_repo.get_pages_needing_ocr(batch_limit)?;

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

                    match ocr_document_page(&page, &doc_repo) {
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
            .get(doc_id)?
            .ok_or_else(|| anyhow::anyhow!("Document not found: {}", doc_id))?;

        println!("  {} Processing: {}", console::style("→").cyan(), doc.title);

        // Extract text per-page
        match extract_document_text_per_page(&doc, &self.doc_repo) {
            Ok(pages) => {
                println!(
                    "  {} Extracted {} pages",
                    console::style("✓").green(),
                    pages
                );
            }
            Err(e) => {
                println!("  {} Failed: {}", console::style("✗").red(), e);
                return Err(e);
            }
        }

        // Finalize the document
        self.doc_repo.finalize_document(doc_id)?;
        println!("  {} Document finalized", console::style("✓").green());

        Ok(())
    }
}
