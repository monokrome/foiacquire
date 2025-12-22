//! OCR processing service.
//!
//! Handles document text extraction and OCR processing.
//! Separated from UI concerns - emits events for progress tracking.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::models::{Document, DocumentPage, PageOcrStatus};
use crate::ocr::TextExtractor;
use crate::repository::DocumentRepository;

/// Events emitted during OCR processing.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum OcrEvent {
    /// Phase 1: Text extraction started
    Phase1Started { total_documents: usize },
    /// Document text extraction started
    DocumentStarted { document_id: String, title: String },
    /// Document text extraction completed
    DocumentCompleted {
        document_id: String,
        pages_extracted: usize,
    },
    /// Document extraction failed
    DocumentFailed { document_id: String, error: String },
    /// Phase 1 complete
    Phase1Complete {
        succeeded: usize,
        failed: usize,
        pages_created: usize,
    },

    /// Phase 2: OCR started
    Phase2Started { total_pages: usize },
    /// Page OCR started
    PageOcrStarted {
        document_id: String,
        page_number: u32,
    },
    /// Page OCR completed
    PageOcrCompleted {
        document_id: String,
        page_number: u32,
        improved: bool,
    },
    /// Page OCR failed
    PageOcrFailed {
        document_id: String,
        page_number: u32,
        error: String,
    },
    /// Document finalized (all pages complete)
    DocumentFinalized { document_id: String },
    /// Phase 2 complete
    Phase2Complete {
        improved: usize,
        skipped: usize,
        failed: usize,
    },
}

/// Result of OCR processing.
#[derive(Debug)]
#[allow(dead_code)]
pub struct OcrResult {
    pub phase1_succeeded: usize,
    pub phase1_failed: usize,
    pub pages_created: usize,
    pub phase2_improved: usize,
    pub phase2_skipped: usize,
    pub phase2_failed: usize,
}

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
        use std::sync::atomic::{AtomicUsize, Ordering};

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
        let total_count = self.doc_repo.count_needing_ocr(source_id)?;

        if total_count > 0 {
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
                        let _ =
                            futures::executor::block_on(event_tx.send(OcrEvent::DocumentStarted {
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
                                    tracing::warn!(
                                        "Text extraction failed for {}: {}",
                                        doc.title,
                                        e
                                    );
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
        }

        // ==================== PHASE 2: OCR All Pages ====================
        let pages_needing_ocr = self.doc_repo.count_pages_needing_ocr()?;

        if pages_needing_ocr > 0 {
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

                        let _ =
                            futures::executor::block_on(event_tx.send(OcrEvent::PageOcrStarted {
                                document_id: doc_id.clone(),
                                page_number: page_num,
                            }));

                        match ocr_document_page(&page, &doc_repo) {
                            Ok(result) => {
                                if result.improved {
                                    ocr_improved.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    ocr_skipped.fetch_add(1, Ordering::Relaxed);
                                }
                                let _ = futures::executor::block_on(event_tx.send(
                                    OcrEvent::PageOcrCompleted {
                                        document_id: doc_id.clone(),
                                        page_number: page_num,
                                        improved: result.improved,
                                    },
                                ));

                                // Emit event when document is finalized during incremental processing
                                if result.document_finalized {
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
        }

        // Documents are finalized incrementally as their last page completes.
        // No separate Phase 3 needed.

        Ok(result)
    }

    /// Process a single document by ID.
    pub async fn process_single(
        &self,
        doc_id: &str,
        _event_tx: mpsc::Sender<OcrEvent>,
    ) -> anyhow::Result<()> {
        // Get the document
        let doc = self.doc_repo.get(doc_id)?
            .ok_or_else(|| anyhow::anyhow!("Document not found: {}", doc_id))?;

        println!("  {} Processing: {}", console::style("→").cyan(), doc.title);

        // Extract text per-page
        match extract_document_text_per_page(&doc, &self.doc_repo) {
            Ok(pages) => {
                println!("  {} Extracted {} pages", console::style("✓").green(), pages);
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

/// Extract text from a document per-page using pdftotext.
fn extract_document_text_per_page(
    doc: &Document,
    doc_repo: &DocumentRepository,
) -> anyhow::Result<usize> {
    let extractor = TextExtractor::new();

    let version = doc
        .current_version()
        .ok_or_else(|| anyhow::anyhow!("Document has no versions"))?;

    // Only process PDFs with per-page extraction
    if version.mime_type != "application/pdf" {
        // For non-PDFs, use the old extraction method
        let result = extractor.extract(&version.file_path, &version.mime_type)?;

        // Create a single "page" for non-PDF documents
        let mut page = DocumentPage::new(doc.id.clone(), version.id, 1);
        page.pdf_text = Some(result.text.clone());
        page.final_text = Some(result.text);
        page.ocr_status = PageOcrStatus::OcrComplete;
        doc_repo.save_page(&page)?;

        // Cache page count (1 for non-PDFs)
        let _ = doc_repo.set_version_page_count(version.id, 1);

        // Non-PDFs are complete immediately - finalize the document
        let _ = doc_repo.finalize_document(&doc.id);

        return Ok(1);
    }

    // Get page count (use cached value if available)
    let page_count = version.page_count.unwrap_or_else(|| {
        extractor
            .get_pdf_page_count(&version.file_path)
            .unwrap_or(1)
    });

    // Cache page count if not already cached
    if version.page_count.is_none() {
        let _ = doc_repo.set_version_page_count(version.id, page_count);
    }

    // Delete any existing pages for this document version (in case of re-processing)
    doc_repo.delete_pages(&doc.id, version.id)?;

    let mut pages_created = 0;

    for page_num in 1..=page_count {
        // Extract text using pdftotext
        let pdf_text = extractor
            .extract_pdf_page_text(&version.file_path, page_num)
            .unwrap_or_default();

        let mut page = DocumentPage::new(doc.id.clone(), version.id, page_num);
        page.pdf_text = Some(pdf_text);
        page.ocr_status = PageOcrStatus::TextExtracted;

        doc_repo.save_page(&page)?;
        pages_created += 1;
    }

    Ok(pages_created)
}

/// Result of OCR on a single page.
pub struct PageOcrResult {
    /// Whether the OCR text was better than the PDF text.
    pub improved: bool,
    /// Whether this page completion triggered document finalization.
    #[allow(dead_code)]
    pub document_finalized: bool,
}

/// Run OCR on a page and compare with existing text.
/// If all pages for this document are now complete, the document is finalized
/// (status set to OcrComplete, combined text saved).
fn ocr_document_page(
    page: &DocumentPage,
    doc_repo: &DocumentRepository,
) -> anyhow::Result<PageOcrResult> {
    let extractor = TextExtractor::new();

    // Get the document to find the file path
    let doc = doc_repo
        .get(&page.document_id)?
        .ok_or_else(|| anyhow::anyhow!("Document not found"))?;

    let version = doc
        .versions
        .iter()
        .find(|v| v.id == page.version_id)
        .ok_or_else(|| anyhow::anyhow!("Version not found"))?;

    // Run OCR on this page
    let mut updated_page = page.clone();
    let mut improved = false;

    match extractor.ocr_pdf_page(&version.file_path, page.page_number) {
        Ok(ocr_text) => {
            let ocr_chars = ocr_text.chars().filter(|c| !c.is_whitespace()).count();
            let pdf_chars = page
                .pdf_text
                .as_ref()
                .map(|t| t.chars().filter(|c| !c.is_whitespace()).count())
                .unwrap_or(0);

            // OCR is better if it has >20% more content
            improved = ocr_chars > pdf_chars + (pdf_chars / 5);

            updated_page.ocr_text = Some(ocr_text.clone());
            updated_page.ocr_status = PageOcrStatus::OcrComplete;
            updated_page.final_text = if improved {
                Some(ocr_text)
            } else {
                page.pdf_text.clone()
            };
        }
        Err(e) => {
            tracing::debug!(
                "OCR failed for page {}, using PDF text: {}",
                page.page_number,
                e
            );
            // Mark as failed but still set final_text to PDF text so document can be finalized
            updated_page.ocr_status = PageOcrStatus::Failed;
            updated_page.final_text = page.pdf_text.clone();
        }
    };

    doc_repo.save_page(&updated_page)?;

    // Check if all pages for this document are now complete, and if so, finalize it
    let mut document_finalized = false;
    if doc_repo.are_all_pages_complete(&page.document_id, page.version_id)?
        && doc_repo.finalize_document(&page.document_id)?
    {
        document_finalized = true;
        tracing::debug!(
            "Document {} finalized after page {} completed",
            page.document_id,
            page.page_number
        );
    }

    Ok(PageOcrResult {
        improved,
        document_finalized,
    })
}
