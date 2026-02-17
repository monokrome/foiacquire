//! Pipeline stage implementations for analysis: text extraction and OCR.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};

use foia::config::OcrConfig;
use foia::repository::DieselDocumentRepository;
use foia::work_queue::db_analysis::DbAnalysisQueue;
use foia::work_queue::{
    ChunkResult, PipelineError, PipelineEvent, PipelineStage, WorkFilter, WorkQueue,
    WorkQueueError,
};

use crate::ocr::OcrBackendType;
use super::processing::{
    detect_mime_mismatch, extract_document_text_per_page, ocr_document_page_with_config,
};

/// Text extraction stage (Phase 0 MIME check + Phase 1 extraction merged).
///
/// For each document:
/// 1. Inline MIME check (< 1ms) — fixes mismatches before extraction
/// 2. Claim via work queue
/// 3. Extract text per page using pdftotext / generic extractors
pub struct TextExtractionStage {
    queue: DbAnalysisQueue,
    doc_repo: DieselDocumentRepository,
    documents_dir: PathBuf,
    filter: WorkFilter,
    workers: usize,
    cursor: Mutex<Option<String>>,
}

impl TextExtractionStage {
    pub fn new(
        doc_repo: DieselDocumentRepository,
        documents_dir: PathBuf,
        source_id: Option<&str>,
        mime_type: Option<&str>,
        retry_interval_hours: u32,
        workers: usize,
    ) -> Self {
        let queue = DbAnalysisQueue::new(doc_repo.clone());
        let filter = WorkFilter {
            work_type: "ocr".into(),
            source_id: source_id.map(Into::into),
            mime_type: mime_type.map(Into::into),
            retry_interval_hours: Some(retry_interval_hours),
            ..Default::default()
        };
        Self {
            queue,
            doc_repo,
            documents_dir,
            filter,
            workers,
            cursor: Mutex::new(None),
        }
    }
}

#[async_trait]
impl PipelineStage for TextExtractionStage {
    fn name(&self) -> &str {
        "Text extraction"
    }

    fn is_deferred(&self) -> bool {
        false
    }

    async fn count(&self) -> Result<u64, PipelineError> {
        Ok(self.queue.count(&self.filter).await?)
    }

    async fn run_chunk(
        &self,
        chunk_size: usize,
        remaining_limit: usize,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<ChunkResult, PipelineError> {
        let batch_limit = if remaining_limit > 0 {
            chunk_size.min(remaining_limit)
        } else {
            chunk_size
        };

        let cursor = self.cursor.lock().await.clone();
        let docs = self
            .queue
            .fetch_batch(&self.filter, batch_limit, cursor.as_deref())
            .await?;

        if docs.is_empty() {
            return Ok(ChunkResult::default());
        }

        // Advance cursor
        if let Some(last) = docs.last() {
            *self.cursor.lock().await = Some(last.id.clone());
        }

        let succeeded = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let skipped = Arc::new(AtomicUsize::new(0));
        let has_more = docs.len() >= batch_limit;

        let mut handles = Vec::with_capacity(docs.len().min(self.workers));
        let stage_name = self.name().to_string();

        for doc in &docs {
            // Inline MIME check (was Phase 0)
            if let Some(version) = doc.current_version() {
                let path =
                    version.resolve_path(&self.documents_dir, &doc.source_url, &doc.title);
                if path.exists() {
                    if let Some((detected, _old)) =
                        detect_mime_mismatch(&path, &version.mime_type)
                    {
                        let _ = self
                            .doc_repo
                            .update_version_mime_type(version.id, &detected)
                            .await;
                    }
                }
            }

            // Skip documents whose files aren't on disk
            let file_available = doc.current_version().is_some_and(|v| {
                let path = v.resolve_path(&self.documents_dir, &doc.source_url, &doc.title);
                std::fs::metadata(&path).is_ok()
            });

            if !file_available {
                skipped.fetch_add(1, Ordering::Relaxed);
                let _ = event_tx
                    .send(PipelineEvent::ItemSkipped {
                        stage: stage_name.clone(),
                        item_id: doc.id.clone(),
                    })
                    .await;
                continue;
            }

            // Claim the document
            let work_handle = match self.queue.claim(doc, &self.filter).await {
                Ok(h) => h,
                Err(WorkQueueError::AlreadyClaimed) => continue,
                Err(e) => {
                    tracing::warn!("Failed to claim {}: {}", doc.id, e);
                    continue;
                }
            };
            // Consume immediately — analysis pipeline manages its own result storage
            let _ = self.queue.complete(work_handle).await;

            let doc = doc.clone();
            let doc_repo = self.doc_repo.clone();
            let documents_dir = self.documents_dir.clone();
            let succeeded = succeeded.clone();
            let failed = failed.clone();
            let event_tx = event_tx.clone();
            let stage_name = stage_name.clone();

            let handle = tokio::task::spawn_blocking(move || {
                let doc_id = doc.id.clone();
                let title = doc.title.clone();

                let _ = futures::executor::block_on(event_tx.send(PipelineEvent::ItemStarted {
                    stage: stage_name.clone(),
                    item_id: doc_id.clone(),
                    label: title,
                }));

                let rt_handle = tokio::runtime::Handle::current();

                match extract_document_text_per_page(&doc, &doc_repo, &rt_handle, &documents_dir) {
                    Ok(page_count) => {
                        succeeded.fetch_add(1, Ordering::Relaxed);
                        let _ = futures::executor::block_on(event_tx.send(
                            PipelineEvent::ItemCompleted {
                                stage: stage_name,
                                item_id: doc_id,
                                detail: Some(format!("{} pages", page_count)),
                            },
                        ));
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if !err_str.contains("Unsupported file type") {
                            tracing::warn!("Text extraction failed for {}: {}", doc.title, e);
                            failed.fetch_add(1, Ordering::Relaxed);
                            let _ = futures::executor::block_on(event_tx.send(
                                PipelineEvent::ItemFailed {
                                    stage: stage_name,
                                    item_id: doc_id,
                                    error: err_str,
                                },
                            ));
                        }
                    }
                }
            });

            handles.push(handle);

            if handles.len() >= self.workers {
                for h in handles.drain(..) {
                    if let Err(e) = h.await {
                        tracing::error!("Text extraction worker panicked: {}", e);
                    }
                }
            }
        }

        for h in handles {
            if let Err(e) = h.await {
                tracing::error!("Text extraction worker panicked: {}", e);
            }
        }

        Ok(ChunkResult {
            succeeded: succeeded.load(Ordering::Relaxed),
            failed: failed.load(Ordering::Relaxed),
            skipped: skipped.load(Ordering::Relaxed),
            has_more,
        })
    }
}

/// OCR stage — runs configured OCR backends on pages that need it.
///
/// Queries pages directly (not through WorkQueue) since pages have their own
/// query methods in the repository.
pub struct OcrStage {
    doc_repo: DieselDocumentRepository,
    ocr_config: OcrConfig,
    documents_dir: PathBuf,
    workers: usize,
    deferred: bool,
}

impl OcrStage {
    pub fn new(
        doc_repo: DieselDocumentRepository,
        ocr_config: OcrConfig,
        documents_dir: PathBuf,
        workers: usize,
    ) -> Self {
        // Determine if the primary OCR backend is deferred (cloud API)
        let deferred = ocr_config
            .backends
            .first()
            .map(|entry| {
                let names = entry.backends();
                names.first().map_or(false, |name| {
                    OcrBackendType::from_str(name)
                        .map_or(false, |t| t.is_deferred())
                })
            })
            .unwrap_or(false);

        Self {
            doc_repo,
            ocr_config,
            documents_dir,
            workers,
            deferred,
        }
    }
}

#[async_trait]
impl PipelineStage for OcrStage {
    fn name(&self) -> &str {
        "OCR"
    }

    fn is_deferred(&self) -> bool {
        self.deferred
    }

    async fn count(&self) -> Result<u64, PipelineError> {
        let n = self
            .doc_repo
            .count_pages_needing_ocr()
            .await
            .map_err(|e| PipelineError::Other(e.into()))?;
        Ok(n as u64)
    }

    async fn run_chunk(
        &self,
        chunk_size: usize,
        _remaining_limit: usize,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<ChunkResult, PipelineError> {
        let pages = self
            .doc_repo
            .get_all_pages_needing_ocr(chunk_size)
            .await
            .map_err(|e| PipelineError::Other(e.into()))?;

        if pages.is_empty() {
            return Ok(ChunkResult::default());
        }

        let has_more = pages.len() >= chunk_size;

        let succeeded = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let skipped = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(pages.len().min(self.workers));
        let stage_name = self.name().to_string();

        for page in pages {
            let doc_repo = self.doc_repo.clone();
            let ocr_config = self.ocr_config.clone();
            let documents_dir = self.documents_dir.clone();
            let succeeded = succeeded.clone();
            let failed = failed.clone();
            let skipped = skipped.clone();
            let event_tx = event_tx.clone();
            let stage_name = stage_name.clone();

            let handle = tokio::task::spawn_blocking(move || {
                let item_id = format!("{}:p{}", page.document_id, page.page_number);

                let _ = futures::executor::block_on(event_tx.send(PipelineEvent::ItemStarted {
                    stage: stage_name.clone(),
                    item_id: item_id.clone(),
                    label: format!("page {}", page.page_number),
                }));

                let rt_handle = tokio::runtime::Handle::current();

                match ocr_document_page_with_config(
                    &page,
                    &doc_repo,
                    &rt_handle,
                    &ocr_config,
                    &documents_dir,
                ) {
                    Ok(ocr_result) => {
                        if ocr_result.improved {
                            succeeded.fetch_add(1, Ordering::Relaxed);
                        } else {
                            skipped.fetch_add(1, Ordering::Relaxed);
                        }
                        let detail = if ocr_result.document_finalized {
                            Some("document finalized".to_string())
                        } else {
                            None
                        };
                        let _ = futures::executor::block_on(event_tx.send(
                            PipelineEvent::ItemCompleted {
                                stage: stage_name,
                                item_id,
                                detail,
                            },
                        ));
                    }
                    Err(e) => {
                        tracing::debug!("OCR failed for page {}: {}", page.page_number, e);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let _ = futures::executor::block_on(event_tx.send(
                            PipelineEvent::ItemFailed {
                                stage: stage_name,
                                item_id,
                                error: e.to_string(),
                            },
                        ));
                    }
                }
            });

            handles.push(handle);

            if handles.len() >= self.workers {
                for h in handles.drain(..) {
                    if let Err(e) = h.await {
                        tracing::error!("OCR worker panicked: {}", e);
                    }
                }
            }
        }

        for h in handles {
            if let Err(e) = h.await {
                tracing::error!("OCR worker panicked: {}", e);
            }
        }

        Ok(ChunkResult {
            succeeded: succeeded.load(Ordering::Relaxed),
            failed: failed.load(Ordering::Relaxed),
            skipped: skipped.load(Ordering::Relaxed),
            has_more,
        })
    }
}
