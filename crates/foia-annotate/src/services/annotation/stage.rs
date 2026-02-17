//! Pipeline stage implementation for annotation.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex};

use foia::repository::DieselDocumentRepository;
use foia::work_queue::db_annotation::DbAnnotationQueue;
use foia::work_queue::{
    ChunkResult, PipelineError, PipelineEvent, PipelineStage, WorkFilter, WorkQueue,
    WorkQueueError,
};

use super::annotator::Annotator;
use super::types::AnnotationOutput;

/// Annotation pipeline stage — runs a single `Annotator` against documents.
pub struct AnnotationStage {
    queue: DbAnnotationQueue,
    doc_repo: DieselDocumentRepository,
    annotator: Arc<dyn Annotator>,
    filter: WorkFilter,
    cursor: Mutex<Option<String>>,
}

impl AnnotationStage {
    pub fn new(
        doc_repo: DieselDocumentRepository,
        annotator: Arc<dyn Annotator>,
        source_id: Option<&str>,
    ) -> Self {
        let queue = DbAnnotationQueue::new(doc_repo.clone());
        let filter = WorkFilter {
            work_type: annotator.annotation_type().into(),
            source_id: source_id.map(Into::into),
            version: Some(annotator.version()),
            ..Default::default()
        };
        Self {
            queue,
            doc_repo,
            annotator,
            filter,
            cursor: Mutex::new(None),
        }
    }
}

#[async_trait]
impl PipelineStage for AnnotationStage {
    fn name(&self) -> &str {
        self.annotator.display_name()
    }

    fn is_deferred(&self) -> bool {
        self.annotator.is_deferred()
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

        let docs = self
            .queue
            .fetch_batch(&self.filter, batch_limit, None)
            .await?;

        if docs.is_empty() {
            return Ok(ChunkResult::default());
        }

        let has_more = docs.len() >= batch_limit;
        let mut succeeded = 0usize;
        let mut failed = 0usize;
        let mut skipped = 0usize;
        let stage_name = self.name().to_string();

        for doc in &docs {
            // Claim the document
            let work_handle = match self.queue.claim(doc, &self.filter).await {
                Ok(h) => h,
                Err(WorkQueueError::AlreadyClaimed) => {
                    skipped += 1;
                    continue;
                }
                Err(e) => {
                    tracing::warn!("Failed to claim {}: {}", doc.id, e);
                    continue;
                }
            };

            let _ = event_tx
                .send(PipelineEvent::ItemStarted {
                    stage: stage_name.clone(),
                    item_id: doc.id.clone(),
                    label: doc.title.clone(),
                })
                .await;

            match self.annotator.annotate(doc, &self.doc_repo).await {
                Ok(output @ AnnotationOutput::Data(_)) => {
                    let data = match &output {
                        AnnotationOutput::Data(d) => d.as_str(),
                        _ => unreachable!(),
                    };
                    if let Err(e) = self
                        .doc_repo
                        .record_annotation(
                            &doc.id,
                            self.annotator.annotation_type(),
                            self.annotator.version(),
                            Some(data),
                            None,
                        )
                        .await
                    {
                        tracing::warn!("Failed to record annotation for {}: {}", doc.id, e);
                        let _ = self.queue.fail(work_handle, &e.to_string(), false).await;
                        let _ = event_tx
                            .send(PipelineEvent::ItemFailed {
                                stage: stage_name.clone(),
                                item_id: doc.id.clone(),
                                error: e.to_string(),
                            })
                            .await;
                        failed += 1;
                        continue;
                    }
                    if let Err(e) = self.annotator.post_record(doc, &self.doc_repo, &output).await {
                        tracing::warn!("post_record failed for {}: {}", doc.id, e);
                    }
                    let _ = self.queue.complete(work_handle).await;
                    let _ = event_tx
                        .send(PipelineEvent::ItemCompleted {
                            stage: stage_name.clone(),
                            item_id: doc.id.clone(),
                            detail: None,
                        })
                        .await;
                    succeeded += 1;
                }
                Ok(output @ AnnotationOutput::NoResult) => {
                    let _ = self
                        .doc_repo
                        .record_annotation(
                            &doc.id,
                            self.annotator.annotation_type(),
                            self.annotator.version(),
                            Some("no_result"),
                            None,
                        )
                        .await;
                    if let Err(e) = self.annotator.post_record(doc, &self.doc_repo, &output).await {
                        tracing::warn!("post_record failed for {}: {}", doc.id, e);
                    }
                    let _ = self.queue.complete(work_handle).await;
                    let _ = event_tx
                        .send(PipelineEvent::ItemCompleted {
                            stage: stage_name.clone(),
                            item_id: doc.id.clone(),
                            detail: None,
                        })
                        .await;
                    succeeded += 1;
                }
                Ok(AnnotationOutput::Skipped) => {
                    let _ = self.queue.complete(work_handle).await;
                    let _ = event_tx
                        .send(PipelineEvent::ItemSkipped {
                            stage: stage_name.clone(),
                            item_id: doc.id.clone(),
                        })
                        .await;
                    skipped += 1;
                }
                Err(e) => {
                    let _ = self
                        .doc_repo
                        .record_annotation(
                            &doc.id,
                            self.annotator.annotation_type(),
                            self.annotator.version(),
                            None,
                            Some(&e.to_string()),
                        )
                        .await;
                    let _ = self.queue.fail(work_handle, &e.to_string(), false).await;
                    let _ = event_tx
                        .send(PipelineEvent::ItemFailed {
                            stage: stage_name.clone(),
                            item_id: doc.id.clone(),
                            error: e.to_string(),
                        })
                        .await;
                    failed += 1;
                }
            }
        }

        Ok(ChunkResult {
            succeeded,
            failed,
            skipped,
            has_more,
        })
    }
}
