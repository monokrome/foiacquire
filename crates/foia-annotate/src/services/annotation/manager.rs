//! Annotation manager — generic batch orchestration for any `Annotator`.

use std::sync::Arc;

use tokio::sync::mpsc;

use foia::repository::DieselDocumentRepository;
use foia::work_queue::db_annotation::DbAnnotationQueue;
use foia::work_queue::{
    ExecutionStrategy, PipelineEvent, PipelineRunner, WorkFilter, WorkQueue,
};

use super::annotator::Annotator;
use super::stage::AnnotationStage;
use super::types::{AnnotationEvent, AnnotationOutput, BatchAnnotationResult};

/// Orchestrates batch annotation using a registered `Annotator`.
pub struct AnnotationManager {
    doc_repo: DieselDocumentRepository,
}

impl AnnotationManager {
    pub fn new(doc_repo: DieselDocumentRepository) -> Self {
        Self { doc_repo }
    }

    /// Build a WorkFilter from annotator metadata and optional source filter.
    fn build_filter(annotator: &dyn Annotator, source_id: Option<&str>) -> WorkFilter {
        WorkFilter {
            work_type: annotator.annotation_type().into(),
            source_id: source_id.map(Into::into),
            version: Some(annotator.version()),
            ..Default::default()
        }
    }

    /// Count documents that still need the given annotation.
    pub async fn count_needing(
        &self,
        annotator: &dyn Annotator,
        source_id: Option<&str>,
    ) -> anyhow::Result<u64> {
        let queue = DbAnnotationQueue::new(self.doc_repo.clone());
        let filter = Self::build_filter(annotator, source_id);
        Ok(queue.count(&filter).await?)
    }

    /// Run a batch of annotations, emitting events for progress tracking.
    ///
    /// The caller owns the event receiver and decides how to present progress
    /// (progress bars, log lines, etc.). This keeps the manager free of UI concerns.
    pub async fn run_batch(
        &self,
        annotator: Arc<dyn Annotator>,
        source_id: Option<&str>,
        limit: usize,
        chunk_size: Option<usize>,
        strategy: ExecutionStrategy,
        event_tx: mpsc::Sender<AnnotationEvent>,
    ) -> anyhow::Result<BatchAnnotationResult> {
        if !annotator.is_available().await {
            let _ = event_tx
                .send(AnnotationEvent::Complete {
                    succeeded: 0,
                    failed: 0,
                    skipped: 0,
                    remaining: 0,
                })
                .await;
            anyhow::bail!(
                "{} is not available: {}",
                annotator.display_name(),
                annotator.availability_hint()
            );
        }

        let queue = DbAnnotationQueue::new(self.doc_repo.clone());
        let filter = Self::build_filter(annotator.as_ref(), source_id);

        let total_count = queue.count(&filter).await?;

        if total_count == 0 {
            let _ = event_tx
                .send(AnnotationEvent::Complete {
                    succeeded: 0,
                    failed: 0,
                    skipped: 0,
                    remaining: 0,
                })
                .await;
            return Ok(BatchAnnotationResult {
                succeeded: 0,
                failed: 0,
                skipped: 0,
                remaining: 0,
            });
        }

        let effective_chunk = chunk_size.unwrap_or(4096);

        let stage = AnnotationStage::new(
            self.doc_repo.clone(),
            annotator.clone(),
            source_id,
        );

        let mut runner = PipelineRunner::new(effective_chunk, limit);
        runner.add_stage(Box::new(stage));

        // Bridge PipelineEvent -> AnnotationEvent
        let (pipe_tx, pipe_rx) = mpsc::channel::<PipelineEvent>(100);
        let bridge = tokio::spawn(bridge_pipeline_to_annotation_events(pipe_rx, event_tx));

        runner.run(strategy, pipe_tx).await?;

        let result = bridge.await?;
        Ok(result)
    }

    /// Process a single document by ID.
    pub async fn process_single(
        &self,
        annotator: &dyn Annotator,
        doc_id: &str,
        event_tx: mpsc::Sender<AnnotationEvent>,
    ) -> anyhow::Result<()> {
        let doc = self
            .doc_repo
            .get(doc_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Document not found: {}", doc_id))?;

        let _ = event_tx
            .send(AnnotationEvent::Started { total_documents: 1 })
            .await;

        let _ = event_tx
            .send(AnnotationEvent::DocumentStarted {
                document_id: doc.id.clone(),
                title: doc.title.clone(),
            })
            .await;

        match annotator.annotate(&doc, &self.doc_repo).await {
            Ok(output @ AnnotationOutput::Data(_)) => {
                let data = match &output {
                    AnnotationOutput::Data(d) => d.as_str(),
                    _ => unreachable!(),
                };
                self.doc_repo
                    .record_annotation(
                        &doc.id,
                        annotator.annotation_type(),
                        annotator.version(),
                        Some(data),
                        None,
                    )
                    .await?;
                if let Err(e) = annotator.post_record(&doc, &self.doc_repo, &output).await {
                    tracing::warn!("post_record failed for {}: {}", doc.id, e);
                }
                let _ = event_tx
                    .send(AnnotationEvent::DocumentCompleted {
                        document_id: doc.id.clone(),
                    })
                    .await;
            }
            Ok(output @ AnnotationOutput::NoResult) => {
                self.doc_repo
                    .record_annotation(
                        &doc.id,
                        annotator.annotation_type(),
                        annotator.version(),
                        Some("no_result"),
                        None,
                    )
                    .await?;
                if let Err(e) = annotator.post_record(&doc, &self.doc_repo, &output).await {
                    tracing::warn!("post_record failed for {}: {}", doc.id, e);
                }
                let _ = event_tx
                    .send(AnnotationEvent::DocumentCompleted {
                        document_id: doc.id.clone(),
                    })
                    .await;
            }
            Ok(AnnotationOutput::Skipped) => {
                let _ = event_tx
                    .send(AnnotationEvent::DocumentSkipped {
                        document_id: doc.id.clone(),
                    })
                    .await;
            }
            Err(e) => {
                let _ = event_tx
                    .send(AnnotationEvent::DocumentFailed {
                        document_id: doc.id.clone(),
                        error: e.to_string(),
                    })
                    .await;
                return Err(anyhow::anyhow!("{}", e));
            }
        }

        let _ = event_tx
            .send(AnnotationEvent::Complete {
                succeeded: 1,
                failed: 0,
                skipped: 0,
                remaining: 0,
            })
            .await;

        Ok(())
    }
}

/// Bridge generic `PipelineEvent`s to domain-specific `AnnotationEvent`s.
async fn bridge_pipeline_to_annotation_events(
    mut pipe_rx: mpsc::Receiver<PipelineEvent>,
    event_tx: mpsc::Sender<AnnotationEvent>,
) -> BatchAnnotationResult {
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;
    let mut remaining = 0u64;

    while let Some(event) = pipe_rx.recv().await {
        match event {
            PipelineEvent::StageStarted { total_items, .. } => {
                let _ = event_tx
                    .send(AnnotationEvent::Started {
                        total_documents: total_items as usize,
                    })
                    .await;
            }
            PipelineEvent::ItemStarted { item_id, label, .. } => {
                let _ = event_tx
                    .send(AnnotationEvent::DocumentStarted {
                        document_id: item_id,
                        title: label,
                    })
                    .await;
            }
            PipelineEvent::ItemCompleted { item_id, .. } => {
                succeeded += 1;
                let _ = event_tx
                    .send(AnnotationEvent::DocumentCompleted {
                        document_id: item_id,
                    })
                    .await;
            }
            PipelineEvent::ItemSkipped { item_id, .. } => {
                skipped += 1;
                let _ = event_tx
                    .send(AnnotationEvent::DocumentSkipped {
                        document_id: item_id,
                    })
                    .await;
            }
            PipelineEvent::ItemFailed { item_id, error, .. } => {
                failed += 1;
                let _ = event_tx
                    .send(AnnotationEvent::DocumentFailed {
                        document_id: item_id,
                        error,
                    })
                    .await;
            }
            PipelineEvent::StageCompleted {
                remaining: r, ..
            } => {
                remaining = r;
                let _ = event_tx
                    .send(AnnotationEvent::Complete {
                        succeeded,
                        failed,
                        skipped,
                        remaining,
                    })
                    .await;
            }
        }
    }

    BatchAnnotationResult {
        succeeded,
        failed,
        skipped,
        remaining,
    }
}
