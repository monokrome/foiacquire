//! Annotation manager — generic batch orchestration for any `Annotator`.

use tokio::sync::mpsc;

use foia::repository::DieselDocumentRepository;

use super::annotator::Annotator;
use super::types::{AnnotationEvent, AnnotationOutput, BatchAnnotationResult};

/// Orchestrates batch annotation using a registered `Annotator`.
pub struct AnnotationManager {
    doc_repo: DieselDocumentRepository,
}

impl AnnotationManager {
    pub fn new(doc_repo: DieselDocumentRepository) -> Self {
        Self { doc_repo }
    }

    /// Count documents that still need the given annotation.
    pub async fn count_needing(
        &self,
        annotator: &dyn Annotator,
        source_id: Option<&str>,
    ) -> anyhow::Result<u64> {
        let count = self
            .doc_repo
            .count_documents_needing_annotation(
                annotator.annotation_type(),
                annotator.version(),
                source_id,
            )
            .await?;
        Ok(count)
    }

    /// Run a batch of annotations, emitting events for progress tracking.
    ///
    /// The caller owns the event receiver and decides how to present progress
    /// (progress bars, log lines, etc.). This keeps the manager free of UI concerns.
    pub async fn run_batch(
        &self,
        annotator: &dyn Annotator,
        source_id: Option<&str>,
        limit: usize,
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

        let total_count = self
            .doc_repo
            .count_documents_needing_annotation(
                annotator.annotation_type(),
                annotator.version(),
                source_id,
            )
            .await?;

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

        let effective_limit = if limit > 0 {
            limit
        } else {
            total_count as usize
        };

        let _ = event_tx
            .send(AnnotationEvent::Started {
                total_documents: effective_limit,
            })
            .await;

        let mut processed = 0usize;
        let mut succeeded = 0usize;
        let mut failed = 0usize;
        let mut skipped = 0usize;

        while processed < effective_limit {
            let batch_limit = (effective_limit - processed).min(10);
            let docs = self
                .doc_repo
                .get_documents_needing_annotation(
                    annotator.annotation_type(),
                    annotator.version(),
                    source_id,
                    batch_limit,
                )
                .await?;

            if docs.is_empty() {
                break;
            }

            for doc in docs {
                if processed >= effective_limit {
                    break;
                }

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
                        succeeded += 1;
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
                        succeeded += 1;
                    }
                    Ok(AnnotationOutput::Skipped) => {
                        let _ = event_tx
                            .send(AnnotationEvent::DocumentSkipped {
                                document_id: doc.id.clone(),
                            })
                            .await;
                        skipped += 1;
                    }
                    Err(e) => {
                        self.doc_repo
                            .record_annotation(
                                &doc.id,
                                annotator.annotation_type(),
                                annotator.version(),
                                None,
                                Some(&e.to_string()),
                            )
                            .await?;
                        let _ = event_tx
                            .send(AnnotationEvent::DocumentFailed {
                                document_id: doc.id.clone(),
                                error: e.to_string(),
                            })
                            .await;
                        failed += 1;
                    }
                }

                processed += 1;
            }
        }

        let remaining = self
            .doc_repo
            .count_documents_needing_annotation(
                annotator.annotation_type(),
                annotator.version(),
                source_id,
            )
            .await?;

        let _ = event_tx
            .send(AnnotationEvent::Complete {
                succeeded,
                failed,
                skipped,
                remaining,
            })
            .await;

        Ok(BatchAnnotationResult {
            succeeded,
            failed,
            skipped,
            remaining,
        })
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
