//! Document annotation service.
//!
//! Handles document annotation using LLM (generates synopsis and tags).
//! Separated from UI concerns - emits events for progress tracking.

use tokio::sync::mpsc;

use crate::llm::{LlmClient, LlmConfig};
use crate::models::DocumentStatus;
use crate::repository::DieselDocumentRepository;

/// Events emitted during annotation processing.
/// Fields are populated when events are created, even if consumers don't read all of them.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AnnotationEvent {
    /// Annotation started
    Started { total_documents: usize },
    /// Document annotation started
    DocumentStarted { document_id: String, title: String },
    /// Document annotation completed
    DocumentCompleted { document_id: String },
    /// Document annotation failed
    DocumentFailed { document_id: String, error: String },
    /// Document skipped (no text)
    DocumentSkipped { document_id: String },
    /// Annotation complete
    Complete {
        succeeded: usize,
        failed: usize,
        remaining: u64,
    },
}

/// Result of annotation processing.
/// Part of public API - consumers may use any field even if current CLI doesn't read all.
#[derive(Debug)]
#[allow(dead_code)]
pub struct AnnotationResult {
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
    pub remaining: u64,
}

/// Service for annotating documents with LLM.
pub struct AnnotationService {
    doc_repo: DieselDocumentRepository,
    llm_client: LlmClient,
}

impl AnnotationService {
    /// Create a new annotation service.
    pub fn new(doc_repo: DieselDocumentRepository, llm_config: LlmConfig) -> Self {
        let llm_client = LlmClient::new(llm_config);
        Self {
            doc_repo,
            llm_client,
        }
    }

    /// Check if LLM service is available.
    pub async fn is_available(&self) -> bool {
        self.llm_client.is_available().await
    }

    /// Get count of documents needing annotation.
    pub async fn count_needing_annotation(&self, source_id: Option<&str>) -> anyhow::Result<u64> {
        Ok(self.doc_repo.count_needing_summarization(source_id).await?)
    }

    /// Annotate documents.
    pub async fn annotate(
        &self,
        source_id: Option<&str>,
        limit: usize,
        event_tx: mpsc::Sender<AnnotationEvent>,
    ) -> anyhow::Result<AnnotationResult> {
        let total_count = self.doc_repo.count_needing_summarization(source_id).await?;

        if total_count == 0 {
            let _ = event_tx
                .send(AnnotationEvent::Complete {
                    succeeded: 0,
                    failed: 0,
                    remaining: 0,
                })
                .await;

            return Ok(AnnotationResult {
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

        let mut processed = 0;
        let mut succeeded = 0;
        let mut failed = 0;
        let mut skipped = 0;

        // Process documents one at a time (sequentially to avoid LLM memory pressure)
        while processed < effective_limit {
            let batch_limit = (effective_limit - processed).min(10);
            let docs = self.doc_repo.get_needing_summarization(batch_limit).await?;

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

                // Get text from pages
                let version_id = match doc.current_version() {
                    Some(v) => v.id,
                    None => {
                        let _ = event_tx
                            .send(AnnotationEvent::DocumentSkipped {
                                document_id: doc.id.clone(),
                            })
                            .await;
                        skipped += 1;
                        processed += 1;
                        continue;
                    }
                };

                let text = match self
                    .doc_repo
                    .get_combined_page_text(&doc.id, version_id as i32)
                    .await
                {
                    Ok(Some(t)) if !t.is_empty() => t,
                    _ => {
                        let _ = event_tx
                            .send(AnnotationEvent::DocumentSkipped {
                                document_id: doc.id.clone(),
                            })
                            .await;
                        skipped += 1;
                        processed += 1;
                        continue;
                    }
                };

                // Run summarization
                match self.llm_client.summarize(&text, &doc.title).await {
                    Ok(result) => {
                        // Update document with synopsis and tags
                        let mut updated_doc = doc.clone();
                        updated_doc.synopsis = Some(result.synopsis);
                        updated_doc.tags = result.tags;
                        updated_doc.status = DocumentStatus::Indexed;
                        updated_doc.updated_at = chrono::Utc::now();

                        if let Err(e) = self.doc_repo.save(&updated_doc).await {
                            let _ = event_tx
                                .send(AnnotationEvent::DocumentFailed {
                                    document_id: doc.id.clone(),
                                    error: format!("Save failed: {}", e),
                                })
                                .await;
                            failed += 1;
                        } else {
                            let _ = event_tx
                                .send(AnnotationEvent::DocumentCompleted {
                                    document_id: doc.id.clone(),
                                })
                                .await;
                            succeeded += 1;
                        }
                    }
                    Err(e) => {
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

        let remaining = self.doc_repo.count_needing_summarization(source_id).await?;

        let _ = event_tx
            .send(AnnotationEvent::Complete {
                succeeded,
                failed,
                remaining,
            })
            .await;

        Ok(AnnotationResult {
            succeeded,
            failed,
            skipped,
            remaining,
        })
    }

    /// Process a single document by ID.
    pub async fn process_single(
        &self,
        doc_id: &str,
        event_tx: mpsc::Sender<AnnotationEvent>,
    ) -> anyhow::Result<()> {
        // Get the document
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

        // Get version ID
        let version_id = match doc.current_version() {
            Some(v) => v.id,
            None => {
                println!("  {} No version found", console::style("!").yellow());
                let _ = event_tx
                    .send(AnnotationEvent::DocumentSkipped {
                        document_id: doc.id.clone(),
                    })
                    .await;
                return Ok(());
            }
        };

        // Get combined text from pages
        let text = match self
            .doc_repo
            .get_combined_page_text(doc_id, version_id as i32)
            .await
        {
            Ok(Some(t)) if !t.is_empty() => t,
            _ => {
                println!(
                    "  {} No text available for annotation",
                    console::style("!").yellow()
                );
                let _ = event_tx
                    .send(AnnotationEvent::DocumentSkipped {
                        document_id: doc.id.clone(),
                    })
                    .await;
                return Ok(());
            }
        };

        println!(
            "  {} Generating annotation for: {}",
            console::style("→").cyan(),
            doc.title
        );

        // Generate annotation
        match self.llm_client.summarize(&text, &doc.title).await {
            Ok(result) => {
                // Update document with synopsis and tags
                let mut updated_doc = doc.clone();
                updated_doc.synopsis = Some(result.synopsis.clone());
                updated_doc.tags = result.tags.clone();
                updated_doc.status = DocumentStatus::Indexed;
                updated_doc.updated_at = chrono::Utc::now();

                self.doc_repo.save(&updated_doc).await?;

                println!(
                    "  {} Synopsis: {}",
                    console::style("✓").green(),
                    result.synopsis.chars().take(100).collect::<String>()
                );
                if !result.tags.is_empty() {
                    println!(
                        "  {} Tags: {}",
                        console::style("✓").green(),
                        result.tags.join(", ")
                    );
                }

                let _ = event_tx
                    .send(AnnotationEvent::DocumentCompleted {
                        document_id: doc.id.clone(),
                    })
                    .await;
            }
            Err(e) => {
                println!("  {} Failed: {}", console::style("✗").red(), e);
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
                remaining: 0,
            })
            .await;

        Ok(())
    }
}
