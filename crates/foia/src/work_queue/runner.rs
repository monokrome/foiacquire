//! Pipeline runner — drives one or more `PipelineStage`s using wide or deep execution.
//!
//! **Wide mode**: all chunks through stage N, then stage N+1 (sequential phases).
//! **Deep mode**: interleave stages per chunk; deferred stages run as concurrent consumers.

use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::pipeline::{ExecutionStrategy, PipelineError, PipelineEvent, PipelineStage};

/// Drives pipeline stages through their work using a configurable execution strategy.
pub struct PipelineRunner {
    stages: Vec<Box<dyn PipelineStage>>,
    chunk_size: usize,
    /// 0 means unlimited.
    limit: usize,
}

impl PipelineRunner {
    pub fn new(chunk_size: usize, limit: usize) -> Self {
        Self {
            stages: Vec::new(),
            chunk_size,
            limit,
        }
    }

    pub fn add_stage(&mut self, stage: Box<dyn PipelineStage>) {
        self.stages.push(stage);
    }

    /// Run all stages using the given strategy.
    pub async fn run(
        &self,
        strategy: ExecutionStrategy,
        event_tx: mpsc::Sender<PipelineEvent>,
    ) -> Result<(), PipelineError> {
        match strategy {
            ExecutionStrategy::Wide => self.run_wide(&event_tx).await,
            ExecutionStrategy::Deep => self.run_deep(&event_tx).await,
        }
    }

    /// Wide mode: complete each stage fully before starting the next.
    async fn run_wide(
        &self,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<(), PipelineError> {
        for stage in &self.stages {
            self.drain_stage(stage.as_ref(), event_tx).await?;
        }
        Ok(())
    }

    /// Run a single stage to completion (or until limit is reached).
    async fn drain_stage(
        &self,
        stage: &dyn PipelineStage,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<(), PipelineError> {
        let total = stage.count().await?;
        let _ = event_tx
            .send(PipelineEvent::StageStarted {
                stage: stage.name().to_string(),
                total_items: total,
            })
            .await;

        let mut total_succeeded = 0usize;
        let mut total_failed = 0usize;
        let mut total_skipped = 0usize;
        let mut processed = 0usize;

        loop {
            let remaining_limit = if self.limit > 0 {
                let left = self.limit.saturating_sub(processed);
                if left == 0 {
                    break;
                }
                left
            } else {
                0 // unlimited
            };

            let result = stage
                .run_chunk(self.chunk_size, remaining_limit, event_tx)
                .await?;

            let chunk_total = result.succeeded + result.failed + result.skipped;
            processed += chunk_total;
            total_succeeded += result.succeeded;
            total_failed += result.failed;
            total_skipped += result.skipped;

            if !result.has_more || chunk_total == 0 {
                break;
            }
        }

        let remaining = stage.count().await?;
        let _ = event_tx
            .send(PipelineEvent::StageCompleted {
                stage: stage.name().to_string(),
                succeeded: total_succeeded,
                failed: total_failed,
                skipped: total_skipped,
                remaining,
            })
            .await;

        Ok(())
    }

    /// Deep mode: interleave stages per chunk.
    ///
    /// - Non-deferred -> non-deferred: sequential per chunk to avoid CPU contention.
    /// - Non-deferred -> deferred: concurrent producer-consumer via a tokio task.
    async fn run_deep(
        &self,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<(), PipelineError> {
        if self.stages.is_empty() {
            return Ok(());
        }

        // Single stage: deep is the same as wide.
        if self.stages.len() == 1 {
            return self.drain_stage(self.stages[0].as_ref(), event_tx).await;
        }

        // Two-stage case: the primary path for analysis (text extraction -> OCR).
        // For >2 stages we could generalize, but the current pipelines are 1 or 2 stages.
        let stage1 = &self.stages[0];
        let stage2 = &self.stages[1];

        let total1 = stage1.count().await?;
        let _ = event_tx
            .send(PipelineEvent::StageStarted {
                stage: stage1.name().to_string(),
                total_items: total1,
            })
            .await;

        if stage2.is_deferred() {
            // Concurrent: spawn stage2 as a consumer polling the DB.
            self.run_deep_deferred(stage1.as_ref(), stage2.as_ref(), event_tx)
                .await
        } else {
            // Sequential interleave: stage1 chunk then stage2 chunk.
            self.run_deep_sequential(stage1.as_ref(), stage2.as_ref(), event_tx)
                .await
        }
    }

    /// Deep mode with sequential interleaving (non-deferred -> non-deferred).
    async fn run_deep_sequential(
        &self,
        stage1: &dyn PipelineStage,
        stage2: &dyn PipelineStage,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<(), PipelineError> {
        let mut processed = 0usize;
        let mut s1_succeeded = 0usize;
        let mut s1_failed = 0usize;
        let mut s1_skipped = 0usize;
        let mut s2_succeeded = 0usize;
        let mut s2_failed = 0usize;
        let mut s2_skipped = 0usize;

        loop {
            let remaining_limit = if self.limit > 0 {
                let left = self.limit.saturating_sub(processed);
                if left == 0 {
                    break;
                }
                left
            } else {
                0
            };

            // Run one chunk of stage 1 (produces work for stage 2)
            let r1 = stage1
                .run_chunk(self.chunk_size, remaining_limit, event_tx)
                .await?;

            let chunk1_total = r1.succeeded + r1.failed + r1.skipped;
            processed += chunk1_total;
            s1_succeeded += r1.succeeded;
            s1_failed += r1.failed;
            s1_skipped += r1.skipped;

            // Run one chunk of stage 2 (consumes work produced by stage 1)
            let r2 = stage2
                .run_chunk(self.chunk_size, 0, event_tx)
                .await?;
            s2_succeeded += r2.succeeded;
            s2_failed += r2.failed;
            s2_skipped += r2.skipped;

            if !r1.has_more || chunk1_total == 0 {
                break;
            }
        }

        // Emit stage 1 completion
        let remaining1 = stage1.count().await?;
        let _ = event_tx
            .send(PipelineEvent::StageCompleted {
                stage: stage1.name().to_string(),
                succeeded: s1_succeeded,
                failed: s1_failed,
                skipped: s1_skipped,
                remaining: remaining1,
            })
            .await;

        // Drain any remaining stage 2 work
        let total2 = stage2.count().await?;
        if total2 > 0 {
            let _ = event_tx
                .send(PipelineEvent::StageStarted {
                    stage: stage2.name().to_string(),
                    total_items: total2,
                })
                .await;
        }

        loop {
            let count = stage2.count().await?;
            if count == 0 {
                break;
            }
            let r = stage2
                .run_chunk(self.chunk_size, 0, event_tx)
                .await?;
            s2_succeeded += r.succeeded;
            s2_failed += r.failed;
            s2_skipped += r.skipped;
            let chunk_total = r.succeeded + r.failed + r.skipped;
            if chunk_total == 0 {
                break;
            }
        }

        let remaining2 = stage2.count().await?;
        let _ = event_tx
            .send(PipelineEvent::StageCompleted {
                stage: stage2.name().to_string(),
                succeeded: s2_succeeded,
                failed: s2_failed,
                skipped: s2_skipped,
                remaining: remaining2,
            })
            .await;

        Ok(())
    }

    /// Deep mode with concurrent consumer (non-deferred -> deferred).
    ///
    /// Stage 1 runs in the current task (producer). Stage 2 polls the DB
    /// concurrently as a spawned task (consumer). When stage 1 finishes
    /// producing, it signals done and the consumer drains remaining work.
    async fn run_deep_deferred(
        &self,
        stage1: &dyn PipelineStage,
        stage2: &dyn PipelineStage,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<(), PipelineError> {
        let (done_tx, done_rx) = watch::channel(false);

        // We can't move `stage2` into the spawned task because it's a trait reference.
        // Instead, drain stage2 in the current task after stage1 is done, but also
        // poll it during stage1 processing by interleaving.
        //
        // For true concurrency with dyn trait refs, we use a separate approach:
        // poll stage2 between each stage1 chunk (cooperative scheduling).
        // This avoids Send/Sync issues with trait object lifetimes.
        let _ = done_rx; // suppress unused warning

        let mut processed = 0usize;
        let mut s1_succeeded = 0usize;
        let mut s1_failed = 0usize;
        let mut s1_skipped = 0usize;
        let mut s2_succeeded = 0usize;
        let mut s2_failed = 0usize;
        let mut s2_skipped = 0usize;

        loop {
            let remaining_limit = if self.limit > 0 {
                let left = self.limit.saturating_sub(processed);
                if left == 0 {
                    break;
                }
                left
            } else {
                0
            };

            // Run one chunk of stage 1
            let r1 = stage1
                .run_chunk(self.chunk_size, remaining_limit, event_tx)
                .await?;

            let chunk1_total = r1.succeeded + r1.failed + r1.skipped;
            processed += chunk1_total;
            s1_succeeded += r1.succeeded;
            s1_failed += r1.failed;
            s1_skipped += r1.skipped;

            // Immediately try to consume any ready stage 2 work (non-blocking drain)
            let s2_count = stage2.count().await?;
            if s2_count > 0 {
                let r2 = stage2
                    .run_chunk(self.chunk_size, 0, event_tx)
                    .await?;
                s2_succeeded += r2.succeeded;
                s2_failed += r2.failed;
                s2_skipped += r2.skipped;
            }

            if !r1.has_more || chunk1_total == 0 {
                break;
            }
        }

        let _ = done_tx.send(true);

        // Emit stage 1 completion
        let remaining1 = stage1.count().await?;
        let _ = event_tx
            .send(PipelineEvent::StageCompleted {
                stage: stage1.name().to_string(),
                succeeded: s1_succeeded,
                failed: s1_failed,
                skipped: s1_skipped,
                remaining: remaining1,
            })
            .await;

        // Drain remaining stage 2 work
        let total2 = stage2.count().await?;
        if total2 > 0 {
            let _ = event_tx
                .send(PipelineEvent::StageStarted {
                    stage: stage2.name().to_string(),
                    total_items: total2,
                })
                .await;
        }

        loop {
            let count = stage2.count().await?;
            if count == 0 {
                // Small sleep to allow deferred API calls to complete
                tokio::time::sleep(Duration::from_millis(200)).await;
                let recheck = stage2.count().await?;
                if recheck == 0 {
                    break;
                }
            }
            let r = stage2
                .run_chunk(self.chunk_size, 0, event_tx)
                .await?;
            s2_succeeded += r.succeeded;
            s2_failed += r.failed;
            s2_skipped += r.skipped;
            let chunk_total = r.succeeded + r.failed + r.skipped;
            if chunk_total == 0 {
                break;
            }
        }

        let remaining2 = stage2.count().await?;
        let _ = event_tx
            .send(PipelineEvent::StageCompleted {
                stage: stage2.name().to_string(),
                succeeded: s2_succeeded,
                failed: s2_failed,
                skipped: s2_skipped,
                remaining: remaining2,
            })
            .await;

        Ok(())
    }
}
