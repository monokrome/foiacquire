//! Pipeline stage abstraction and shared types for batch processing pipelines.
//!
//! Both analysis and annotation pipelines follow the same pattern:
//! count work, fetch batch, claim, process, complete/fail, track progress, repeat.
//! This module provides the trait and event types so any pipeline stage
//! can be driven by the generic `PipelineRunner`.

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;

use super::WorkQueueError;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Work queue error: {0}")]
    WorkQueue(#[from] WorkQueueError),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

/// Cumulative result of a single `run_chunk` call.
#[derive(Debug, Default)]
pub struct ChunkResult {
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
    /// Whether there is more work remaining after this chunk.
    pub has_more: bool,
}

/// Progress events emitted by stages and forwarded by the runner.
///
/// These are generic enough that callers can bridge them to their own
/// domain-specific event types (e.g., `AnalysisEvent`, `AnnotationEvent`).
#[derive(Debug, Clone)]
pub enum PipelineEvent {
    StageStarted {
        stage: String,
        total_items: u64,
    },
    ItemStarted {
        stage: String,
        item_id: String,
        label: String,
    },
    ItemCompleted {
        stage: String,
        item_id: String,
        detail: Option<String>,
    },
    ItemSkipped {
        stage: String,
        item_id: String,
    },
    ItemFailed {
        stage: String,
        item_id: String,
        error: String,
    },
    StageCompleted {
        stage: String,
        succeeded: usize,
        failed: usize,
        skipped: usize,
        remaining: u64,
    },
}

/// Execution strategy for multi-stage pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ExecutionStrategy {
    /// All chunks through stage N, then stage N+1 (current behavior).
    #[default]
    Wide,
    /// Each chunk through all stages; deferred stages run concurrently.
    Deep,
}

/// A self-contained processing stage.
///
/// Each stage owns its data source and processing logic.
/// The runner only calls `count()` and `run_chunk()`.
#[async_trait]
pub trait PipelineStage: Send + Sync {
    /// Human-readable name for progress output.
    fn name(&self) -> &str;

    /// Whether this stage sends work to a remote API (true) vs. local CPU (false).
    /// In deep mode, deferred stages run as concurrent consumers.
    fn is_deferred(&self) -> bool;

    /// Count items currently available for processing.
    async fn count(&self) -> Result<u64, PipelineError>;

    /// Process one chunk of work.
    ///
    /// - `chunk_size`: maximum items to fetch in this chunk
    /// - `remaining_limit`: how many more items the runner will allow (0 = unlimited)
    /// - `event_tx`: channel for emitting progress events
    async fn run_chunk(
        &self,
        chunk_size: usize,
        remaining_limit: usize,
        event_tx: &mpsc::Sender<PipelineEvent>,
    ) -> Result<ChunkResult, PipelineError>;
}
