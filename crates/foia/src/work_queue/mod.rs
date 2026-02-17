//! Work queue abstraction for claim/complete/fail lifecycle.
//!
//! Both analysis and annotation pipelines follow the same pattern:
//! discover work, fetch a batch, claim each item, process, record outcome.
//! This module provides a shared trait so pipelines can be backend-agnostic
//! (DB polling today, message queues in the future).

mod error;
mod handle;
pub mod pipeline;
pub mod runner;

pub mod db_analysis;
pub mod db_annotation;

pub use error::WorkQueueError;
pub use handle::WorkHandle;
pub use pipeline::{ChunkResult, ExecutionStrategy, PipelineError, PipelineEvent, PipelineStage};
pub use runner::PipelineRunner;

use async_trait::async_trait;

/// Filter parameters for discovering work items.
#[derive(Debug, Clone, Default)]
pub struct WorkFilter {
    /// Analysis/annotation method name (e.g. "ocr", "llm_summary").
    pub work_type: String,
    /// Restrict to a specific source.
    pub source_id: Option<String>,
    /// Restrict to a specific MIME type.
    pub mime_type: Option<String>,
    /// Annotation schema version (used by annotation queues).
    pub version: Option<i32>,
    /// How long to wait before retrying failed items (hours). Default: 12.
    pub retry_interval_hours: Option<u32>,
}

/// A queue that manages the claim/complete/fail lifecycle for work items.
///
/// Implementations wrap a discovery mechanism (DB queries, message broker)
/// and provide distributed locking so multiple workers don't process the
/// same item concurrently.
///
/// Result storage is NOT part of this trait — pipelines write their own
/// results. The queue only manages the claim lifecycle.
#[async_trait]
pub trait WorkQueue: Send + Sync {
    /// The work item type returned by this queue.
    type Item: Send + Sync;

    /// Count items matching the filter that are available for processing.
    async fn count(&self, filter: &WorkFilter) -> Result<u64, WorkQueueError>;

    /// Fetch a batch of items available for processing.
    ///
    /// `cursor` enables pagination for DB backends; MQ backends ignore it.
    async fn fetch_batch(
        &self,
        filter: &WorkFilter,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<Self::Item>, WorkQueueError>;

    /// Claim an item for processing (distributed lock).
    ///
    /// Returns a `WorkHandle` that must be completed or failed.
    /// Returns `WorkQueueError::AlreadyClaimed` if another worker holds it.
    async fn claim(
        &self,
        item: &Self::Item,
        filter: &WorkFilter,
    ) -> Result<WorkHandle<Self::Item>, WorkQueueError>;

    /// Mark a claimed item as successfully processed.
    async fn complete(&self, handle: WorkHandle<Self::Item>) -> Result<(), WorkQueueError>;

    /// Mark a claimed item as failed.
    ///
    /// `requeue`: MQ backends use this for nack; DB backends ignore it
    /// (expiry handles retry).
    async fn fail(
        &self,
        handle: WorkHandle<Self::Item>,
        error: &str,
        requeue: bool,
    ) -> Result<(), WorkQueueError>;
}
