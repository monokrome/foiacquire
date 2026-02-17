//! Database-backed work queue for the analysis pipeline.
//!
//! Wraps existing `DieselDocumentRepository` methods — no new queries.

use async_trait::async_trait;

use crate::models::Document;
use crate::repository::DieselDocumentRepository;

use super::handle::{ClaimId, WorkHandle};
use super::{WorkFilter, WorkQueue, WorkQueueError};

/// Work queue that discovers analysis work via DB queries and claims
/// items by inserting a `pending` row in `document_analysis_results`.
pub struct DbAnalysisQueue {
    repo: DieselDocumentRepository,
}

impl DbAnalysisQueue {
    pub fn new(repo: DieselDocumentRepository) -> Self {
        Self { repo }
    }
}

const DEFAULT_RETRY_HOURS: u32 = 12;

#[async_trait]
impl WorkQueue for DbAnalysisQueue {
    type Item = Document;

    async fn count(&self, filter: &WorkFilter) -> Result<u64, WorkQueueError> {
        let retry_hours = filter.retry_interval_hours.unwrap_or(DEFAULT_RETRY_HOURS);
        Ok(self
            .repo
            .count_needing_analysis(
                &filter.work_type,
                filter.source_id.as_deref(),
                filter.mime_type.as_deref(),
                retry_hours,
            )
            .await?)
    }

    async fn fetch_batch(
        &self,
        filter: &WorkFilter,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<Document>, WorkQueueError> {
        let retry_hours = filter.retry_interval_hours.unwrap_or(DEFAULT_RETRY_HOURS);
        Ok(self
            .repo
            .get_needing_analysis(
                &filter.work_type,
                limit,
                filter.source_id.as_deref(),
                filter.mime_type.as_deref(),
                cursor,
                retry_hours,
            )
            .await?)
    }

    async fn claim(
        &self,
        item: &Document,
        filter: &WorkFilter,
    ) -> Result<WorkHandle<Document>, WorkQueueError> {
        let version_id = item
            .current_version()
            .map(|v| v.id as i32)
            .ok_or_else(|| {
                WorkQueueError::NotFound(format!("no version for document {}", item.id))
            })?;

        self.repo
            .claim_analysis(&item.id, version_id, &filter.work_type)
            .await?;

        Ok(WorkHandle::new(item.clone(), ClaimId::None))
    }

    /// No-op: the analysis pipeline calls `store_analysis_result_for_document`
    /// which cleans up the pending claim row as part of the upsert.
    async fn complete(&self, handle: WorkHandle<Document>) -> Result<(), WorkQueueError> {
        handle.consume();
        Ok(())
    }

    /// No-op: the pending claim row expires after 90 minutes. The analysis
    /// pipeline records the error via `store_analysis_result_for_document`.
    async fn fail(
        &self,
        handle: WorkHandle<Document>,
        _error: &str,
        _requeue: bool,
    ) -> Result<(), WorkQueueError> {
        handle.consume();
        Ok(())
    }
}
