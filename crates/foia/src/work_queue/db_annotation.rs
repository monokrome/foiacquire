//! Database-backed work queue for the annotation pipeline.
//!
//! Wraps existing `DieselDocumentRepository` annotation queries and adds
//! distributed locking via `claim_analysis` with a prefixed type name
//! (e.g. `annotate:llm_summary`) to avoid collision with analysis claims.

use async_trait::async_trait;

use crate::models::Document;
use crate::repository::DieselDocumentRepository;

use super::handle::{ClaimId, WorkHandle};
use super::{WorkFilter, WorkQueue, WorkQueueError};

/// Work queue that discovers annotation work via metadata version checks
/// and claims items via `document_analysis_results` pending rows.
pub struct DbAnnotationQueue {
    repo: DieselDocumentRepository,
}

impl DbAnnotationQueue {
    pub fn new(repo: DieselDocumentRepository) -> Self {
        Self { repo }
    }

    /// Build the prefixed analysis type used for claim rows.
    /// Avoids collision with analysis pipeline claims.
    fn claim_type(work_type: &str) -> String {
        format!("annotate:{}", work_type)
    }
}

#[async_trait]
impl WorkQueue for DbAnnotationQueue {
    type Item = Document;

    async fn count(&self, filter: &WorkFilter) -> Result<u64, WorkQueueError> {
        let version = filter.version.unwrap_or(1);
        Ok(self
            .repo
            .count_documents_needing_annotation(
                &filter.work_type,
                version,
                filter.source_id.as_deref(),
            )
            .await?)
    }

    async fn fetch_batch(
        &self,
        filter: &WorkFilter,
        limit: usize,
        _cursor: Option<&str>,
    ) -> Result<Vec<Document>, WorkQueueError> {
        let version = filter.version.unwrap_or(1);
        Ok(self
            .repo
            .get_documents_needing_annotation(
                &filter.work_type,
                version,
                filter.source_id.as_deref(),
                limit,
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

        let claim_type = Self::claim_type(&filter.work_type);
        self.repo
            .claim_analysis(&item.id, version_id, &claim_type)
            .await?;

        let claim_id = ClaimId::DbPendingClaim {
            document_id: item.id.clone(),
            version_id,
            analysis_type: claim_type,
        };

        Ok(WorkHandle::new(item.clone(), claim_id))
    }

    /// Delete the pending claim row. Annotation results are stored in
    /// document metadata (not `document_analysis_results`), so the pending
    /// row must be explicitly cleaned up.
    async fn complete(&self, handle: WorkHandle<Document>) -> Result<(), WorkQueueError> {
        let (_item, claim_id) = handle.consume();
        if let ClaimId::DbPendingClaim {
            document_id,
            version_id,
            analysis_type,
        } = claim_id
        {
            self.repo
                .delete_pending_claim(&document_id, version_id, &analysis_type)
                .await?;
        }
        Ok(())
    }

    /// No-op: the pending claim row expires after 90 minutes.
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
