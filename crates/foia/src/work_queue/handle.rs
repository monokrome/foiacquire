//! Work handle — wraps a claimed work item with its claim identifier.

/// Opaque claim identifier used by queue backends to track ownership.
#[derive(Debug)]
pub(crate) enum ClaimId {
    /// Row ID in `document_analysis_results` (pending claim row).
    #[allow(dead_code)]
    DbRow(i64),
    /// Pending claim that needs explicit cleanup.
    /// Stores (document_id, version_id, analysis_type) for `delete_pending_claim`.
    DbPendingClaim {
        document_id: String,
        version_id: i32,
        analysis_type: String,
    },
    /// Message delivery tag (for future AMQP backends).
    #[allow(dead_code)]
    DeliveryTag(u64),
    /// No external identifier (claim uses upsert with no returned ID).
    None,
}

/// A claimed work item. Move semantics: consumed by `complete()` or `fail()`.
///
/// If dropped without being consumed, logs a warning. The DB's 90-minute
/// claim expiry is the real safety net — the warning is for debugging only.
pub struct WorkHandle<T: Send + Sync> {
    pub item: T,
    pub(crate) claim_id: ClaimId,
    pub(crate) consumed: bool,
}

impl<T: Send + Sync> WorkHandle<T> {
    pub(crate) fn new(item: T, claim_id: ClaimId) -> Self {
        Self {
            item,
            claim_id,
            consumed: false,
        }
    }

    /// Mark this handle as consumed (called internally by complete/fail).
    pub(crate) fn consume(mut self) -> (T, ClaimId) {
        self.consumed = true;
        // Safety: we set consumed=true before returning, so Drop won't warn.
        // We need to move item and claim_id out. Use ManuallyDrop-like pattern
        // via unsafe to avoid double-drop — but since we have ownership and
        // set consumed=true, the Drop impl will be a no-op.
        let item = unsafe { std::ptr::read(&self.item) };
        let claim_id = unsafe { std::ptr::read(&self.claim_id) };
        std::mem::forget(self);
        (item, claim_id)
    }
}

impl<T: Send + Sync> Drop for WorkHandle<T> {
    fn drop(&mut self) {
        if !self.consumed {
            tracing::warn!(
                "WorkHandle dropped without being completed or failed — \
                 claim will expire after 90 minutes"
            );
        }
    }
}
