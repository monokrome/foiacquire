//! Database persistence for rate limit state.
//!
//! This module is currently stubbed out pending Diesel migration.
//! Rate limiting still works in-memory, but state is not persisted across runs.

use std::path::Path;

use super::RateLimiter;

/// Load rate limit state from database into a RateLimiter.
///
/// Currently stubbed - returns 0 (no domains loaded).
pub async fn load_rate_limit_state(
    _limiter: &RateLimiter,
    _db_path: &Path,
) -> anyhow::Result<usize> {
    // TODO: Implement with Diesel once migration is complete
    Ok(0)
}

/// Save rate limit state to database.
///
/// Currently stubbed - returns 0 (no domains saved).
pub async fn save_rate_limit_state(
    _limiter: &RateLimiter,
    _db_path: &Path,
) -> anyhow::Result<usize> {
    // TODO: Implement with Diesel once migration is complete
    Ok(0)
}

/// Save state for a single domain (call after rate limit events).
///
/// Currently stubbed - no-op.
pub async fn save_domain_state(
    _limiter: &RateLimiter,
    _domain: &str,
    _db_path: &Path,
) -> anyhow::Result<()> {
    // TODO: Implement with Diesel once migration is complete
    Ok(())
}
