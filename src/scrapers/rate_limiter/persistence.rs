//! Database persistence for rate limit state.

use std::path::Path;
use std::time::Duration;

use rusqlite::{params, Connection};
use tracing::{debug, info};

use super::domain_state::DomainState;
use super::RateLimiter;

/// Open a database connection with proper concurrency settings.
fn open_db(db_path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 30000;
    "#,
    )?;
    Ok(conn)
}

/// Initialize the rate limit table in the database.
pub fn init_rate_limit_table(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS rate_limit_state (
            domain TEXT PRIMARY KEY,
            current_delay_ms INTEGER NOT NULL,
            in_backoff INTEGER NOT NULL DEFAULT 0,
            total_requests INTEGER NOT NULL DEFAULT 0,
            rate_limit_hits INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    "#,
    )?;
    Ok(())
}

/// Load rate limit state from database into a RateLimiter.
pub async fn load_rate_limit_state(limiter: &RateLimiter, db_path: &Path) -> anyhow::Result<usize> {
    let conn = open_db(db_path)?;
    init_rate_limit_table(&conn)?;

    let mut stmt = conn.prepare(
        "SELECT domain, current_delay_ms, in_backoff, total_requests, rate_limit_hits FROM rate_limit_state"
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)? as u64,
            row.get::<_, i32>(2)? != 0,
            row.get::<_, i64>(3)? as u64,
            row.get::<_, i64>(4)? as u64,
        ))
    })?;

    let mut domains = limiter.domains.write().await;
    let base_delay = limiter.config.base_delay;
    let mut count = 0;

    for row in rows {
        let (domain, delay_ms, in_backoff, total_requests, rate_limit_hits) = row?;

        // Only load domains that are still in backoff (have meaningful state)
        if in_backoff || delay_ms > base_delay.as_millis() as u64 {
            let state = DomainState {
                current_delay: Duration::from_millis(delay_ms),
                last_request: None, // Can't restore Instant from DB
                consecutive_successes: 0,
                recent_403s: Vec::new(),
                in_backoff,
                total_requests,
                rate_limit_hits,
            };
            info!(
                "Restored rate limit state for {}: delay={}ms, in_backoff={}",
                domain, delay_ms, in_backoff
            );
            domains.insert(domain, state);
            count += 1;
        }
    }

    if count > 0 {
        info!(
            "Loaded rate limit state for {} domains from database",
            count
        );
    }

    Ok(count)
}

/// Save rate limit state to database.
pub async fn save_rate_limit_state(limiter: &RateLimiter, db_path: &Path) -> anyhow::Result<usize> {
    let conn = open_db(db_path)?;
    init_rate_limit_table(&conn)?;

    let domains = limiter.domains.read().await;
    let base_delay = limiter.config.base_delay;
    let mut count = 0;

    for (domain, state) in domains.iter() {
        // Only save domains with non-default state
        if state.in_backoff || state.current_delay > base_delay {
            conn.execute(
                r#"INSERT OR REPLACE INTO rate_limit_state
                   (domain, current_delay_ms, in_backoff, total_requests, rate_limit_hits, updated_at)
                   VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"#,
                params![
                    domain,
                    state.current_delay.as_millis() as i64,
                    state.in_backoff as i32,
                    state.total_requests as i64,
                    state.rate_limit_hits as i64,
                ],
            )?;
            count += 1;
        }
    }

    // Clean up old entries that are no longer in backoff
    conn.execute(
        "DELETE FROM rate_limit_state WHERE in_backoff = 0 AND current_delay_ms <= ?",
        params![base_delay.as_millis() as i64],
    )?;

    if count > 0 {
        debug!("Saved rate limit state for {} domains to database", count);
    }

    Ok(count)
}

/// Save state for a single domain (call after rate limit events).
pub async fn save_domain_state(
    limiter: &RateLimiter,
    domain: &str,
    db_path: &Path,
) -> anyhow::Result<()> {
    let domains = limiter.domains.read().await;
    let base_delay = limiter.config.base_delay;

    if let Some(state) = domains.get(domain) {
        if state.in_backoff || state.current_delay > base_delay {
            let conn = open_db(db_path)?;
            init_rate_limit_table(&conn)?;

            conn.execute(
                r#"INSERT OR REPLACE INTO rate_limit_state
                   (domain, current_delay_ms, in_backoff, total_requests, rate_limit_hits, updated_at)
                   VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"#,
                params![
                    domain,
                    state.current_delay.as_millis() as i64,
                    state.in_backoff as i32,
                    state.total_requests as i64,
                    state.rate_limit_hits as i64,
                ],
            )?;
        }
    }

    Ok(())
}
