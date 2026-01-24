//! Redis-backed rate limiter for distributed multi-process coordination.
//!
//! Uses Redis for atomic operations and automatic expiration of rate limit data.

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};

use super::backend::{DomainRateState, RateLimitBackend, RateLimitError, RateLimitResult};

/// Key prefix for rate limit data in Redis.
const KEY_PREFIX: &str = "foiacquire:ratelimit:";
/// TTL for domain state keys (auto-cleanup of stale domains).
const DOMAIN_TTL_SECS: u64 = 86400; // 24 hours
/// TTL for 403 tracking keys.
const FORBIDDEN_TTL_SECS: u64 = 300; // 5 minutes

/// Redis-backed rate limit storage.
/// Uses atomic Lua scripts for concurrent access.
pub struct RedisRateLimitBackend {
    conn: ConnectionManager,
    base_delay_ms: u64,
}

impl RedisRateLimitBackend {
    /// Create a new Redis rate limit backend.
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection URL (e.g., "redis://localhost:6379")
    /// * `base_delay_ms` - Default delay between requests in milliseconds
    pub async fn new(redis_url: &str, base_delay_ms: u64) -> RateLimitResult<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| RateLimitError::Database(format!("Redis connection error: {}", e)))?;

        let conn = ConnectionManager::new(client).await.map_err(|e| {
            RateLimitError::Database(format!("Redis connection manager error: {}", e))
        })?;

        Ok(Self {
            conn,
            base_delay_ms,
        })
    }

    /// Get the Redis key for a domain's state.
    fn domain_key(&self, domain: &str) -> String {
        format!("{}domain:{}", KEY_PREFIX, domain)
    }

    /// Get the Redis key for a domain's 403 tracking set.
    fn forbidden_key(&self, domain: &str) -> String {
        format!("{}403:{}", KEY_PREFIX, domain)
    }
}

#[async_trait]
impl RateLimitBackend for RedisRateLimitBackend {
    async fn get_or_create_domain(
        &self,
        domain: &str,
        base_delay_ms: u64,
    ) -> RateLimitResult<DomainRateState> {
        let mut conn = self.conn.clone();
        let key = self.domain_key(domain);

        // Try to get existing state
        let result: Option<Vec<Option<String>>> = redis::cmd("HMGET")
            .arg(&key)
            .arg("current_delay_ms")
            .arg("last_request_at")
            .arg("consecutive_successes")
            .arg("in_backoff")
            .arg("total_requests")
            .arg("rate_limit_hits")
            .query_async(&mut conn)
            .await
            .map_err(|e| RateLimitError::Database(e.to_string()))?;

        if let Some(fields) = result {
            if fields.iter().any(|f| f.is_some()) {
                // Parse existing state
                let current_delay_ms: u64 = fields[0]
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(base_delay_ms);
                let last_request_at: Option<i64> = fields[1].as_ref().and_then(|s| s.parse().ok());
                let consecutive_successes: u32 =
                    fields[2].as_ref().and_then(|s| s.parse().ok()).unwrap_or(0);
                let in_backoff: bool = fields[3].as_ref().map(|s| s == "1").unwrap_or(false);
                let total_requests: u64 =
                    fields[4].as_ref().and_then(|s| s.parse().ok()).unwrap_or(0);
                let rate_limit_hits: u64 =
                    fields[5].as_ref().and_then(|s| s.parse().ok()).unwrap_or(0);

                return Ok(DomainRateState {
                    domain: domain.to_string(),
                    current_delay_ms,
                    last_request_at,
                    consecutive_successes,
                    in_backoff,
                    total_requests,
                    rate_limit_hits,
                });
            }
        }

        // Create new state
        let state = DomainRateState::new(domain.to_string(), base_delay_ms);
        self.update_domain(&state).await?;
        Ok(state)
    }

    async fn update_domain(&self, state: &DomainRateState) -> RateLimitResult<()> {
        let mut conn = self.conn.clone();
        let key = self.domain_key(&state.domain);

        redis::pipe()
            .hset(&key, "current_delay_ms", state.current_delay_ms.to_string())
            .hset(
                &key,
                "last_request_at",
                state
                    .last_request_at
                    .map(|t| t.to_string())
                    .unwrap_or_default(),
            )
            .hset(
                &key,
                "consecutive_successes",
                state.consecutive_successes.to_string(),
            )
            .hset(&key, "in_backoff", if state.in_backoff { "1" } else { "0" })
            .hset(&key, "total_requests", state.total_requests.to_string())
            .hset(&key, "rate_limit_hits", state.rate_limit_hits.to_string())
            .expire(&key, DOMAIN_TTL_SECS as i64)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| RateLimitError::Database(e.to_string()))?;

        Ok(())
    }

    async fn acquire(&self, domain: &str, base_delay_ms: u64) -> RateLimitResult<Duration> {
        let mut conn = self.conn.clone();
        let key = self.domain_key(domain);

        // Lua script for atomic acquire operation
        // Returns: wait_time_ms (0 if ready now)
        let script = Script::new(
            r#"
            local key = KEYS[1]
            local now_ms = tonumber(ARGV[1])
            local base_delay_ms = tonumber(ARGV[2])
            local ttl = tonumber(ARGV[3])

            -- Get current state
            local current_delay = tonumber(redis.call('HGET', key, 'current_delay_ms')) or base_delay_ms
            local last_request = tonumber(redis.call('HGET', key, 'last_request_at')) or 0

            -- Calculate wait time
            local elapsed = now_ms - last_request
            local wait_time = 0
            if elapsed < current_delay then
                wait_time = current_delay - elapsed
            end

            -- Update last_request_at and increment total_requests
            local request_time = now_ms + wait_time
            redis.call('HSET', key, 'last_request_at', request_time)
            redis.call('HINCRBY', key, 'total_requests', 1)
            redis.call('HSETNX', key, 'current_delay_ms', base_delay_ms)
            redis.call('EXPIRE', key, ttl)

            return wait_time
        "#,
        );

        let now_ms = chrono::Utc::now().timestamp_millis();

        let wait_time_ms: i64 = script
            .key(&key)
            .arg(now_ms)
            .arg(base_delay_ms as i64)
            .arg(DOMAIN_TTL_SECS as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| RateLimitError::Database(e.to_string()))?;

        Ok(Duration::from_millis(wait_time_ms.max(0) as u64))
    }

    async fn record_403(&self, domain: &str, url: &str) -> RateLimitResult<()> {
        let mut conn = self.conn.clone();
        let key = self.forbidden_key(domain);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Use sorted set with timestamp as score for automatic ordering
        // Member is "timestamp:url" to allow same URL at different times
        let member = format!("{}:{}", now_ms, url);

        redis::pipe()
            .zadd(&key, member, now_ms as f64)
            .expire(&key, FORBIDDEN_TTL_SECS as i64)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| RateLimitError::Database(e.to_string()))?;

        Ok(())
    }

    async fn get_403_count(&self, domain: &str, window_ms: u64) -> RateLimitResult<usize> {
        let mut conn = self.conn.clone();
        let key = self.forbidden_key(domain);
        let cutoff_ms = chrono::Utc::now().timestamp_millis() - window_ms as i64;

        // Get all entries within the window
        let entries: Vec<String> = conn
            .zrangebyscore(&key, cutoff_ms as f64, "+inf")
            .await
            .map_err(|e| RateLimitError::Database(e.to_string()))?;

        // Extract unique URLs (entries are "timestamp:url")
        let mut unique_urls: Vec<&str> = entries
            .iter()
            .filter_map(|e| e.split_once(':').map(|(_, url)| url))
            .collect();
        unique_urls.sort();
        unique_urls.dedup();

        Ok(unique_urls.len())
    }

    async fn clear_403s(&self, domain: &str) -> RateLimitResult<()> {
        let mut conn = self.conn.clone();
        let key = self.forbidden_key(domain);

        conn.del::<_, ()>(&key)
            .await
            .map_err(|e| RateLimitError::Database(e.to_string()))?;

        Ok(())
    }

    async fn cleanup_expired_403s(&self, window_ms: u64) -> RateLimitResult<u64> {
        // Redis handles expiration automatically via TTL
        // But we can clean up old entries from sorted sets
        let mut conn = self.conn.clone();
        let cutoff_ms = chrono::Utc::now().timestamp_millis() - window_ms as i64;

        // Get all 403 keys and remove old entries
        let pattern = format!("{}403:*", KEY_PREFIX);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| RateLimitError::Database(e.to_string()))?;

        let mut total_removed = 0u64;
        for key in keys {
            let removed: i64 = conn
                .zrembyscore(&key, "-inf", cutoff_ms as f64)
                .await
                .map_err(|e| RateLimitError::Database(e.to_string()))?;
            total_removed += removed as u64;
        }

        Ok(total_removed)
    }
}

impl Clone for RedisRateLimitBackend {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            base_delay_ms: self.base_delay_ms,
        }
    }
}
