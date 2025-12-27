//! In-memory cache for expensive stats queries.
//!
//! Provides TTL-based caching to avoid recomputing stats on every page load.
//! Stats change infrequently (only when documents are added/modified),
//! so a 5-minute TTL is reasonable.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Default TTL for cached stats (5 minutes).
/// Stats change infrequently (only when documents are added/modified),
/// so a longer TTL significantly improves performance.
const DEFAULT_TTL: Duration = Duration::from_secs(300);

/// A cached value with expiration time.
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

impl<T: Clone> CacheEntry<T> {
    fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    fn get(&self) -> Option<T> {
        if self.is_expired() {
            None
        } else {
            Some(self.value.clone())
        }
    }
}

/// Cache for document statistics.
#[allow(clippy::type_complexity)]
pub struct StatsCache {
    /// Type stats: category -> count
    type_stats: RwLock<Option<CacheEntry<Vec<(String, u64)>>>>,
    /// All tags with counts
    all_tags: RwLock<Option<CacheEntry<Vec<(String, usize)>>>>,
    /// Source counts: source_id -> count
    source_counts: RwLock<Option<CacheEntry<HashMap<String, u64>>>>,
    /// Browse counts: cache_key -> count (keyed by filter params)
    browse_counts: RwLock<HashMap<String, CacheEntry<u64>>>,
    /// TTL for cache entries
    ttl: Duration,
}

impl StatsCache {
    /// Create a new stats cache with default TTL.
    pub fn new() -> Self {
        Self {
            type_stats: RwLock::new(None),
            all_tags: RwLock::new(None),
            source_counts: RwLock::new(None),
            browse_counts: RwLock::new(HashMap::new()),
            ttl: DEFAULT_TTL,
        }
    }

    /// Create a new stats cache with custom TTL.
    #[allow(dead_code)]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            type_stats: RwLock::new(None),
            all_tags: RwLock::new(None),
            source_counts: RwLock::new(None),
            browse_counts: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Build a cache key for browse count from filter parameters.
    pub fn browse_count_key(
        source_id: Option<&str>,
        types: &[String],
        tags: &[String],
        query: Option<&str>,
    ) -> String {
        format!(
            "src:{}_types:{}_tags:{}_q:{}",
            source_id.unwrap_or("*"),
            types.join(","),
            tags.join(","),
            query.unwrap_or("")
        )
    }

    /// Get cached browse count, or None if expired/missing.
    pub fn get_browse_count(&self, key: &str) -> Option<u64> {
        self.browse_counts
            .read()
            .ok()
            .and_then(|guard| guard.get(key).and_then(|e| e.get()))
    }

    /// Set browse count in cache.
    pub fn set_browse_count(&self, key: String, count: u64) {
        if let Ok(mut guard) = self.browse_counts.write() {
            guard.insert(key, CacheEntry::new(count, self.ttl));
            // Prune expired entries occasionally (when cache grows large)
            if guard.len() > 100 {
                guard.retain(|_, entry| !entry.is_expired());
            }
        }
    }

    /// Get cached type stats, or None if expired/missing.
    #[allow(dead_code)]
    pub fn get_type_stats(&self) -> Option<Vec<(String, u64)>> {
        self.type_stats
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().and_then(|e| e.get()))
    }

    /// Set type stats in cache.
    #[allow(dead_code)]
    pub fn set_type_stats(&self, stats: Vec<(String, u64)>) {
        if let Ok(mut guard) = self.type_stats.write() {
            *guard = Some(CacheEntry::new(stats, self.ttl));
        }
    }

    /// Get cached tags, or None if expired/missing.
    pub fn get_all_tags(&self) -> Option<Vec<(String, usize)>> {
        self.all_tags
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().and_then(|e| e.get()))
    }

    /// Set tags in cache.
    pub fn set_all_tags(&self, tags: Vec<(String, usize)>) {
        if let Ok(mut guard) = self.all_tags.write() {
            *guard = Some(CacheEntry::new(tags, self.ttl));
        }
    }

    /// Get cached source counts, or None if expired/missing.
    pub fn get_source_counts(&self) -> Option<HashMap<String, u64>> {
        self.source_counts
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().and_then(|e| e.get()))
    }

    /// Set source counts in cache.
    pub fn set_source_counts(&self, counts: HashMap<String, u64>) {
        if let Ok(mut guard) = self.source_counts.write() {
            *guard = Some(CacheEntry::new(counts, self.ttl));
        }
    }

    /// Invalidate all cached stats (call when documents change).
    #[allow(dead_code)]
    pub fn invalidate(&self) {
        if let Ok(mut guard) = self.type_stats.write() {
            *guard = None;
        }
        if let Ok(mut guard) = self.all_tags.write() {
            *guard = None;
        }
        if let Ok(mut guard) = self.source_counts.write() {
            *guard = None;
        }
        if let Ok(mut guard) = self.browse_counts.write() {
            guard.clear();
        }
    }
}

impl Default for StatsCache {
    fn default() -> Self {
        Self::new()
    }
}
