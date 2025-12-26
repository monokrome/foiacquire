//! Per-domain rate limiting state.

use std::time::{Duration, Instant};

use super::config::{RATE_LIMIT_403_THRESHOLD, RATE_LIMIT_WINDOW};

/// State for a single domain.
#[derive(Debug, Clone)]
pub struct DomainState {
    /// Current delay for this domain.
    pub current_delay: Duration,
    /// Last request time.
    pub last_request: Option<Instant>,
    /// Consecutive successes since last rate limit.
    pub consecutive_successes: u32,
    /// Recent 403 responses: (timestamp, url) for pattern detection.
    /// Only triggers rate limit if multiple unique URLs get 403 in a short window.
    pub recent_403s: Vec<(Instant, String)>,
    /// Whether currently in backoff.
    pub in_backoff: bool,
    /// Total requests made.
    pub total_requests: u64,
    /// Total rate limit hits.
    pub rate_limit_hits: u64,
}

impl DomainState {
    pub fn new(base_delay: Duration) -> Self {
        Self {
            current_delay: base_delay,
            last_request: None,
            consecutive_successes: 0,
            recent_403s: Vec::new(),
            in_backoff: false,
            total_requests: 0,
            rate_limit_hits: 0,
        }
    }

    /// Add a 403 response, returns true if this triggers rate limit detection.
    pub fn add_403(&mut self, url: &str) -> bool {
        let now = Instant::now();
        let cutoff = now - RATE_LIMIT_WINDOW;

        // Binary search for cutoff point (list is chronologically sorted since we always append)
        let cutoff_idx = self
            .recent_403s
            .binary_search_by(|(time, _)| {
                if *time < cutoff {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            })
            .unwrap_or_else(|i| i);

        // Remove old entries by slicing
        if cutoff_idx > 0 {
            self.recent_403s.drain(0..cutoff_idx);
        }

        // Add new entry (even if URL already exists - we want to track timing)
        self.recent_403s.push((now, url.to_string()));

        // Count unique URLs in the window
        self.unique_403_count() >= RATE_LIMIT_403_THRESHOLD
    }

    /// Count unique URLs that received 403 in the current window.
    pub fn unique_403_count(&self) -> usize {
        let mut unique_urls: Vec<&str> = self.recent_403s.iter().map(|(_, u)| u.as_str()).collect();
        unique_urls.sort();
        unique_urls.dedup();
        unique_urls.len()
    }

    /// Clear 403 tracking (on success or confirmed rate limit).
    pub fn clear_403_tracking(&mut self) {
        self.recent_403s.clear();
    }

    /// Get stats about recent 403s for debugging.
    /// Returns (unique_url_count, time_span_of_window).
    pub fn get_403_stats(&self) -> (usize, Duration) {
        if self.recent_403s.is_empty() {
            return (0, Duration::ZERO);
        }

        // Since list is chronologically sorted, first entry is oldest, last is newest
        let oldest_time = self.recent_403s.first().map(|(t, _)| *t);
        let newest_time = self.recent_403s.last().map(|(t, _)| *t);

        let time_span = match (oldest_time, newest_time) {
            (Some(oldest), Some(newest)) => newest.duration_since(oldest),
            _ => Duration::ZERO,
        };

        (self.unique_403_count(), time_span)
    }

    /// Time until this domain is ready for another request.
    pub fn time_until_ready(&self) -> Duration {
        match self.last_request {
            Some(last) => {
                let elapsed = last.elapsed();
                if elapsed >= self.current_delay {
                    Duration::ZERO
                } else {
                    self.current_delay - elapsed
                }
            }
            None => Duration::ZERO,
        }
    }

    /// Check if this domain is ready for a request now.
    pub fn is_ready(&self) -> bool {
        self.time_until_ready() == Duration::ZERO
    }
}
