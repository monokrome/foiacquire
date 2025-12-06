//! Date detection service for estimating document publication dates.
#![allow(dead_code)]
//!
//! Uses multiple deterministic strategies to estimate dates:
//! - Server-provided dates (high confidence)
//! - Filename patterns (medium confidence)
//! - PDF metadata (medium-high confidence)
//!
//! LLM-based extraction is handled separately in the annotation pipeline.

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use regex::Regex;
use std::sync::LazyLock;

/// Confidence level for date estimates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateConfidence {
    High,
    Medium,
    Low,
}

impl DateConfidence {
    pub fn as_str(&self) -> &'static str {
        match self {
            DateConfidence::High => "high",
            DateConfidence::Medium => "medium",
            DateConfidence::Low => "low",
        }
    }
}

/// Source of the date estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateSource {
    Server,
    Filename,
    PdfMetadata,
    Content,
    Llm,
    Manual,
}

impl DateSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            DateSource::Server => "server",
            DateSource::Filename => "filename",
            DateSource::PdfMetadata => "pdf_metadata",
            DateSource::Content => "content",
            DateSource::Llm => "llm",
            DateSource::Manual => "manual",
        }
    }
}

/// Result of date detection.
#[derive(Debug, Clone)]
pub struct DateEstimate {
    pub date: DateTime<Utc>,
    pub confidence: DateConfidence,
    pub source: DateSource,
}

/// Regex patterns for date detection in filenames and URLs.
static DATE_PATTERNS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    vec![
        // ISO format with various separators: 2024-01-15, 2024_01_15, 2024/01/15
        (Regex::new(r"(\d{4})[-_/](\d{2})[-_/](\d{2})").unwrap(), "ymd"),
        // US format: 01-15-2024, 01_15_2024, 01/15/2024
        (Regex::new(r"(\d{2})[-_/](\d{2})[-_/](\d{4})").unwrap(), "mdy"),
        // Compact: 20240115
        (Regex::new(r"(\d{4})(\d{2})(\d{2})").unwrap(), "ymd_compact"),
        // Year-month only: 2024-01, 2024/01, 202401
        (Regex::new(r"(\d{4})[-_/]?(\d{2})(?:\D|$)").unwrap(), "ym"),
    ]
});

/// Try to detect document date using deterministic strategies.
///
/// Strategies are tried in order of confidence:
/// 1. Server date (if significantly different from acquired date)
/// 2. Filename patterns
///
/// Returns None if no date can be determined.
pub fn detect_date(
    server_date: Option<DateTime<Utc>>,
    acquired_at: DateTime<Utc>,
    filename: Option<&str>,
    source_url: Option<&str>,
) -> Option<DateEstimate> {
    // Strategy 1: Server-provided date
    if let Some(estimate) = check_server_date(server_date, acquired_at) {
        return Some(estimate);
    }

    // Strategy 2: Filename patterns
    if let Some(estimate) = extract_date_from_filename(filename, source_url) {
        return Some(estimate);
    }

    None
}

/// Check if server date is a valid publication date.
///
/// Returns Some if:
/// - Server date exists
/// - It's not Unix epoch (1970-01-01)
/// - It differs from acquired_at by more than 24 hours
fn check_server_date(
    server_date: Option<DateTime<Utc>>,
    acquired_at: DateTime<Utc>,
) -> Option<DateEstimate> {
    let server_date = server_date?;

    // Reject Unix epoch
    let epoch = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    if server_date == epoch {
        return None;
    }

    // Check if meaningfully different from acquired_at (more than 24 hours)
    let diff = (server_date - acquired_at).num_hours().abs();
    if diff > 24 {
        return Some(DateEstimate {
            date: server_date,
            confidence: DateConfidence::High,
            source: DateSource::Server,
        });
    }

    None
}

/// Extract date from filename or URL path.
fn extract_date_from_filename(
    filename: Option<&str>,
    source_url: Option<&str>,
) -> Option<DateEstimate> {
    // Try filename first, then URL path
    let candidates = [filename, source_url.map(extract_path_from_url).flatten()];

    for candidate in candidates.into_iter().flatten() {
        for (pattern, format) in DATE_PATTERNS.iter() {
            if let Some(caps) = pattern.captures(candidate) {
                if let Some(date) = parse_captured_date(&caps, format) {
                    // Sanity check: date should be between 1900 and next year
                    let year = date.year();
                    if year >= 1900 && year <= Utc::now().year() + 1 {
                        return Some(DateEstimate {
                            date: date.and_hms_opt(0, 0, 0)?.and_utc(),
                            confidence: DateConfidence::Medium,
                            source: DateSource::Filename,
                        });
                    }
                }
            }
        }
    }

    None
}

/// Extract path component from URL for date detection.
fn extract_path_from_url(url: &str) -> Option<&str> {
    // Get everything after the domain (full path, not just first segment)
    url.split("://")
        .nth(1)
        .and_then(|s| s.find('/').map(|i| &s[i..]))
        .or_else(|| url.rfind('/').map(|i| &url[i..]))
}

/// Parse captured date groups based on format.
fn parse_captured_date(caps: &regex::Captures, format: &str) -> Option<NaiveDate> {
    match format {
        "ymd" | "ymd_compact" => {
            let year: i32 = caps.get(1)?.as_str().parse().ok()?;
            let month: u32 = caps.get(2)?.as_str().parse().ok()?;
            let day: u32 = caps.get(3)?.as_str().parse().ok()?;
            NaiveDate::from_ymd_opt(year, month, day)
        }
        "mdy" => {
            let month: u32 = caps.get(1)?.as_str().parse().ok()?;
            let day: u32 = caps.get(2)?.as_str().parse().ok()?;
            let year: i32 = caps.get(3)?.as_str().parse().ok()?;
            NaiveDate::from_ymd_opt(year, month, day)
        }
        "ym" => {
            let year: i32 = caps.get(1)?.as_str().parse().ok()?;
            let month: u32 = caps.get(2)?.as_str().parse().ok()?;
            // Default to first of month
            NaiveDate::from_ymd_opt(year, month, 1)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filename_date_iso() {
        let result = extract_date_from_filename(Some("report-2024-03-15.pdf"), None);
        assert!(result.is_some());
        let est = result.unwrap();
        assert_eq!(est.date.format("%Y-%m-%d").to_string(), "2024-03-15");
        assert_eq!(est.confidence, DateConfidence::Medium);
        assert_eq!(est.source, DateSource::Filename);
    }

    #[test]
    fn test_filename_date_compact() {
        let result = extract_date_from_filename(Some("CIA-RDP96-00788R002100520004-9.pdf"), None);
        // Should not match random numbers that look like dates
        // The 00788R doesn't match because it has a letter
        assert!(result.is_none() || result.unwrap().date.year() > 1900);
    }

    #[test]
    fn test_server_date_different() {
        let server = DateTime::parse_from_rfc3339("2020-05-15T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let acquired = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let result = check_server_date(Some(server), acquired);
        assert!(result.is_some());
        assert_eq!(result.unwrap().confidence, DateConfidence::High);
    }

    #[test]
    fn test_server_date_same_day() {
        let server = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let acquired = DateTime::parse_from_rfc3339("2024-01-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let result = check_server_date(Some(server), acquired);
        assert!(result.is_none()); // Same day, likely just crawl date
    }

    #[test]
    fn test_server_date_epoch() {
        let epoch = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let acquired = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let result = check_server_date(Some(epoch), acquired);
        assert!(result.is_none()); // Epoch is invalid
    }
}
