//! Wayback Machine (web.archive.org) archive source.
//!
//! Uses the CDX API to query snapshot metadata efficiently without
//! downloading actual content. The CDX API returns tab-separated data
//! with fields: urlkey, timestamp, original, mimetype, statuscode, digest, length

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use std::time::Duration;

use super::{ArchiveError, ArchiveSource, SnapshotInfo};
use crate::{HttpClient, WAYBACK_CDX_API_URL};
use foiacquire::models::ArchiveService;
use foiacquire::privacy::PrivacyConfig;

/// Wayback Machine CDX API client.
pub struct WaybackSource {
    cdx_url: String,
    privacy: PrivacyConfig,
}

impl Default for WaybackSource {
    fn default() -> Self {
        Self::new()
    }
}

impl WaybackSource {
    pub fn new() -> Self {
        Self {
            cdx_url: WAYBACK_CDX_API_URL.to_string(),
            privacy: PrivacyConfig::default(),
        }
    }

    /// Create with privacy configuration.
    pub fn with_privacy(privacy: PrivacyConfig) -> Self {
        Self {
            cdx_url: WAYBACK_CDX_API_URL.to_string(),
            privacy,
        }
    }

    /// Create with custom CDX API endpoint (for testing or alternative instances).
    pub fn with_cdx_url(cdx_url: impl Into<String>) -> Self {
        Self {
            cdx_url: cdx_url.into(),
            privacy: PrivacyConfig::default(),
        }
    }

    /// Parse a CDX timestamp (YYYYMMDDhhmmss) into DateTime<Utc>.
    fn parse_timestamp(ts: &str) -> Option<DateTime<Utc>> {
        // CDX format: YYYYMMDDhhmmss (14 digits)
        if ts.len() < 14 {
            return None;
        }

        NaiveDateTime::parse_from_str(&ts[..14], "%Y%m%d%H%M%S")
            .ok()
            .map(|dt| dt.and_utc())
    }

    /// Format a DateTime as CDX timestamp.
    fn format_timestamp(dt: DateTime<Utc>) -> String {
        dt.format("%Y%m%d%H%M%S").to_string()
    }

    /// Build the archive URL for retrieving content.
    fn build_archive_url(&self, timestamp: &str, original_url: &str) -> String {
        // Wayback URL format: https://web.archive.org/web/{timestamp}/{original_url}
        format!("https://web.archive.org/web/{}/{}", timestamp, original_url)
    }

    /// Build the raw archive URL (without Wayback's toolbar/frame).
    fn build_raw_archive_url(&self, timestamp: &str, original_url: &str) -> String {
        // Adding 'id_' after timestamp gives raw content
        format!(
            "https://web.archive.org/web/{}id_/{}",
            timestamp, original_url
        )
    }

    /// Parse a CDX response line into SnapshotInfo.
    fn parse_cdx_line(&self, line: &str) -> Option<SnapshotInfo> {
        // CDX format: urlkey timestamp original mimetype statuscode digest length
        let fields: Vec<&str> = line.split_whitespace().collect();

        if fields.len() < 7 {
            return None;
        }

        let timestamp = fields[1];
        let original_url = fields[2];
        let mimetype = fields[3];
        let status_code = fields[4];
        let digest = fields[5];
        let length = fields[6];

        let captured_at = Self::parse_timestamp(timestamp)?;

        Some(SnapshotInfo {
            service: ArchiveService::Wayback,
            original_url: original_url.to_string(),
            archive_url: self.build_raw_archive_url(timestamp, original_url),
            captured_at,
            http_status: status_code.parse().ok(),
            mimetype: if mimetype == "-" {
                None
            } else {
                Some(mimetype.to_string())
            },
            content_length: if length == "-" {
                None
            } else {
                length.parse().ok()
            },
            digest: if digest == "-" {
                None
            } else {
                Some(digest.to_string())
            },
        })
    }

    /// Query the CDX API.
    async fn query_cdx(
        &self,
        url: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<SnapshotInfo>, ArchiveError> {
        let mut query_url = format!(
            "{}?url={}&output=json&fl=urlkey,timestamp,original,mimetype,statuscode,digest,length",
            self.cdx_url,
            urlencoding::encode(url)
        );

        if let Some(from) = from {
            query_url.push_str(&format!("&from={}", Self::format_timestamp(from)));
        }
        if let Some(to) = to {
            query_url.push_str(&format!("&to={}", Self::format_timestamp(to)));
        }

        // Create HTTP client with privacy configuration
        let client = HttpClient::with_privacy(
            "wayback_archive",
            Duration::from_secs(30),
            Duration::from_millis(500),
            Some("FOIAcquire/0.7 (archive-research; +https://github.com/monokrome/foiacquire)"),
            &self.privacy,
        )
        .map_err(|e| ArchiveError::Parse(format!("Failed to create HTTP client: {}", e)))?;

        let body = client.get_text(&query_url).await.map_err(|e| {
            // Check if it's a rate limit or server error based on error message
            let err_str = e.to_string();
            if err_str.contains("429") {
                ArchiveError::RateLimited
            } else if err_str.contains("5") && err_str.contains("status") {
                ArchiveError::Unavailable
            } else {
                ArchiveError::Http(e)
            }
        })?;

        // CDX with output=json returns array of arrays
        // First row is headers, rest are data
        let rows: Vec<Vec<String>> = serde_json::from_str(&body).map_err(|e| {
            // Might be empty response (no snapshots)
            if body.trim().is_empty() {
                return ArchiveError::NotFound;
            }
            ArchiveError::Parse(format!("Failed to parse CDX JSON: {}", e))
        })?;

        // Skip header row
        let snapshots: Vec<SnapshotInfo> = rows
            .into_iter()
            .skip(1)
            .filter_map(|row| self.parse_cdx_row(&row))
            .collect();

        Ok(snapshots)
    }

    /// Parse a CDX JSON row into SnapshotInfo.
    fn parse_cdx_row(&self, row: &[String]) -> Option<SnapshotInfo> {
        if row.len() < 7 {
            return None;
        }

        let timestamp = &row[1];
        let original_url = &row[2];
        let mimetype = &row[3];
        let status_code = &row[4];
        let digest = &row[5];
        let length = &row[6];

        let captured_at = Self::parse_timestamp(timestamp)?;

        Some(SnapshotInfo {
            service: ArchiveService::Wayback,
            original_url: original_url.clone(),
            archive_url: self.build_raw_archive_url(timestamp, original_url),
            captured_at,
            http_status: status_code.parse().ok(),
            mimetype: if mimetype == "-" {
                None
            } else {
                Some(mimetype.clone())
            },
            content_length: if length == "-" {
                None
            } else {
                length.parse().ok()
            },
            digest: if digest == "-" {
                None
            } else {
                Some(digest.clone())
            },
        })
    }
}

#[async_trait]
impl ArchiveSource for WaybackSource {
    fn service(&self) -> ArchiveService {
        ArchiveService::Wayback
    }

    async fn list_snapshots(&self, url: &str) -> Result<Vec<SnapshotInfo>, ArchiveError> {
        self.query_cdx(url, None, None).await
    }

    async fn list_snapshots_range(
        &self,
        url: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<SnapshotInfo>, ArchiveError> {
        self.query_cdx(url, from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_parse_timestamp() {
        let ts = "20231215143022";
        let dt = WaybackSource::parse_timestamp(ts).unwrap();
        assert_eq!(dt.year(), 2023);
        assert_eq!(dt.month(), 12);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 14);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 22);
    }

    #[test]
    fn test_format_timestamp() {
        use chrono::TimeZone;
        let dt = Utc.with_ymd_and_hms(2023, 12, 15, 14, 30, 22).unwrap();
        let ts = WaybackSource::format_timestamp(dt);
        assert_eq!(ts, "20231215143022");
    }

    #[test]
    fn test_build_archive_url() {
        let source = WaybackSource::new();
        let url = source.build_archive_url("20231215143022", "https://example.com/doc.pdf");
        assert_eq!(
            url,
            "https://web.archive.org/web/20231215143022/https://example.com/doc.pdf"
        );
    }

    #[test]
    fn test_build_raw_archive_url() {
        let source = WaybackSource::new();
        let url = source.build_raw_archive_url("20231215143022", "https://example.com/doc.pdf");
        assert_eq!(
            url,
            "https://web.archive.org/web/20231215143022id_/https://example.com/doc.pdf"
        );
    }

    #[test]
    fn test_parse_cdx_row() {
        let source = WaybackSource::new();
        let row = vec![
            "com,example)/doc.pdf".to_string(),
            "20231215143022".to_string(),
            "https://example.com/doc.pdf".to_string(),
            "application/pdf".to_string(),
            "200".to_string(),
            "ABCD1234EFGH5678".to_string(),
            "12345".to_string(),
        ];

        let snapshot = source.parse_cdx_row(&row).unwrap();
        assert_eq!(snapshot.service, ArchiveService::Wayback);
        assert_eq!(snapshot.original_url, "https://example.com/doc.pdf");
        assert_eq!(snapshot.http_status, Some(200));
        assert_eq!(snapshot.mimetype, Some("application/pdf".to_string()));
        assert_eq!(snapshot.content_length, Some(12345));
        assert_eq!(snapshot.digest, Some("ABCD1234EFGH5678".to_string()));
    }

    #[test]
    fn test_parse_cdx_row_with_dashes() {
        let source = WaybackSource::new();
        let row = vec![
            "com,example)/doc.pdf".to_string(),
            "20231215143022".to_string(),
            "https://example.com/doc.pdf".to_string(),
            "-".to_string(),
            "200".to_string(),
            "-".to_string(),
            "-".to_string(),
        ];

        let snapshot = source.parse_cdx_row(&row).unwrap();
        assert_eq!(snapshot.mimetype, None);
        assert_eq!(snapshot.content_length, None);
        assert_eq!(snapshot.digest, None);
    }
}
