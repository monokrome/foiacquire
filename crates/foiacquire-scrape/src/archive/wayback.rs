//! Wayback Machine (web.archive.org) archive source.
//!
//! Uses the CDX API to query snapshot metadata efficiently without
//! downloading actual content.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;

use super::{ArchiveError, ArchiveSource, SnapshotInfo};
use crate::cdx::{self, CdxQuery, CdxRow};
use crate::HttpClient;
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
            cdx_url: cdx::WAYBACK_CDX_API_URL.to_string(),
            privacy: PrivacyConfig::default(),
        }
    }

    /// Create with privacy configuration.
    pub fn with_privacy(privacy: PrivacyConfig) -> Self {
        Self {
            cdx_url: cdx::WAYBACK_CDX_API_URL.to_string(),
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

    /// Convert a CDX row into a SnapshotInfo.
    fn row_to_snapshot(row: &CdxRow) -> Option<SnapshotInfo> {
        let timestamp = row.get_raw("timestamp")?;
        let original_url = row.get_raw("original")?;
        let captured_at = cdx::parse_cdx_timestamp(timestamp)?;

        Some(SnapshotInfo {
            service: ArchiveService::Wayback,
            original_url: original_url.to_string(),
            archive_url: cdx::build_raw_archive_url(timestamp, original_url),
            captured_at,
            http_status: row.get("statuscode").and_then(|s| s.parse().ok()),
            mimetype: row.get("mimetype").map(|s| s.to_string()),
            content_length: row.get("length").and_then(|s| s.parse().ok()),
            digest: row.get("digest").map(|s| s.to_string()),
        })
    }

    /// Query the CDX API.
    async fn query_cdx(
        &self,
        url: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<SnapshotInfo>, ArchiveError> {
        let mut query = CdxQuery::new(url).base_url(&self.cdx_url).fields(&[
            "urlkey",
            "timestamp",
            "original",
            "mimetype",
            "statuscode",
            "digest",
            "length",
        ]);

        if let Some(from) = from {
            query = query.from_date(cdx::format_cdx_timestamp(from));
        }
        if let Some(to) = to {
            query = query.to_date(cdx::format_cdx_timestamp(to));
        }

        let query_url = query.build();

        let client = HttpClient::builder(
            "wayback_archive",
            Duration::from_secs(30),
            Duration::from_millis(500),
        )
        .user_agent("FOIAcquire/0.7 (archive-research; +https://github.com/monokrome/foiacquire)")
        .privacy(&self.privacy)
        .build()
        .map_err(|e| ArchiveError::Parse(format!("Failed to create HTTP client: {}", e)))?;

        let body = client.get_text(&query_url).await.map_err(|e| {
            let err_str = e.to_string();
            if err_str.contains("429") {
                ArchiveError::RateLimited
            } else if err_str.contains("5") && err_str.contains("status") {
                ArchiveError::Unavailable
            } else {
                ArchiveError::Http(e)
            }
        })?;

        let rows = cdx::parse_cdx_response(&body).map_err(|e| match e {
            cdx::CdxParseError::Empty => ArchiveError::NotFound,
            cdx::CdxParseError::Json(msg) => ArchiveError::Parse(msg),
        })?;

        let snapshots = rows.iter().filter_map(Self::row_to_snapshot).collect();
        Ok(snapshots)
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

    #[test]
    fn row_to_snapshot_basic() {
        let json = r#"[
            ["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
            ["com,example)/doc.pdf","20231215143022","https://example.com/doc.pdf","application/pdf","200","ABCD1234EFGH5678","12345"]
        ]"#;
        let rows = cdx::parse_cdx_response(json).unwrap();
        let snapshot = WaybackSource::row_to_snapshot(&rows[0]).unwrap();

        assert_eq!(snapshot.service, ArchiveService::Wayback);
        assert_eq!(snapshot.original_url, "https://example.com/doc.pdf");
        assert_eq!(snapshot.http_status, Some(200));
        assert_eq!(snapshot.mimetype, Some("application/pdf".to_string()));
        assert_eq!(snapshot.content_length, Some(12345));
        assert_eq!(snapshot.digest, Some("ABCD1234EFGH5678".to_string()));
        assert!(snapshot.archive_url.contains("20231215143022id_/"));
    }

    #[test]
    fn row_to_snapshot_with_dashes() {
        let json = r#"[
            ["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
            ["com,example)/doc.pdf","20231215143022","https://example.com/doc.pdf","-","200","-","-"]
        ]"#;
        let rows = cdx::parse_cdx_response(json).unwrap();
        let snapshot = WaybackSource::row_to_snapshot(&rows[0]).unwrap();

        assert_eq!(snapshot.mimetype, None);
        assert_eq!(snapshot.content_length, None);
        assert_eq!(snapshot.digest, None);
    }
}
