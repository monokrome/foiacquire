//! Shared Wayback Machine CDX API utilities.
//!
//! Provides URL construction, response parsing, and timestamp handling
//! used by both the archive and discovery wayback modules.

use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, NaiveDateTime, Utc};

/// Wayback Machine CDX API base URL.
pub const WAYBACK_CDX_API_URL: &str = "https://web.archive.org/cdx/search/cdx";

/// Builder for CDX API query URLs.
pub struct CdxQuery {
    base_url: String,
    url_pattern: String,
    fields: Vec<String>,
    match_type: Option<String>,
    collapse: Option<String>,
    filters: Vec<String>,
    from_date: Option<String>,
    to_date: Option<String>,
    limit: Option<usize>,
}

impl CdxQuery {
    /// Create a new query for the given URL pattern.
    pub fn new(url_pattern: impl Into<String>) -> Self {
        Self {
            base_url: WAYBACK_CDX_API_URL.to_string(),
            url_pattern: url_pattern.into(),
            fields: Vec::new(),
            match_type: None,
            collapse: None,
            filters: Vec::new(),
            from_date: None,
            to_date: None,
            limit: None,
        }
    }

    /// Override the CDX API base URL (for testing or alternative instances).
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Set the fields to return (`fl=` parameter).
    pub fn fields(mut self, fields: &[&str]) -> Self {
        self.fields = fields.iter().map(|s| (*s).to_string()).collect();
        self
    }

    /// Set the match type (`matchType=` parameter).
    pub fn match_type(mut self, mt: impl Into<String>) -> Self {
        self.match_type = Some(mt.into());
        self
    }

    /// Set the collapse parameter (`collapse=` parameter).
    pub fn collapse(mut self, field: impl Into<String>) -> Self {
        self.collapse = Some(field.into());
        self
    }

    /// Append a filter (`filter=` parameter). Can be called multiple times.
    pub fn filter(mut self, f: impl Into<String>) -> Self {
        self.filters.push(f.into());
        self
    }

    /// Set the start date (`from=` parameter).
    pub fn from_date(mut self, date: impl Into<String>) -> Self {
        self.from_date = Some(date.into());
        self
    }

    /// Set the end date (`to=` parameter).
    pub fn to_date(mut self, date: impl Into<String>) -> Self {
        self.to_date = Some(date.into());
        self
    }

    /// Set the result limit (`limit=` parameter).
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Build the final CDX API URL.
    pub fn build(&self) -> String {
        // Encode the URL pattern but preserve CDX wildcard characters (*).
        let encoded_pattern = urlencoding::encode(&self.url_pattern).replace("%2A", "*");
        let mut url = format!("{}?url={}&output=json", self.base_url, encoded_pattern);

        if !self.fields.is_empty() {
            url.push_str(&format!("&fl={}", self.fields.join(",")));
        }
        if let Some(ref mt) = self.match_type {
            url.push_str(&format!("&matchType={}", mt));
        }
        if let Some(ref c) = self.collapse {
            url.push_str(&format!("&collapse={}", c));
        }
        for f in &self.filters {
            url.push_str(&format!("&filter={}", f));
        }
        if let Some(ref from) = self.from_date {
            url.push_str(&format!("&from={}", from));
        }
        if let Some(ref to) = self.to_date {
            url.push_str(&format!("&to={}", to));
        }
        if let Some(n) = self.limit {
            if n > 0 {
                url.push_str(&format!("&limit={}", n));
            }
        }

        url
    }
}

/// Errors from CDX response parsing.
#[derive(Debug)]
pub enum CdxParseError {
    /// Response body was empty or whitespace-only.
    Empty,
    /// JSON deserialization failed.
    Json(String),
}

impl fmt::Display for CdxParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "Empty CDX response"),
            Self::Json(msg) => write!(f, "Failed to parse CDX JSON: {}", msg),
        }
    }
}

impl std::error::Error for CdxParseError {}

/// A single row from a CDX JSON response with named field access.
#[derive(Debug, Clone)]
pub struct CdxRow {
    fields: HashMap<String, String>,
}

impl CdxRow {
    /// Get a field value by name.
    ///
    /// Returns `None` for missing fields and CDX null markers (`"-"`).
    pub fn get(&self, field: &str) -> Option<&str> {
        self.fields
            .get(field)
            .map(|s| s.as_str())
            .filter(|s| *s != "-")
    }

    /// Get a field value by name without filtering CDX null markers.
    pub fn get_raw(&self, field: &str) -> Option<&str> {
        self.fields.get(field).map(|s| s.as_str())
    }
}

/// Parse a CDX JSON response body into named-field rows.
///
/// The CDX API with `output=json` returns `Vec<Vec<String>>` where the first
/// row contains field names and subsequent rows contain data.
pub fn parse_cdx_response(body: &str) -> Result<Vec<CdxRow>, CdxParseError> {
    if body.trim().is_empty() {
        return Err(CdxParseError::Empty);
    }

    let rows: Vec<Vec<String>> =
        serde_json::from_str(body).map_err(|e| CdxParseError::Json(e.to_string()))?;

    let headers = match rows.first() {
        Some(h) if !h.is_empty() => h.clone(),
        _ => return Ok(Vec::new()),
    };

    let result = rows
        .into_iter()
        .skip(1)
        .map(|row| {
            let fields = headers
                .iter()
                .zip(row)
                .map(|(k, v)| (k.clone(), v))
                .collect();
            CdxRow { fields }
        })
        .collect();

    Ok(result)
}

/// Parse a CDX timestamp (`YYYYMMDDhhmmss`) into `DateTime<Utc>`.
pub fn parse_cdx_timestamp(ts: &str) -> Option<DateTime<Utc>> {
    if ts.len() < 14 {
        return None;
    }
    NaiveDateTime::parse_from_str(&ts[..14], "%Y%m%d%H%M%S")
        .ok()
        .map(|dt| dt.and_utc())
}

/// Format a `DateTime<Utc>` as a CDX timestamp (`YYYYMMDDhhmmss`).
pub fn format_cdx_timestamp(dt: DateTime<Utc>) -> String {
    dt.format("%Y%m%d%H%M%S").to_string()
}

/// Build a Wayback Machine archive URL (with toolbar/frame).
pub fn build_archive_url(timestamp: &str, original_url: &str) -> String {
    format!("https://web.archive.org/web/{}/{}", timestamp, original_url)
}

/// Build a raw Wayback Machine archive URL (without toolbar/frame).
pub fn build_raw_archive_url(timestamp: &str, original_url: &str) -> String {
    format!(
        "https://web.archive.org/web/{}id_/{}",
        timestamp, original_url
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Timelike};

    #[test]
    fn parse_timestamp_valid() {
        let dt = parse_cdx_timestamp("20231215143022").unwrap();
        assert_eq!(dt.year(), 2023);
        assert_eq!(dt.month(), 12);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 14);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 22);
    }

    #[test]
    fn parse_timestamp_too_short() {
        assert!(parse_cdx_timestamp("2023121514").is_none());
    }

    #[test]
    fn format_timestamp_roundtrip() {
        let dt = Utc.with_ymd_and_hms(2023, 12, 15, 14, 30, 22).unwrap();
        let ts = format_cdx_timestamp(dt);
        assert_eq!(ts, "20231215143022");
        assert_eq!(parse_cdx_timestamp(&ts).unwrap(), dt);
    }

    #[test]
    fn archive_url_standard() {
        assert_eq!(
            build_archive_url("20231215143022", "https://example.com/doc.pdf"),
            "https://web.archive.org/web/20231215143022/https://example.com/doc.pdf"
        );
    }

    #[test]
    fn archive_url_raw() {
        assert_eq!(
            build_raw_archive_url("20231215143022", "https://example.com/doc.pdf"),
            "https://web.archive.org/web/20231215143022id_/https://example.com/doc.pdf"
        );
    }

    #[test]
    fn parse_response_basic() {
        let json = r#"[
            ["original","mimetype","statuscode","timestamp"],
            ["https://example.com/a.pdf","application/pdf","200","20231215143022"],
            ["https://example.com/b.pdf","text/html","301","20230101000000"]
        ]"#;

        let rows = parse_cdx_response(json).unwrap();
        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].get("original"), Some("https://example.com/a.pdf"));
        assert_eq!(rows[0].get("mimetype"), Some("application/pdf"));
        assert_eq!(rows[0].get("statuscode"), Some("200"));
        assert_eq!(rows[0].get("timestamp"), Some("20231215143022"));

        assert_eq!(rows[1].get("original"), Some("https://example.com/b.pdf"));
        assert_eq!(rows[1].get("statuscode"), Some("301"));
    }

    #[test]
    fn parse_response_dash_null_markers() {
        let json = r#"[
            ["original","mimetype","digest","length"],
            ["https://example.com/a.pdf","-","-","-"]
        ]"#;

        let rows = parse_cdx_response(json).unwrap();
        assert_eq!(rows.len(), 1);

        assert_eq!(rows[0].get("mimetype"), None);
        assert_eq!(rows[0].get("digest"), None);
        assert_eq!(rows[0].get("length"), None);

        assert_eq!(rows[0].get_raw("mimetype"), Some("-"));
        assert_eq!(rows[0].get_raw("digest"), Some("-"));
    }

    #[test]
    fn parse_response_empty() {
        assert!(matches!(parse_cdx_response(""), Err(CdxParseError::Empty)));
        assert!(matches!(
            parse_cdx_response("  \n  "),
            Err(CdxParseError::Empty)
        ));
    }

    #[test]
    fn parse_response_invalid_json() {
        assert!(matches!(
            parse_cdx_response("not json"),
            Err(CdxParseError::Json(_))
        ));
    }

    #[test]
    fn parse_response_header_only() {
        let json = r#"[["original","mimetype"]]"#;
        let rows = parse_cdx_response(json).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn cdx_row_missing_field() {
        let json = r#"[["original"],["https://example.com"]]"#;
        let rows = parse_cdx_response(json).unwrap();
        assert_eq!(rows[0].get("mimetype"), None);
        assert_eq!(rows[0].get_raw("mimetype"), None);
    }

    #[test]
    fn query_builder_basic() {
        let url = CdxQuery::new("https://example.com")
            .fields(&["original", "timestamp", "mimetype"])
            .build();

        assert!(url.contains("url=https%3A%2F%2Fexample.com"));
        assert!(url.contains("output=json"));
        assert!(url.contains("fl=original,timestamp,mimetype"));
    }

    #[test]
    fn query_builder_full() {
        let url = CdxQuery::new("*.example.gov")
            .fields(&["original", "mimetype", "statuscode", "timestamp"])
            .match_type("domain")
            .collapse("urlkey")
            .filter("statuscode:200")
            .from_date("20200101")
            .to_date("20231231")
            .limit(100)
            .build();

        assert!(url.contains("url=*.example.gov"));
        assert!(url.contains("matchType=domain"));
        assert!(url.contains("collapse=urlkey"));
        assert!(url.contains("filter=statuscode:200"));
        assert!(url.contains("from=20200101"));
        assert!(url.contains("to=20231231"));
        assert!(url.contains("limit=100"));
    }

    #[test]
    fn query_builder_custom_base_url() {
        let url = CdxQuery::new("https://example.com")
            .base_url("http://localhost:8080/cdx")
            .build();

        assert!(url.starts_with("http://localhost:8080/cdx?"));
    }

    #[test]
    fn query_builder_zero_limit_omitted() {
        let url = CdxQuery::new("https://example.com").limit(0).build();
        assert!(!url.contains("limit="));
    }
}
