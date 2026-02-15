//! Google Drive folder enumeration support.
//!
//! Provides functionality to:
//! - Detect Google Drive folder URLs
//! - Enumerate all files in a public folder
//! - Generate download URLs for each file
//!
//! Uses Google Drive's public folder API to list contents without authentication.

use std::collections::HashSet;

use regex::Regex;
use thiserror::Error;
use tracing::{debug, info, warn};

use super::HttpClient;

/// Error types for Google Drive operations.
#[derive(Error, Debug)]
pub enum GoogleDriveError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Invalid folder URL: {0}")]
    InvalidUrl(String),
    #[error("Failed to parse response: {0}")]
    ParseError(String),
    #[error("Rate limited by Google Drive")]
    RateLimited,
}

/// Information about a file in a Google Drive folder.
#[derive(Debug, Clone)]
pub struct DriveFile {
    /// Google Drive file ID.
    pub id: String,
    /// File name.
    pub name: String,
    /// MIME type.
    pub mime_type: String,
    /// File size in bytes (if available).
    pub size: Option<u64>,
    /// Direct download URL.
    pub download_url: String,
    /// Parent folder ID.
    pub parent_folder_id: String,
}

impl DriveFile {
    /// Check if this is a downloadable file (not a folder).
    pub fn is_downloadable(&self) -> bool {
        self.mime_type != "application/vnd.google-apps.folder"
    }
}

/// Check if a URL is a Google Drive folder URL.
pub fn is_google_drive_folder_url(url: &str) -> bool {
    url.contains("drive.google.com/drive/folders/")
        || url.contains("drive.google.com/drive/u/") && url.contains("/folders/")
}

/// Extract folder ID from a Google Drive folder URL.
///
/// Handles formats:
/// - https://drive.google.com/drive/folders/FOLDER_ID
/// - https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing
/// - https://drive.google.com/drive/u/0/folders/FOLDER_ID
pub fn extract_folder_id(url: &str) -> Option<String> {
    // Pattern: /folders/([a-zA-Z0-9_-]+)
    let re = Regex::new(r"/folders/([a-zA-Z0-9_-]+)").ok()?;
    re.captures(url).map(|c| c[1].to_string())
}

/// Check if a URL is a Google Drive file URL (not folder).
pub fn is_google_drive_file_url(url: &str) -> bool {
    (url.contains("drive.google.com/file/d/") || url.contains("drive.google.com/uc?"))
        && !url.contains("/folders/")
}

/// Extract file ID from a Google Drive file URL.
///
/// Handles formats:
/// - https://drive.google.com/file/d/FILE_ID/view
/// - https://drive.google.com/uc?id=FILE_ID
/// - https://drive.google.com/open?id=FILE_ID
pub fn extract_file_id(url: &str) -> Option<String> {
    // Pattern: /d/([a-zA-Z0-9_-]+) or id=([a-zA-Z0-9_-]+)
    let re_d = Regex::new(r"/d/([a-zA-Z0-9_-]+)").ok()?;
    let re_id = Regex::new(r"[?&]id=([a-zA-Z0-9_-]+)").ok()?;

    re_d.captures(url)
        .or_else(|| re_id.captures(url))
        .map(|c| c[1].to_string())
}

/// Generate a direct download URL for a Google Drive file.
pub fn file_download_url(file_id: &str) -> String {
    // Using the export/download endpoint which handles large files
    format!(
        "https://drive.google.com/uc?export=download&id={}&confirm=t",
        file_id
    )
}

/// Google Drive folder enumerator.
pub struct DriveFolder {
    folder_id: String,
    client: HttpClient,
}

impl DriveFolder {
    /// Create a new folder enumerator from a folder URL.
    pub fn from_url(url: &str, client: HttpClient) -> Result<Self, GoogleDriveError> {
        let folder_id =
            extract_folder_id(url).ok_or_else(|| GoogleDriveError::InvalidUrl(url.to_string()))?;

        Ok(Self { folder_id, client })
    }

    /// Create a new folder enumerator from a folder ID.
    pub fn from_id(folder_id: String, client: HttpClient) -> Self {
        Self { folder_id, client }
    }

    /// List all files in the folder (non-recursive).
    pub async fn list_files(&self) -> Result<Vec<DriveFile>, GoogleDriveError> {
        info!("Enumerating Google Drive folder: {}", self.folder_id);

        let mut all_files = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let (files, next_token) = self.fetch_page(page_token.as_deref()).await?;
            all_files.extend(files);

            match next_token {
                Some(token) => {
                    debug!("Fetching next page with token");
                    page_token = Some(token);
                }
                None => break,
            }
        }

        info!(
            "Found {} files in folder {}",
            all_files.len(),
            self.folder_id
        );
        Ok(all_files)
    }

    /// List all files recursively, including subfolders.
    pub async fn list_files_recursive(&self) -> Result<Vec<DriveFile>, GoogleDriveError> {
        let mut all_files = Vec::new();
        let mut folders_to_process = vec![self.folder_id.clone()];
        let mut processed_folders = HashSet::new();

        while let Some(folder_id) = folders_to_process.pop() {
            if processed_folders.contains(&folder_id) {
                continue;
            }
            processed_folders.insert(folder_id.clone());

            let folder = DriveFolder::from_id(folder_id, self.client.clone());
            match folder.list_files().await {
                Ok(files) => {
                    for file in files {
                        if file.mime_type == "application/vnd.google-apps.folder" {
                            folders_to_process.push(file.id.clone());
                        } else {
                            all_files.push(file);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to enumerate subfolder: {}", e);
                }
            }
        }

        info!("Found {} total files recursively", all_files.len());
        Ok(all_files)
    }

    /// Fetch a single page of results.
    async fn fetch_page(
        &self,
        page_token: Option<&str>,
    ) -> Result<(Vec<DriveFile>, Option<String>), GoogleDriveError> {
        // Use the Google Drive webpage and extract embedded JSON
        // This works for public folders without authentication
        let url = match page_token {
            Some(token) => format!(
                "https://drive.google.com/drive/folders/{}?pageToken={}",
                self.folder_id, token
            ),
            None => format!("https://drive.google.com/drive/folders/{}", self.folder_id),
        };

        let response = self
            .client
            .get(&url, None, None)
            .await
            .map_err(|e| GoogleDriveError::Http(e.to_string()))?;

        if !response.is_success() {
            if response.is_rate_limited() {
                return Err(GoogleDriveError::RateLimited);
            }
            return Err(GoogleDriveError::Http(format!("HTTP {}", response.status)));
        }

        let html = response
            .text()
            .await
            .map_err(|e| GoogleDriveError::Http(e.to_string()))?;

        self.parse_folder_page(&html)
    }

    /// Parse the folder page HTML to extract file information.
    fn parse_folder_page(
        &self,
        html: &str,
    ) -> Result<(Vec<DriveFile>, Option<String>), GoogleDriveError> {
        // Google Drive embeds file data as JSON in the page
        // Look for the data array pattern: window['_DRIVE_ivd'] = '...'
        // or the newer pattern in the page source

        let mut files = Vec::new();

        // Try multiple extraction methods
        if let Some(extracted) = self.extract_from_embedded_json(html) {
            return Ok((extracted, None));
        }

        // Fallback: parse file links from HTML
        if let Some(extracted) = self.extract_from_html_links(html) {
            files.extend(extracted);
        }

        // Look for pagination token
        let next_token = self.extract_page_token(html);

        Ok((files, next_token))
    }

    /// Extract file info from embedded JSON in the page.
    fn extract_from_embedded_json(&self, html: &str) -> Option<Vec<DriveFile>> {
        // Look for the pattern containing file metadata
        // Google Drive uses various patterns, try common ones

        let mut files = Vec::new();

        // Pattern 1: Look for JSON arrays with file IDs
        // Files appear as arrays like: ["FILE_ID","filename.pdf",...]
        let file_pattern = Regex::new(
            r#"\["([a-zA-Z0-9_-]{20,})","([^"]+)","([^"]*)",\s*"([^"]*)"(?:,\s*"?(\d+)"?)?"#,
        )
        .ok()?;

        for cap in file_pattern.captures_iter(html) {
            let id = cap[1].to_string();
            let name = cap[2].to_string();
            let mime_type = cap.get(4).map(|m| m.as_str()).unwrap_or("").to_string();

            // Skip if it looks like a folder ID reference, not a file entry
            if name.is_empty() || name.len() < 2 {
                continue;
            }

            // Try to get size
            let size = cap.get(5).and_then(|s| s.as_str().parse().ok());

            let file = DriveFile {
                id: id.clone(),
                name,
                mime_type: if mime_type.is_empty() {
                    foia::utils::guess_mime_from_filename(&cap[2]).to_string()
                } else {
                    mime_type
                },
                size,
                download_url: file_download_url(&id),
                parent_folder_id: self.folder_id.clone(),
            };

            if file.is_downloadable() {
                files.push(file);
            }
        }

        // Pattern 2: Look for data-id attributes with file info
        let data_id_pattern =
            Regex::new(r#"data-id="([a-zA-Z0-9_-]{20,})"[^>]*data-target="doc"[^>]*>"#).ok()?;
        let name_pattern = Regex::new(r#"data-tooltip="([^"]+)""#).ok()?;

        for cap in data_id_pattern.captures_iter(html) {
            let id = cap[1].to_string();

            // Skip if we already have this file
            if files.iter().any(|f| f.id == id) {
                continue;
            }

            // Try to find the name nearby
            let start = cap.get(0)?.start();
            let context = &html[start.saturating_sub(500)..start.min(html.len())];
            let name = name_pattern
                .captures(context)
                .map(|c| c[1].to_string())
                .unwrap_or_else(|| format!("file_{}", id));

            let file = DriveFile {
                id: id.clone(),
                name: name.clone(),
                mime_type: foia::utils::guess_mime_from_filename(&name).to_string(),
                size: None,
                download_url: file_download_url(&id),
                parent_folder_id: self.folder_id.clone(),
            };

            if file.is_downloadable() {
                files.push(file);
            }
        }

        if files.is_empty() {
            None
        } else {
            Some(files)
        }
    }

    /// Extract file links from HTML anchor tags.
    fn extract_from_html_links(&self, html: &str) -> Option<Vec<DriveFile>> {
        let mut files = Vec::new();

        // Look for file/d/ links
        let link_pattern =
            Regex::new(r#"href="[^"]*(?:/file/d/|/open\?id=)([a-zA-Z0-9_-]{20,})[^"]*""#).ok()?;

        for cap in link_pattern.captures_iter(html) {
            let id = cap[1].to_string();

            // Skip duplicates
            if files.iter().any(|f: &DriveFile| f.id == id) {
                continue;
            }

            // Try to extract name from surrounding context
            let start = cap.get(0)?.start();
            let end = (start + 500).min(html.len());
            let context = &html[start..end];

            // Look for title or text content
            let name = extract_name_from_context(context).unwrap_or_else(|| format!("file_{}", id));

            let file = DriveFile {
                id: id.clone(),
                name: name.clone(),
                mime_type: foia::utils::guess_mime_from_filename(&name).to_string(),
                size: None,
                download_url: file_download_url(&id),
                parent_folder_id: self.folder_id.clone(),
            };

            files.push(file);
        }

        if files.is_empty() {
            None
        } else {
            Some(files)
        }
    }

    /// Extract pagination token from the page.
    fn extract_page_token(&self, html: &str) -> Option<String> {
        // Look for next page token in the page
        let token_pattern = Regex::new(r#"pageToken['":\s]+['"]([^'"]+)['"]"#).ok()?;
        token_pattern.captures(html).map(|c| c[1].to_string())
    }
}

/// Extract a name from HTML context around a link.
fn extract_name_from_context(context: &str) -> Option<String> {
    // Try to find title attribute
    let title_pattern = Regex::new(r#"title="([^"]+)""#).ok()?;
    if let Some(cap) = title_pattern.captures(context) {
        return Some(cap[1].to_string());
    }

    // Try to find text between tags
    let text_pattern = Regex::new(r#">([^<]{3,100})<"#).ok()?;
    for cap in text_pattern.captures_iter(context) {
        let text = cap[1].trim();
        // Skip if it looks like HTML or script
        if !text.contains("function")
            && !text.contains("{")
            && !text.starts_with("//")
            && text.len() > 3
        {
            return Some(text.to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_google_drive_folder_url() {
        assert!(is_google_drive_folder_url(
            "https://drive.google.com/drive/folders/1TrGxDGQLDLZu1vvvZDBAh-e7wN3y6Hoz"
        ));
        assert!(is_google_drive_folder_url(
            "https://drive.google.com/drive/folders/1TrGxDGQLDLZu1vvvZDBAh-e7wN3y6Hoz?usp=sharing"
        ));
        assert!(is_google_drive_folder_url(
            "https://drive.google.com/drive/u/0/folders/1TrGxDGQLDLZu1vvvZDBAh-e7wN3y6Hoz"
        ));
        assert!(!is_google_drive_folder_url("https://google.com"));
        assert!(!is_google_drive_folder_url(
            "https://drive.google.com/file/d/abc123"
        ));
    }

    #[test]
    fn test_extract_folder_id() {
        assert_eq!(
            extract_folder_id(
                "https://drive.google.com/drive/folders/1TrGxDGQLDLZu1vvvZDBAh-e7wN3y6Hoz"
            ),
            Some("1TrGxDGQLDLZu1vvvZDBAh-e7wN3y6Hoz".to_string())
        );
        assert_eq!(
            extract_folder_id(
                "https://drive.google.com/drive/folders/1TrGxDGQLDLZu1vvvZDBAh-e7wN3y6Hoz?usp=sharing"
            ),
            Some("1TrGxDGQLDLZu1vvvZDBAh-e7wN3y6Hoz".to_string())
        );
        assert_eq!(
            extract_folder_id(
                "https://drive.google.com/drive/u/0/folders/1hTNH5woIRio578onLGElkTWofUSWRoH_"
            ),
            Some("1hTNH5woIRio578onLGElkTWofUSWRoH_".to_string())
        );
    }

    #[test]
    fn test_is_google_drive_file_url() {
        assert!(is_google_drive_file_url(
            "https://drive.google.com/file/d/1abc123/view"
        ));
        assert!(is_google_drive_file_url(
            "https://drive.google.com/uc?id=1abc123"
        ));
        assert!(!is_google_drive_file_url(
            "https://drive.google.com/drive/folders/abc"
        ));
    }

    #[test]
    fn test_extract_file_id() {
        assert_eq!(
            extract_file_id("https://drive.google.com/file/d/1abc123def456/view"),
            Some("1abc123def456".to_string())
        );
        assert_eq!(
            extract_file_id("https://drive.google.com/uc?id=1abc123def456"),
            Some("1abc123def456".to_string())
        );
        assert_eq!(
            extract_file_id("https://drive.google.com/open?id=1abc123def456"),
            Some("1abc123def456".to_string())
        );
    }

    #[test]
    fn test_file_download_url() {
        let url = file_download_url("1abc123");
        assert!(url.contains("1abc123"));
        assert!(url.contains("export=download"));
    }

    #[test]
    fn test_guess_mime_type() {
        assert_eq!(
            foia::utils::guess_mime_from_filename("document.pdf"),
            "application/pdf"
        );
        assert_eq!(
            foia::utils::guess_mime_from_filename("DOCUMENT.PDF"),
            "application/pdf"
        );
        assert_eq!(
            foia::utils::guess_mime_from_filename("file.docx"),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
        assert_eq!(
            foia::utils::guess_mime_from_filename("image.jpg"),
            "image/jpeg"
        );
        assert_eq!(
            foia::utils::guess_mime_from_filename("unknown"),
            "application/octet-stream"
        );
    }
}
