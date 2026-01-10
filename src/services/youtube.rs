//! YouTube video download service using yt-dlp.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::process::Command;
use tracing::{debug, info, warn};

/// Check if a URL is a YouTube video URL.
pub fn is_youtube_url(url: &str) -> bool {
    url.contains("youtube.com/watch")
        || url.contains("youtube.com/embed/")
        || url.contains("youtu.be/")
        || url.contains("youtube.com/v/")
}

/// Metadata returned by yt-dlp.
#[derive(Debug, Clone, Deserialize)]
pub struct VideoMetadata {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub upload_date: Option<String>,
    #[serde(default)]
    pub uploader: Option<String>,
    #[serde(default)]
    pub duration: Option<f64>,
    #[serde(default)]
    pub view_count: Option<u64>,
}

/// Result of a YouTube download.
#[derive(Debug)]
pub struct DownloadResult {
    pub video_path: PathBuf,
    pub metadata: VideoMetadata,
}

/// Download a YouTube video using yt-dlp.
///
/// If `proxy_url` is provided, it will be passed to yt-dlp's --proxy flag.
/// This should be a SOCKS5 URL like "socks5://127.0.0.1:9050".
pub async fn download_video(
    url: &str,
    output_dir: &Path,
    proxy_url: Option<&str>,
) -> Result<DownloadResult> {
    info!("Downloading YouTube video: {}", url);

    // First, get metadata
    let metadata = fetch_metadata(url, proxy_url).await?;
    debug!("Video metadata: {:?}", metadata);

    // Create output directory
    tokio::fs::create_dir_all(output_dir).await?;

    // Build output template - use video ID and title
    let safe_title = sanitize_filename(&metadata.title);
    let output_template = output_dir
        .join(format!("{}-{}.%(ext)s", metadata.id, safe_title))
        .to_string_lossy()
        .to_string();

    // Build yt-dlp command
    let mut cmd = Command::new("yt-dlp");
    cmd.args([
        "--no-playlist",
        "--format",
        "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "--merge-output-format",
        "mp4",
        "--output",
        &output_template,
        "--no-progress",
    ]);

    // Add proxy if configured
    if let Some(proxy) = proxy_url {
        debug!("Using proxy for yt-dlp: {}", proxy);
        cmd.args(["--proxy", proxy]);
    }

    cmd.arg(url);

    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("Failed to execute yt-dlp")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!("yt-dlp stderr: {}", stderr);
        anyhow::bail!("yt-dlp failed: {}", stderr);
    }

    // Find the downloaded file
    let expected_path = output_dir.join(format!("{}-{}.mp4", metadata.id, safe_title));

    if expected_path.exists() {
        info!("Downloaded: {:?}", expected_path);
        Ok(DownloadResult {
            video_path: expected_path,
            metadata,
        })
    } else {
        // Try to find any file matching the video ID
        let mut entries = tokio::fs::read_dir(output_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&metadata.id) {
                info!("Downloaded: {:?}", entry.path());
                return Ok(DownloadResult {
                    video_path: entry.path(),
                    metadata,
                });
            }
        }
        anyhow::bail!("Downloaded file not found for video {}", metadata.id)
    }
}

/// Fetch video metadata without downloading.
///
/// If `proxy_url` is provided, it will be passed to yt-dlp's --proxy flag.
pub async fn fetch_metadata(url: &str, proxy_url: Option<&str>) -> Result<VideoMetadata> {
    let mut cmd = Command::new("yt-dlp");
    cmd.args(["--dump-json", "--no-playlist"]);

    if let Some(proxy) = proxy_url {
        cmd.args(["--proxy", proxy]);
    }

    cmd.arg(url);

    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("Failed to execute yt-dlp for metadata")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("yt-dlp metadata fetch failed: {}", stderr);
    }

    let metadata: VideoMetadata =
        serde_json::from_slice(&output.stdout).context("Failed to parse yt-dlp JSON output")?;

    Ok(metadata)
}

/// Sanitize a string for use as a filename.
fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .take(100) // Limit length
        .collect::<String>()
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_youtube_url() {
        assert!(is_youtube_url("https://www.youtube.com/watch?v=abc123"));
        assert!(is_youtube_url("https://youtube.com/embed/abc123"));
        assert!(is_youtube_url("https://youtu.be/abc123"));
        assert!(!is_youtube_url("https://example.com/video.mp4"));
        assert!(!is_youtube_url("https://vimeo.com/123456"));
    }
}
