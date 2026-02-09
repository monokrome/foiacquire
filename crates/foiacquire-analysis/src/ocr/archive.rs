//! Archive extraction for processing files within zip archives.
//!
//! This module provides functionality to:
//! - List files contained in zip archives
//! - Extract files to temporary locations for OCR processing
//! - Determine MIME types for archive contents

#![allow(dead_code)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use thiserror::Error;
use zip::ZipArchive;

/// Errors that can occur during archive operations.
#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("Failed to open archive: {0}")]
    OpenFailed(String),

    #[error("Failed to read archive entry: {0}")]
    ReadEntry(String),

    #[error("Failed to extract file: {0}")]
    ExtractFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Zip error: {0}")]
    Zip(#[from] zip::result::ZipError),

    #[error("Unsupported archive format: {0}")]
    UnsupportedFormat(String),
}

/// Information about a file within an archive.
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    /// Path within the archive.
    pub path: String,
    /// Filename (last component of path).
    pub filename: String,
    /// Size in bytes (uncompressed).
    pub size: u64,
    /// Detected MIME type.
    pub mime_type: String,
    /// Whether this is a directory.
    pub is_dir: bool,
}

impl ArchiveEntry {
    /// Check if this file type is supported for text extraction.
    pub fn is_extractable(&self) -> bool {
        matches!(
            self.mime_type.as_str(),
            "application/pdf"
                | "image/png"
                | "image/jpeg"
                | "image/tiff"
                | "image/gif"
                | "image/bmp"
                | "text/plain"
                | "text/html"
        )
    }
}

/// Result of extracting a file from an archive.
pub struct ExtractedFile {
    /// The archive entry information.
    pub entry: ArchiveEntry,
    /// Temporary directory containing the extracted file.
    pub temp_dir: TempDir,
    /// Path to the extracted file.
    pub file_path: PathBuf,
}

/// Archive handler for zip files.
pub struct ArchiveExtractor;

impl ArchiveExtractor {
    /// Check if a MIME type represents a supported archive format.
    pub fn is_archive(mime_type: &str) -> bool {
        matches!(
            mime_type,
            "application/zip" | "application/x-zip" | "application/x-zip-compressed"
        )
    }

    /// List all files in a zip archive.
    pub fn list_zip_contents(archive_path: &Path) -> Result<Vec<ArchiveEntry>, ArchiveError> {
        let file = File::open(archive_path).map_err(|e| ArchiveError::OpenFailed(e.to_string()))?;

        let mut archive = ZipArchive::new(file)?;
        let mut entries = Vec::new();

        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            let path = file.name().to_string();

            // Skip directories and __MACOSX metadata
            if file.is_dir() || path.starts_with("__MACOSX") {
                continue;
            }

            let filename = path.rsplit('/').next().unwrap_or(&path).to_string();

            // Skip hidden files
            if filename.starts_with('.') {
                continue;
            }

            let mime_type = mime_from_filename(&filename);

            entries.push(ArchiveEntry {
                path: path.clone(),
                filename,
                size: file.size(),
                mime_type,
                is_dir: false,
            });
        }

        Ok(entries)
    }

    /// Extract a single file from a zip archive to a temporary location.
    pub fn extract_file(
        archive_path: &Path,
        entry_path: &str,
    ) -> Result<ExtractedFile, ArchiveError> {
        let file = File::open(archive_path).map_err(|e| ArchiveError::OpenFailed(e.to_string()))?;

        let mut archive = ZipArchive::new(file)?;

        // Find the entry by path
        let mut zip_file = archive.by_name(entry_path)?;

        // Create temp directory
        let temp_dir = TempDir::new()?;

        // Extract filename for the temp file, sanitizing to prevent path traversal
        let filename = entry_path
            .rsplit('/')
            .next()
            .unwrap_or(entry_path)
            .replace('\\', "_") // Remove backslashes
            .replace("..", "_") // Remove parent directory references
            .trim_start_matches('.') // Remove leading dots (hidden files)
            .to_string();

        // Ensure we have a valid filename after sanitization
        let filename = if filename.is_empty() {
            "extracted_file".to_string()
        } else {
            filename
        };

        let file_path = temp_dir.path().join(&filename);

        // Extract the file
        let mut outfile = File::create(&file_path)?;
        let mut buffer = Vec::new();
        zip_file.read_to_end(&mut buffer)?;
        outfile.write_all(&buffer)?;

        let mime_type = mime_from_filename(&filename);

        let entry = ArchiveEntry {
            path: entry_path.to_string(),
            filename,
            size: zip_file.size(),
            mime_type,
            is_dir: false,
        };

        Ok(ExtractedFile {
            entry,
            temp_dir,
            file_path,
        })
    }

    /// Extract all extractable files from a zip archive.
    pub fn extract_all_extractable(
        archive_path: &Path,
    ) -> Result<Vec<ExtractedFile>, ArchiveError> {
        let entries = Self::list_zip_contents(archive_path)?;
        let mut extracted = Vec::new();

        for entry in entries {
            if entry.is_extractable() {
                match Self::extract_file(archive_path, &entry.path) {
                    Ok(extracted_file) => extracted.push(extracted_file),
                    Err(e) => {
                        tracing::warn!("Failed to extract {}: {}", entry.path, e);
                    }
                }
            }
        }

        Ok(extracted)
    }
}

/// Determine MIME type from filename extension.
fn mime_from_filename(filename: &str) -> String {
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();

    match ext.as_str() {
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "bmp" => "image/bmp",
        "tif" | "tiff" => "image/tiff",
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "zip" => "application/zip",
        _ => "application/octet-stream",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mime_from_filename() {
        assert_eq!(mime_from_filename("test.pdf"), "application/pdf");
        assert_eq!(mime_from_filename("image.PNG"), "image/png");
        assert_eq!(mime_from_filename("doc.JPEG"), "image/jpeg");
        assert_eq!(mime_from_filename("file.txt"), "text/plain");
        assert_eq!(mime_from_filename("unknown"), "application/octet-stream");
    }

    #[test]
    fn test_is_archive() {
        assert!(ArchiveExtractor::is_archive("application/zip"));
        assert!(ArchiveExtractor::is_archive("application/x-zip-compressed"));
        assert!(!ArchiveExtractor::is_archive("application/pdf"));
    }
}
