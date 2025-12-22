//! Shared utilities for OCR backends.
//!
//! Provides common functionality for:
//! - Downloading and locating OCR models
//! - Checking for CLI tool availability

// These utilities are only used when ocr-ocrs or ocr-paddle features are enabled
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use super::backend::OcrError;

/// Check if a binary is available in PATH.
pub fn check_binary(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Model file specification for downloading.
pub struct ModelSpec {
    /// URL to download from.
    pub url: &'static str,
    /// Filename to save as.
    pub filename: &'static str,
    /// Human-readable size for progress messages.
    pub size_hint: &'static str,
}

/// Configuration for model directory management.
pub struct ModelDirConfig {
    /// Subdirectory name under data_dir (e.g., "ocrs", "paddle-ocr").
    pub subdir: &'static str,
    /// Required model files to check for presence.
    pub required_files: &'static [&'static str],
}

impl ModelDirConfig {
    /// Get the default model directory for this backend.
    pub fn default_dir(&self) -> PathBuf {
        dirs::data_dir()
            .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")))
            .join(self.subdir)
            .join("models")
    }

    /// Get standard candidate directories to search for models.
    pub fn candidate_dirs(&self) -> Vec<PathBuf> {
        [
            dirs::data_dir().map(|d| d.join(self.subdir).join("models")),
            dirs::home_dir().map(|d| d.join(format!(".{}", self.subdir)).join("models")),
            Some(PathBuf::from(format!("/usr/share/{}/models", self.subdir))),
            Some(PathBuf::from(format!(
                "./models/{}",
                self.subdir.split('-').next().unwrap_or(self.subdir)
            ))),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Check if a directory contains all required model files.
    pub fn has_required_files(&self, dir: &Path) -> bool {
        self.required_files
            .iter()
            .all(|file| dir.join(file).exists())
    }
}

/// Download a file from a URL to a local path using curl or wget.
pub fn download_file(url: &str, dest: &Path) -> Result<(), OcrError> {
    let output = Command::new("curl")
        .args(["-fSL", "--progress-bar", "-o"])
        .arg(dest)
        .arg(url)
        .status();

    match output {
        Ok(status) if status.success() => Ok(()),
        Ok(_) => {
            let _ = std::fs::remove_file(dest);
            Err(OcrError::OcrFailed(format!("Failed to download {}", url)))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Try wget as fallback
            let output = Command::new("wget")
                .args(["-q", "--show-progress", "-O"])
                .arg(dest)
                .arg(url)
                .status();

            match output {
                Ok(status) if status.success() => Ok(()),
                Ok(_) => {
                    let _ = std::fs::remove_file(dest);
                    Err(OcrError::OcrFailed(format!("Failed to download {}", url)))
                }
                Err(_) => Err(OcrError::BackendNotAvailable(
                    "Neither curl nor wget found. Install one to download models.".to_string(),
                )),
            }
        }
        Err(e) => Err(OcrError::Io(e)),
    }
}

/// Download a model file if it doesn't exist, with progress message.
pub fn ensure_model_file(spec: &ModelSpec, model_dir: &Path) -> Result<(), OcrError> {
    let dest = model_dir.join(spec.filename);
    if !dest.exists() {
        eprintln!(
            "Downloading {} (~{})...",
            spec.filename, spec.size_hint
        );
        download_file(spec.url, &dest)?;
        eprintln!("  ✓ Downloaded {}", spec.filename);
    }
    Ok(())
}
