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

/// Check pdftoppm availability, returning a hint message if missing.
pub fn check_pdftoppm_hint() -> Option<String> {
    if check_binary("pdftoppm") {
        None
    } else {
        Some("pdftoppm not installed. Install with: apt install poppler-utils".to_string())
    }
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
/// Respects SOCKS_PROXY environment variable for privacy routing.
pub fn download_file(url: &str, dest: &Path) -> Result<(), OcrError> {
    // Check for SOCKS proxy configuration
    let socks_proxy = foiacquire::privacy::socks_proxy_from_env();

    let mut curl_cmd = Command::new("curl");
    curl_cmd.args(["-fSL", "--progress-bar", "-o"]);
    curl_cmd.arg(dest);
    curl_cmd.arg(url);

    // Add proxy args for curl if SOCKS_PROXY is set
    if let Some(ref proxy) = socks_proxy {
        curl_cmd.args(["--proxy", proxy]);
        // Use socks5h:// to force DNS through proxy (prevent DNS leaks)
        if !proxy.starts_with("socks5h://") && proxy.starts_with("socks5://") {
            eprintln!("Warning: Use socks5h:// instead of socks5:// to prevent DNS leaks");
        }
    }

    let output = curl_cmd.status();

    match output {
        Ok(status) if status.success() => Ok(()),
        Ok(_) => {
            let _ = std::fs::remove_file(dest);
            Err(OcrError::OcrFailed(format!("Failed to download {}", url)))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Try wget as fallback
            let mut wget_cmd = Command::new("wget");
            wget_cmd.args(["-q", "--show-progress", "-O"]);
            wget_cmd.arg(dest);
            wget_cmd.arg(url);

            // Add proxy environment variables for wget
            if let Some(ref proxy) = socks_proxy {
                // wget uses http_proxy/https_proxy env vars, not command-line args
                // Convert socks5:// to http:// for wget (it doesn't support SOCKS directly)
                // If SOCKS is required, user should use curl
                eprintln!("Note: wget doesn't support SOCKS proxy directly. Using curl is recommended for privacy.");
                // Set as env vars anyway in case wget is built with SOCKS support
                wget_cmd.env("http_proxy", proxy);
                wget_cmd.env("https_proxy", proxy);
            }

            let output = wget_cmd.status();

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
        eprintln!("Downloading {} (~{})...", spec.filename, spec.size_hint);
        download_file(spec.url, &dest)?;
        eprintln!("  ✓ Downloaded {}", spec.filename);
    }
    Ok(())
}

/// Find model directory by checking config path first, then standard locations.
pub fn find_model_dir(
    config_path: Option<&std::path::PathBuf>,
    model_config: &ModelDirConfig,
) -> Option<PathBuf> {
    // Check config path first
    if let Some(path) = config_path {
        if model_config.has_required_files(path) {
            return Some(path.clone());
        }
    }

    // Check standard locations
    model_config
        .candidate_dirs()
        .into_iter()
        .find(|dir| model_config.has_required_files(dir))
}

/// Ensure models are present, downloading if necessary.
pub fn ensure_models_present(
    config_path: Option<&std::path::PathBuf>,
    model_config: &ModelDirConfig,
    model_specs: &[&ModelSpec],
) -> Result<PathBuf, OcrError> {
    if let Some(dir) = find_model_dir(config_path, model_config) {
        return Ok(dir);
    }

    let model_dir = model_config.default_dir();
    std::fs::create_dir_all(&model_dir).map_err(OcrError::Io)?;

    for spec in model_specs {
        ensure_model_file(spec, &model_dir)?;
    }

    Ok(model_dir)
}

/// Build an OcrResult from text and timing info.
pub fn build_ocr_result(
    text: String,
    backend: super::backend::OcrBackendType,
    model: Option<String>,
    start: std::time::Instant,
) -> super::backend::OcrResult {
    super::backend::OcrResult {
        text,
        confidence: None,
        backend,
        model,
        processing_time_ms: start.elapsed().as_millis() as u64,
    }
}

/// Format availability hint for a model-based backend.
pub fn model_availability_hint(
    config_path: Option<&std::path::PathBuf>,
    model_config: &ModelDirConfig,
    backend_name: &str,
    total_size: &str,
) -> String {
    match find_model_dir(config_path, model_config) {
        Some(path) => format!("{} models found at {:?}", backend_name, path),
        None => format!(
            "{} models will be auto-downloaded on first use (~{}) to {:?}",
            backend_name,
            total_size,
            model_config.default_dir()
        ),
    }
}
