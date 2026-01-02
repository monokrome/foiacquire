//! Custom command-based analysis backend.
//!
//! Allows users to define custom analysis commands in the configuration file.
//! Commands can use placeholders like {file} and {page} in their arguments.
//!
//! # Privacy Integration
//!
//! Custom commands receive privacy-related environment variables:
//! - `SOCKS_PROXY` - SOCKS5 proxy URL if configured
//! - `ALL_PROXY` - Same as SOCKS_PROXY for compatibility
//! - `FOIACQUIRE_DIRECT` - "1" if running in direct mode (no Tor)

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::backend::{
    mimetype_matches, AnalysisBackend, AnalysisError, AnalysisGranularity, AnalysisResult,
    AnalysisType,
};

/// Custom command configuration from config file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CustomAnalysisConfig {
    /// Command to execute.
    pub command: String,
    /// Arguments (can include {file} and {page} placeholders).
    #[serde(default)]
    pub args: Vec<String>,
    /// Mimetypes this command applies to (supports wildcards like "audio/*").
    #[serde(default)]
    pub mimetypes: Vec<String>,
    /// Analysis granularity: "page" or "document" (default: "document").
    #[serde(default = "default_granularity")]
    pub granularity: String,
    /// Whether command outputs to stdout (true) or a file (false).
    #[serde(default = "default_true")]
    pub stdout: bool,
    /// Output file template (if stdout is false). Can use {file} placeholder.
    #[serde(default)]
    pub output_file: Option<String>,
    /// Timeout in seconds (default: 300 = 5 minutes).
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_granularity() -> String {
    "document".to_string()
}

fn default_true() -> bool {
    true
}

fn default_timeout() -> u64 {
    300
}

impl Default for CustomAnalysisConfig {
    fn default() -> Self {
        Self {
            command: String::new(),
            args: Vec::new(),
            mimetypes: Vec::new(),
            granularity: default_granularity(),
            stdout: true,
            output_file: None,
            timeout_seconds: default_timeout(),
        }
    }
}

/// Custom command-based analysis backend.
pub struct CustomBackend {
    name: String,
    config: CustomAnalysisConfig,
}

impl CustomBackend {
    /// Create a new custom backend.
    pub fn new(name: String, config: CustomAnalysisConfig) -> Self {
        Self { name, config }
    }

    /// Apply privacy-related environment variables to a command.
    ///
    /// Sets:
    /// - SOCKS_PROXY / ALL_PROXY if a proxy is available (embedded Arti or external)
    /// - FOIACQUIRE_DIRECT if running in direct mode
    fn apply_privacy_env(&self, cmd: &mut Command) {
        // Check for embedded Arti first
        #[cfg(feature = "embedded-tor")]
        if let Some(proxy_url) = crate::privacy::get_arti_socks_url() {
            cmd.env("SOCKS_PROXY", &proxy_url);
            cmd.env("ALL_PROXY", &proxy_url);
            return;
        }

        // If SOCKS_PROXY is already set in environment, it gets inherited automatically
        // Just add ALL_PROXY as an alias for compatibility
        if let Ok(proxy) = std::env::var("SOCKS_PROXY") {
            cmd.env("ALL_PROXY", proxy);
        }

        // Forward FOIACQUIRE_DIRECT if set
        if let Ok(direct) = std::env::var("FOIACQUIRE_DIRECT") {
            cmd.env("FOIACQUIRE_DIRECT", direct);
        }
    }

    /// Replace placeholders in argument string.
    fn expand_arg(&self, arg: &str, file_path: &Path, page: Option<u32>) -> String {
        let file_str = file_path.to_string_lossy();
        let mut result = arg.replace("{file}", &file_str);
        if let Some(p) = page {
            result = result.replace("{page}", &p.to_string());
        }
        // Also support {basename} for just the filename
        if let Some(basename) = file_path.file_name().and_then(|n| n.to_str()) {
            result = result.replace("{basename}", basename);
        }
        // And {stem} for filename without extension
        if let Some(stem) = file_path.file_stem().and_then(|n| n.to_str()) {
            result = result.replace("{stem}", stem);
        }
        result
    }

    /// Build command arguments with placeholders expanded.
    fn build_args(&self, file_path: &Path, page: Option<u32>) -> Vec<String> {
        self.config
            .args
            .iter()
            .map(|arg| self.expand_arg(arg, file_path, page))
            .collect()
    }

    /// Read output from command execution.
    fn read_output(
        &self,
        output: &std::process::Output,
        file_path: &Path,
    ) -> Result<String, AnalysisError> {
        if self.config.stdout {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else if let Some(ref template) = self.config.output_file {
            let output_path = self.expand_arg(template, file_path, None);
            std::fs::read_to_string(&output_path).map_err(|e| {
                AnalysisError::AnalysisFailed(format!(
                    "Failed to read output file '{}': {}",
                    output_path, e
                ))
            })
        } else {
            // Fallback to stdout if no output_file specified
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        }
    }
}

impl AnalysisBackend for CustomBackend {
    fn analysis_type(&self) -> AnalysisType {
        AnalysisType::Custom(self.name.clone())
    }

    fn backend_id(&self) -> &str {
        &self.name
    }

    fn is_available(&self) -> bool {
        // Check if command exists
        Command::new(&self.config.command)
            .arg("--version")
            .output()
            .map(|_| true)
            .or_else(|_| {
                Command::new(&self.config.command)
                    .arg("--help")
                    .output()
                    .map(|_| true)
            })
            .or_else(|_| {
                // Some commands don't have --version or --help
                // Check using 'which' on Unix
                #[cfg(unix)]
                {
                    Command::new("which")
                        .arg(&self.config.command)
                        .output()
                        .map(|o| o.status.success())
                }
                #[cfg(not(unix))]
                {
                    Ok::<bool, std::io::Error>(false)
                }
            })
            .unwrap_or(false)
    }

    fn availability_hint(&self) -> String {
        format!("Install or add to PATH: {}", self.config.command)
    }

    fn granularity(&self) -> AnalysisGranularity {
        match self.config.granularity.to_lowercase().as_str() {
            "page" => AnalysisGranularity::Page,
            _ => AnalysisGranularity::Document,
        }
    }

    fn supports_mimetype(&self, mimetype: &str) -> bool {
        // If no mimetypes specified, match nothing (require explicit config)
        if self.config.mimetypes.is_empty() {
            return false;
        }
        self.config
            .mimetypes
            .iter()
            .any(|pattern| mimetype_matches(pattern, mimetype))
    }

    fn analyze_file(&self, file_path: &Path) -> Result<AnalysisResult, AnalysisError> {
        if self.granularity() == AnalysisGranularity::Page {
            return Err(AnalysisError::UnsupportedOperation(
                "This is a page-level backend. Use analyze_page() instead.".to_string(),
            ));
        }

        let start = Instant::now();
        let args = self.build_args(file_path, None);

        let mut cmd = Command::new(&self.config.command);
        cmd.args(&args);
        self.apply_privacy_env(&mut cmd);

        let output = cmd
            .output()
            .map_err(|e| AnalysisError::CommandFailed(format!("Failed to run command: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AnalysisError::CommandFailed(format!(
                "{} failed (exit code {:?}): {}",
                self.config.command,
                output.status.code(),
                stderr.lines().take(5).collect::<Vec<_>>().join("\n")
            )));
        }

        let text = self.read_output(&output, file_path)?;

        let metadata = serde_json::json!({
            "command": self.config.command,
            "args": args,
        });

        Ok(AnalysisResult {
            text,
            confidence: None,
            backend: self.name.clone(),
            processing_time_ms: start.elapsed().as_millis() as u64,
            metadata: Some(metadata),
        })
    }

    fn analyze_page(&self, file_path: &Path, page: u32) -> Result<AnalysisResult, AnalysisError> {
        if self.granularity() == AnalysisGranularity::Document {
            return Err(AnalysisError::UnsupportedOperation(
                "This is a document-level backend. Use analyze_file() instead.".to_string(),
            ));
        }

        let start = Instant::now();
        let args = self.build_args(file_path, Some(page));

        let mut cmd = Command::new(&self.config.command);
        cmd.args(&args);
        self.apply_privacy_env(&mut cmd);

        let output = cmd
            .output()
            .map_err(|e| AnalysisError::CommandFailed(format!("Failed to run command: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AnalysisError::CommandFailed(format!(
                "{} failed on page {} (exit code {:?}): {}",
                self.config.command,
                page,
                output.status.code(),
                stderr.lines().take(5).collect::<Vec<_>>().join("\n")
            )));
        }

        let text = self.read_output(&output, file_path)?;

        let metadata = serde_json::json!({
            "command": self.config.command,
            "page": page,
            "args": args,
        });

        Ok(AnalysisResult {
            text,
            confidence: None,
            backend: self.name.clone(),
            processing_time_ms: start.elapsed().as_millis() as u64,
            metadata: Some(metadata),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder_expansion() {
        let config = CustomAnalysisConfig {
            command: "test".to_string(),
            args: vec!["{file}".to_string(), "-p".to_string(), "{page}".to_string()],
            ..Default::default()
        };
        let backend = CustomBackend::new("test".to_string(), config);

        let path = Path::new("/tmp/document.pdf");
        let args = backend.build_args(path, Some(5));

        assert_eq!(args, vec!["/tmp/document.pdf", "-p", "5"]);
    }

    #[test]
    fn test_mimetype_matching() {
        let config = CustomAnalysisConfig {
            command: "test".to_string(),
            mimetypes: vec!["audio/*".to_string(), "video/mp4".to_string()],
            ..Default::default()
        };
        let backend = CustomBackend::new("test".to_string(), config);

        assert!(backend.supports_mimetype("audio/mp3"));
        assert!(backend.supports_mimetype("audio/wav"));
        assert!(backend.supports_mimetype("video/mp4"));
        assert!(!backend.supports_mimetype("video/webm")); // Only mp4 specified, not video/*
        assert!(!backend.supports_mimetype("application/pdf"));
    }

    #[test]
    fn test_empty_mimetypes_matches_nothing() {
        let config = CustomAnalysisConfig {
            command: "test".to_string(),
            mimetypes: vec![],
            ..Default::default()
        };
        let backend = CustomBackend::new("test".to_string(), config);

        assert!(!backend.supports_mimetype("anything/here"));
    }
}
