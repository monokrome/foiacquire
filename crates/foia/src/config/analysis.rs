//! Analysis configuration types.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A backend entry - either a single backend or a fallback chain.
///
/// Examples:
/// - `"tesseract"` - single backend, always runs
/// - `["groq", "gemini"]` - fallback chain, tries groq first, gemini if rate limited
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackendEntry {
    /// Single backend that always runs.
    Single(String),
    /// Fallback chain - tries backends in order until one succeeds.
    Chain(Vec<String>),
}

impl BackendEntry {
    /// Get the primary backend name (first in chain or the single backend).
    #[allow(dead_code)]
    pub fn primary(&self) -> &str {
        match self {
            BackendEntry::Single(s) => s,
            BackendEntry::Chain(v) => v.first().map(|s| s.as_str()).unwrap_or(""),
        }
    }

    /// Get all backend names in this entry.
    pub fn backends(&self) -> Vec<&str> {
        match self {
            BackendEntry::Single(s) => vec![s.as_str()],
            BackendEntry::Chain(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Check if this is a fallback chain (multiple backends).
    #[allow(dead_code)]
    pub fn is_chain(&self) -> bool {
        matches!(self, BackendEntry::Chain(v) if v.len() > 1)
    }
}

impl prefer::FromValue for BackendEntry {
    fn from_value(value: &prefer::ConfigValue) -> prefer::Result<Self> {
        // Try as string first
        if let Some(s) = value.as_str() {
            return Ok(BackendEntry::Single(s.to_string()));
        }
        // Try as array of strings
        if let Some(arr) = value.as_array() {
            let mut backends = Vec::new();
            for item in arr {
                if let Some(s) = item.as_str() {
                    backends.push(s.to_string());
                } else {
                    return Err(prefer::Error::ConversionError {
                        key: String::new(),
                        type_name: "BackendEntry".to_string(),
                        source: "array items must be strings".into(),
                    });
                }
            }
            return Ok(BackendEntry::Chain(backends));
        }
        Err(prefer::Error::ConversionError {
            key: String::new(),
            type_name: "BackendEntry".to_string(),
            source: "expected string or array of strings".into(),
        })
    }
}

/// OCR backend configuration with parallel execution and fallback chains.
///
/// Each entry in `backends` is either:
/// - A string: single backend that always runs
/// - An array: fallback chain that tries backends in order
///
/// Example: `["tesseract", ["groq", "gemini"], "deepseek"]`
/// - Runs tesseract, stores result
/// - Runs groq (falls back to gemini if rate limited), stores result
/// - Runs deepseek, stores result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OcrConfig {
    /// Backend entries to run. Each entry produces a separate result.
    #[serde(default = "default_ocr_backends")]
    pub backends: Vec<BackendEntry>,
}

impl prefer::FromValue for OcrConfig {
    fn from_value(value: &prefer::ConfigValue) -> prefer::Result<Self> {
        // Try to get backends array
        let backends = if let Some(obj) = value.as_object() {
            if let Some(backends_val) = obj.get("backends") {
                if let Some(arr) = backends_val.as_array() {
                    let mut entries = Vec::new();
                    for item in arr {
                        entries.push(BackendEntry::from_value(item)?);
                    }
                    entries
                } else {
                    default_ocr_backends()
                }
            } else {
                default_ocr_backends()
            }
        } else {
            default_ocr_backends()
        };
        Ok(OcrConfig { backends })
    }
}

fn default_ocr_backends() -> Vec<BackendEntry> {
    if let Ok(val) = std::env::var("ANALYSIS_OCR_BACKENDS") {
        let backends: Vec<BackendEntry> = val
            .split(',')
            .map(|s| BackendEntry::Single(s.trim().to_string()))
            .filter(|e| !matches!(e, BackendEntry::Single(s) if s.is_empty()))
            .collect();
        if !backends.is_empty() {
            return backends;
        }
    }

    if std::env::var("GROQ_API_KEY").is_ok() {
        return vec![BackendEntry::Single("groq".to_string())];
    }
    if std::env::var("GEMINI_API_KEY").is_ok() {
        return vec![BackendEntry::Single("gemini".to_string())];
    }
    if std::process::Command::new("which")
        .arg("tesseract")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return vec![BackendEntry::Single("tesseract".to_string())];
    }

    vec![BackendEntry::Single("tesseract".to_string())]
}

impl Default for OcrConfig {
    fn default() -> Self {
        Self {
            backends: default_ocr_backends(),
        }
    }
}

/// Analysis configuration for text extraction methods.
#[derive(Debug, Clone, Default, Serialize, Deserialize, prefer::FromValue)]
pub struct AnalysisConfig {
    /// OCR backend configuration with fallback support.
    /// Device-local: auto-detected from available backends, never synced to DB.
    #[serde(skip)]
    #[prefer(skip)]
    pub ocr: OcrConfig,
    /// Named analysis methods (custom commands).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[prefer(default)]
    pub methods: HashMap<String, AnalysisMethodConfig>,
    /// Default methods to run if --method flag not specified.
    /// Defaults to ["ocr"] if empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[prefer(default)]
    pub default_methods: Vec<String>,
}

impl AnalysisConfig {
    /// Check if this is the default (empty) config.
    pub fn is_default(&self) -> bool {
        self.methods.is_empty() && self.default_methods.is_empty()
    }
}

/// Configuration for a single analysis method.
#[derive(Debug, Clone, Serialize, Deserialize, prefer::FromValue)]
pub struct AnalysisMethodConfig {
    /// Command to execute (required for custom commands, optional for built-ins).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Arguments (can include {file} and {page} placeholders).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[prefer(default)]
    pub args: Vec<String>,
    /// Mimetypes this method applies to (supports wildcards like "audio/*").
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[prefer(default)]
    pub mimetypes: Vec<String>,
    /// Analysis granularity: "page" or "document" (default: "document").
    #[serde(default = "default_granularity")]
    #[prefer(default = "document")]
    pub granularity: String,
    /// Whether command outputs to stdout (true) or a file (false).
    #[serde(default = "default_true")]
    #[prefer(default = "true")]
    pub stdout: bool,
    /// Output file template (if stdout is false). Can use {file} placeholder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_file: Option<String>,
    /// Model name (for whisper, ocr backends).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

fn default_granularity() -> String {
    "document".to_string()
}

fn default_true() -> bool {
    true
}

impl Default for AnalysisMethodConfig {
    fn default() -> Self {
        Self {
            command: None,
            args: Vec::new(),
            mimetypes: Vec::new(),
            granularity: default_granularity(),
            stdout: true,
            output_file: None,
            model: None,
        }
    }
}
