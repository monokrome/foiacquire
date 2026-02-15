//! Whisper audio/video transcription backend.
//!
//! Uses the whisper CLI (from openai-whisper Python package) to transcribe
//! audio and video files. The transcription is document-level since audio/video
//! files are processed as a whole.

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use super::backend::{
    mimetype_matches, AnalysisBackend, AnalysisError, AnalysisGranularity, AnalysisResult,
    AnalysisType,
};

/// Whisper transcription backend configuration.
#[derive(Debug, Clone)]
pub struct WhisperConfig {
    /// Whisper model to use (tiny, base, small, medium, large, turbo).
    pub model: String,
    /// Path to whisper binary (if not in PATH).
    pub binary_path: Option<std::path::PathBuf>,
    /// Language hint (auto-detect if None).
    pub language: Option<String>,
    /// Additional CLI arguments.
    pub extra_args: Vec<String>,
}

impl Default for WhisperConfig {
    fn default() -> Self {
        Self {
            model: "base".to_string(),
            binary_path: None,
            language: None,
            extra_args: Vec::new(),
        }
    }
}

/// Whisper transcription backend.
#[derive(Default)]
pub struct WhisperBackend {
    config: WhisperConfig,
}

impl WhisperBackend {
    /// Create a new Whisper backend with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with custom configuration.
    pub fn with_config(config: WhisperConfig) -> Self {
        Self { config }
    }

    /// Create with a specific model.
    pub fn with_model(model: &str) -> Self {
        Self {
            config: WhisperConfig {
                model: model.to_string(),
                ..Default::default()
            },
        }
    }

    fn whisper_binary(&self) -> &str {
        self.config
            .binary_path
            .as_ref()
            .and_then(|p| p.to_str())
            .unwrap_or("whisper")
    }

    /// Get mimetypes supported by Whisper.
    fn supported_mimetypes() -> &'static [&'static str] {
        &[
            "audio/*",
            "video/*",
            "audio/mpeg",
            "audio/mp3",
            "audio/wav",
            "audio/x-wav",
            "audio/ogg",
            "audio/flac",
            "audio/m4a",
            "audio/aac",
            "video/mp4",
            "video/webm",
            "video/x-matroska",
            "video/avi",
            "video/quicktime",
        ]
    }
}

impl AnalysisBackend for WhisperBackend {
    fn analysis_type(&self) -> AnalysisType {
        AnalysisType::Whisper
    }

    fn backend_id(&self) -> &str {
        // Return model name as backend ID
        &self.config.model
    }

    fn is_available(&self) -> bool {
        Command::new(self.whisper_binary())
            .arg("--help")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn availability_hint(&self) -> String {
        "Install whisper: pip install openai-whisper".to_string()
    }

    fn granularity(&self) -> AnalysisGranularity {
        AnalysisGranularity::Document
    }

    fn supports_mimetype(&self, mimetype: &str) -> bool {
        Self::supported_mimetypes()
            .iter()
            .any(|pattern| mimetype_matches(pattern, mimetype))
    }

    fn analyze_file(&self, file_path: &Path) -> Result<AnalysisResult, AnalysisError> {
        let start = Instant::now();

        // Create temp directory for output
        let temp_dir = tempfile::TempDir::new()?;

        // Build whisper command
        let mut cmd = Command::new(self.whisper_binary());
        cmd.arg(file_path)
            .args(["--model", &self.config.model])
            .args(["--output_format", "txt"])
            .args(["--output_dir", temp_dir.path().to_str().unwrap()]);

        if let Some(ref lang) = self.config.language {
            cmd.args(["--language", lang]);
        }

        for arg in &self.config.extra_args {
            cmd.arg(arg);
        }

        let output = cmd.output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AnalysisError::CommandFailed(format!(
                "Whisper failed: {}",
                stderr.lines().take(5).collect::<Vec<_>>().join("\n")
            )));
        }

        // Find the output file (whisper names it based on input file)
        let input_stem = file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output");
        let transcript_file = temp_dir.path().join(format!("{}.txt", input_stem));

        let text = if transcript_file.exists() {
            std::fs::read_to_string(&transcript_file).map_err(|e| {
                AnalysisError::AnalysisFailed(format!("Failed to read transcript: {}", e))
            })?
        } else {
            // Try to find any .txt file in the output directory
            let mut found_text = None;
            if let Ok(entries) = std::fs::read_dir(temp_dir.path()) {
                for entry in entries.flatten() {
                    if entry
                        .path()
                        .extension()
                        .map(|e| e == "txt")
                        .unwrap_or(false)
                    {
                        if let Ok(content) = std::fs::read_to_string(entry.path()) {
                            found_text = Some(content);
                            break;
                        }
                    }
                }
            }
            found_text.ok_or_else(|| {
                AnalysisError::AnalysisFailed("No transcript file found in output".to_string())
            })?
        };

        let metadata = serde_json::json!({
            "language": self.config.language,
        });

        Ok(AnalysisResult {
            text,
            confidence: None,
            backend: "whisper".to_string(),
            model: Some(self.config.model.clone()),
            processing_time_ms: start.elapsed().as_millis() as u64,
            metadata: Some(metadata),
        })
    }

    fn analyze_page(&self, _file_path: &Path, _page: u32) -> Result<AnalysisResult, AnalysisError> {
        Err(AnalysisError::UnsupportedOperation(
            "Whisper is document-level. Use analyze_file() instead.".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_whisper_supports_audio_video() {
        let backend = WhisperBackend::new();
        assert!(backend.supports_mimetype("audio/mp3"));
        assert!(backend.supports_mimetype("audio/wav"));
        assert!(backend.supports_mimetype("video/mp4"));
        assert!(backend.supports_mimetype("video/webm"));
        assert!(!backend.supports_mimetype("application/pdf"));
        assert!(!backend.supports_mimetype("image/png"));
    }

    #[test]
    fn test_whisper_is_document_level() {
        let backend = WhisperBackend::new();
        assert_eq!(backend.granularity(), AnalysisGranularity::Document);
    }
}
