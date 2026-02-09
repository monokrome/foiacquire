//! Google Gemini Vision OCR backend implementation.
//!
//! Uses Gemini's vision API for high-quality OCR via LLM.
//! Requires GEMINI_API_KEY environment variable.
//!
//! Free tier limits (Gemini 1.5 Flash):
//! - 15 requests per minute
//! - 1,500 requests per day
//!
//! Rate limiting:
//! - Set GEMINI_DELAY_MS to configure delay between requests (default: 200ms)
//! - Automatically retries on 429 with exponential backoff
//! - Respects Retry-After header from API

#![allow(dead_code)]

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Handle;
use tracing::{debug, warn};

use super::backend::{OcrBackend, OcrBackendType, OcrConfig, OcrError, OcrResult};
use super::pdf_utils;
use foiacquire::http_client::HttpClient;
use foiacquire::privacy::PrivacyConfig;
use foiacquire::rate_limit::{backoff_delay, get_delay_from_env, parse_retry_after};

/// Maximum retry attempts on rate limit errors.
const MAX_RETRIES: u32 = 5;

/// Gemini Vision OCR backend using Google's Generative AI API.
pub struct GeminiBackend {
    config: OcrConfig,
    api_key: Option<String>,
    model: String,
    privacy: PrivacyConfig,
}

#[derive(Debug, Serialize)]
struct GeminiRequest {
    contents: Vec<GeminiContent>,
    #[serde(rename = "generationConfig")]
    generation_config: GeminiGenerationConfig,
}

#[derive(Debug, Serialize)]
struct GeminiContent {
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum GeminiPart {
    Text { text: String },
    InlineData { inline_data: GeminiInlineData },
}

#[derive(Debug, Serialize)]
struct GeminiInlineData {
    mime_type: String,
    data: String,
}

#[derive(Debug, Serialize)]
struct GeminiGenerationConfig {
    temperature: f32,
    #[serde(rename = "maxOutputTokens")]
    max_output_tokens: u32,
}

#[derive(Debug, Deserialize)]
struct GeminiResponse {
    candidates: Option<Vec<GeminiCandidate>>,
    error: Option<GeminiError>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    content: GeminiResponseContent,
}

#[derive(Debug, Deserialize)]
struct GeminiResponseContent {
    parts: Vec<GeminiResponsePart>,
}

#[derive(Debug, Deserialize)]
struct GeminiResponsePart {
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeminiError {
    message: String,
}

impl GeminiBackend {
    /// Create a new Gemini backend with default configuration.
    pub fn new() -> Self {
        Self {
            config: OcrConfig::default(),
            api_key: std::env::var("GEMINI_API_KEY").ok(),
            model: "gemini-1.5-flash".to_string(),
            privacy: PrivacyConfig::default(),
        }
    }

    /// Create a new Gemini backend with custom configuration.
    pub fn with_config(config: OcrConfig) -> Self {
        Self {
            config,
            api_key: std::env::var("GEMINI_API_KEY").ok(),
            model: "gemini-1.5-flash".to_string(),
            privacy: PrivacyConfig::default(),
        }
    }

    /// Create a new Gemini backend with privacy configuration.
    pub fn with_privacy(privacy: PrivacyConfig) -> Self {
        Self {
            config: OcrConfig::default(),
            api_key: std::env::var("GEMINI_API_KEY").ok(),
            model: "gemini-1.5-flash".to_string(),
            privacy,
        }
    }

    /// Set the API key.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set the model (e.g., "gemini-1.5-flash", "gemini-1.5-pro").
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Create an HTTP client for Gemini requests.
    fn create_client(&self) -> Result<HttpClient, OcrError> {
        HttpClient::with_privacy(
            "gemini-ocr",
            Duration::from_secs(120),
            Duration::from_millis(0),
            None,
            &self.privacy,
        )
        .map_err(|e| OcrError::OcrFailed(format!("Failed to create HTTP client: {}", e)))
    }

    /// Run Gemini OCR on an image (async implementation with rate limiting).
    async fn run_gemini_async(&self, image_path: &Path) -> Result<String, OcrError> {
        let api_key = self.api_key.as_ref().ok_or_else(|| {
            OcrError::BackendNotAvailable(
                "GEMINI_API_KEY not set. Get an API key from https://ai.google.dev/".to_string(),
            )
        })?;

        let image_bytes = fs::read(image_path)?;
        let image_base64 = base64::engine::general_purpose::STANDARD.encode(&image_bytes);

        let mime_type = if image_path.extension().map(|e| e == "png").unwrap_or(false) {
            "image/png"
        } else {
            "image/jpeg"
        };

        let request = GeminiRequest {
            contents: vec![GeminiContent {
                parts: vec![
                    GeminiPart::Text {
                        text: "Extract all text from this image. Return only the extracted text, preserving the original layout and formatting as much as possible. Do not add any explanations or commentary.".to_string(),
                    },
                    GeminiPart::InlineData {
                        inline_data: GeminiInlineData {
                            mime_type: mime_type.to_string(),
                            data: image_base64,
                        },
                    },
                ],
            }],
            generation_config: GeminiGenerationConfig {
                temperature: 0.1,
                max_output_tokens: 8192,
            },
        };

        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            self.model, api_key
        );

        let client = self.create_client()?;

        // Rate limiting: wait before request (default 200ms)
        let delay = get_delay_from_env("GEMINI_DELAY_MS", 200);
        if delay > Duration::ZERO {
            debug!("Gemini: waiting {:?} before request", delay);
            tokio::time::sleep(delay).await;
        }

        // Retry loop with exponential backoff on 429
        let mut attempt = 0;
        loop {
            let response = client
                .post_json(&url, &request)
                .await
                .map_err(|e| OcrError::OcrFailed(format!("HTTP request failed: {}", e)))?;

            // Handle rate limiting (429)
            if response.status.as_u16() == 429 {
                // Get Retry-After header
                let retry_after = response.headers.get("retry-after").map(|s| s.as_str());
                let retry_after_secs = retry_after.and_then(|s| s.parse::<u64>().ok());

                if attempt >= MAX_RETRIES {
                    // Return RateLimited error so fallback chain can try next backend
                    return Err(OcrError::RateLimited {
                        backend: OcrBackendType::Gemini,
                        retry_after_secs,
                    });
                }

                let wait =
                    parse_retry_after(retry_after).unwrap_or_else(|| backoff_delay(attempt, 1000));

                warn!(
                    "Gemini rate limited (attempt {}), waiting {:?}",
                    attempt + 1,
                    wait
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
                continue;
            }

            if !response.status.is_success() {
                let status = response.status;
                let body = response.text().await.unwrap_or_default();
                return Err(OcrError::OcrFailed(format!(
                    "Gemini API error ({}): {}",
                    status, body
                )));
            }

            let gemini_response: GeminiResponse = response
                .json()
                .await
                .map_err(|e| OcrError::OcrFailed(format!("Failed to parse response: {}", e)))?;

            if let Some(error) = gemini_response.error {
                return Err(OcrError::OcrFailed(format!(
                    "Gemini API error: {}",
                    error.message
                )));
            }

            let text = gemini_response
                .candidates
                .and_then(|c| c.into_iter().next())
                .and_then(|c| c.content.parts.into_iter().next())
                .and_then(|p| p.text)
                .unwrap_or_default();

            return Ok(text);
        }
    }

    /// Run Gemini OCR on an image (blocking wrapper).
    fn run_gemini(&self, image_path: &Path) -> Result<String, OcrError> {
        let handle = Handle::try_current().map_err(|_| {
            OcrError::OcrFailed("No tokio runtime available for Gemini OCR".to_string())
        })?;

        handle.block_on(self.run_gemini_async(image_path))
    }
}

impl Default for GeminiBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OcrBackend for GeminiBackend {
    fn backend_type(&self) -> OcrBackendType {
        OcrBackendType::Gemini
    }

    fn is_available(&self) -> bool {
        self.api_key.is_some()
    }

    fn availability_hint(&self) -> String {
        if self.api_key.is_none() {
            "GEMINI_API_KEY not set. Get an API key from https://ai.google.dev/\n\
             Free tier: 15 req/min, 1,500 req/day with Gemini 1.5 Flash"
                .to_string()
        } else {
            format!("Gemini Vision is available (model: {})", self.model)
        }
    }

    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let text = self.run_gemini(image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::Gemini,
            model: Some(self.model.clone()),
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }

    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        let start = Instant::now();

        let temp_dir = TempDir::new()?;
        let image_path = pdf_utils::pdf_page_to_image(pdf_path, page, temp_dir.path())?;

        let text = self.run_gemini(&image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::Gemini,
            model: Some(self.model.clone()),
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }
}
