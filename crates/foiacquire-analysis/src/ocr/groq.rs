//! Groq Vision OCR backend implementation.
//!
//! Uses Groq's OpenAI-compatible API with vision models for OCR.
//! Requires GROQ_API_KEY environment variable.
//!
//! Free tier limits:
//! - 1,000 requests per day
//! - Vision models: Llama 4 Scout (17B), Llama 4 Maverick (17B)
//!
//! Rate limiting:
//! - Set GROQ_DELAY_MS to configure delay between requests (default: 200ms)
//! - Automatically retries on 429 with exponential backoff
//! - Respects Retry-After header from API

#![allow(dead_code)]

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

/// Groq Vision OCR backend using OpenAI-compatible API.
pub struct GroqBackend {
    config: OcrConfig,
    api_key: Option<String>,
    model: String,
    privacy: PrivacyConfig,
}

#[derive(Debug, Serialize)]
struct GroqRequest {
    model: String,
    messages: Vec<GroqMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Debug, Serialize)]
struct GroqMessage {
    role: String,
    content: Vec<GroqContent>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum GroqContent {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image_url")]
    ImageUrl { image_url: GroqImageUrl },
}

#[derive(Debug, Serialize)]
struct GroqImageUrl {
    url: String,
}

#[derive(Debug, Deserialize)]
struct GroqResponse {
    choices: Option<Vec<GroqChoice>>,
    error: Option<GroqError>,
}

#[derive(Debug, Deserialize)]
struct GroqChoice {
    message: GroqResponseMessage,
}

#[derive(Debug, Deserialize)]
struct GroqResponseMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
struct GroqError {
    message: String,
}

impl GroqBackend {
    /// Create a new Groq backend with default configuration.
    pub fn new() -> Self {
        Self {
            config: OcrConfig::default(),
            api_key: std::env::var("GROQ_API_KEY").ok(),
            model: "llama-4-scout-17b-16e-instruct".to_string(),
            privacy: PrivacyConfig::default(),
        }
    }

    /// Create a new Groq backend with custom configuration.
    pub fn with_config(config: OcrConfig) -> Self {
        Self {
            config,
            api_key: std::env::var("GROQ_API_KEY").ok(),
            model: "llama-4-scout-17b-16e-instruct".to_string(),
            privacy: PrivacyConfig::default(),
        }
    }

    /// Create a new Groq backend with privacy configuration.
    pub fn with_privacy(privacy: PrivacyConfig) -> Self {
        Self {
            config: OcrConfig::default(),
            api_key: std::env::var("GROQ_API_KEY").ok(),
            model: "llama-4-scout-17b-16e-instruct".to_string(),
            privacy,
        }
    }

    /// Set the API key.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set the model (e.g., "llama-4-scout-17b-16e-instruct", "llama-4-maverick-17b-128e-instruct").
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Create an HTTP client for Groq requests.
    fn create_client(&self) -> Result<HttpClient, OcrError> {
        HttpClient::with_privacy(
            "groq-ocr",
            Duration::from_secs(120),
            Duration::from_millis(0),
            None,
            &self.privacy,
        )
        .map_err(|e| OcrError::OcrFailed(format!("Failed to create HTTP client: {}", e)))
    }

    /// Run Groq OCR on an image (async implementation with rate limiting).
    async fn run_groq_async(&self, image_path: &Path) -> Result<String, OcrError> {
        let api_key = self.api_key.as_ref().ok_or_else(|| {
            OcrError::BackendNotAvailable(
                "GROQ_API_KEY not set. Get an API key from https://console.groq.com/".to_string(),
            )
        })?;

        let image_bytes = fs::read(image_path)?;
        let image_base64 = base64::engine::general_purpose::STANDARD.encode(&image_bytes);

        let mime_type = if image_path.extension().map(|e| e == "png").unwrap_or(false) {
            "image/png"
        } else {
            "image/jpeg"
        };

        let data_url = format!("data:{};base64,{}", mime_type, image_base64);

        let request = GroqRequest {
            model: self.model.clone(),
            messages: vec![GroqMessage {
                role: "user".to_string(),
                content: vec![
                    GroqContent::Text {
                        text: "Extract all text from this image. Return only the extracted text, preserving the original layout and formatting as much as possible. Do not add any explanations or commentary.".to_string(),
                    },
                    GroqContent::ImageUrl {
                        image_url: GroqImageUrl { url: data_url },
                    },
                ],
            }],
            max_tokens: 8192,
            temperature: 0.1,
        };

        let client = self.create_client()?;
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), format!("Bearer {}", api_key));

        // Rate limiting: wait before request (default 200ms)
        let delay = get_delay_from_env("GROQ_DELAY_MS", 200);
        if delay > Duration::ZERO {
            debug!("Groq: waiting {:?} before request", delay);
            tokio::time::sleep(delay).await;
        }

        // Retry loop with exponential backoff on 429
        let mut attempt = 0;
        loop {
            let response = client
                .post_json_with_headers(
                    "https://api.groq.com/openai/v1/chat/completions",
                    &request,
                    headers.clone(),
                )
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
                        backend: OcrBackendType::Groq,
                        retry_after_secs,
                    });
                }

                let wait =
                    parse_retry_after(retry_after).unwrap_or_else(|| backoff_delay(attempt, 1000));

                warn!(
                    "Groq rate limited (attempt {}), waiting {:?}",
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
                    "Groq API error ({}): {}",
                    status, body
                )));
            }

            let groq_response: GroqResponse = response
                .json()
                .await
                .map_err(|e| OcrError::OcrFailed(format!("Failed to parse response: {}", e)))?;

            if let Some(error) = groq_response.error {
                return Err(OcrError::OcrFailed(format!(
                    "Groq API error: {}",
                    error.message
                )));
            }

            let text = groq_response
                .choices
                .and_then(|c| c.into_iter().next())
                .map(|c| c.message.content)
                .unwrap_or_default();

            return Ok(text);
        }
    }

    /// Run Groq OCR on an image (blocking wrapper).
    fn run_groq(&self, image_path: &Path) -> Result<String, OcrError> {
        let handle = Handle::try_current().map_err(|_| {
            OcrError::OcrFailed("No tokio runtime available for Groq OCR".to_string())
        })?;

        handle.block_on(self.run_groq_async(image_path))
    }
}

impl Default for GroqBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OcrBackend for GroqBackend {
    fn backend_type(&self) -> OcrBackendType {
        OcrBackendType::Groq
    }

    fn is_available(&self) -> bool {
        self.api_key.is_some()
    }

    fn availability_hint(&self) -> String {
        if self.api_key.is_none() {
            "GROQ_API_KEY not set. Get an API key from https://console.groq.com/\n\
             Free tier: 1,000 req/day with vision models"
                .to_string()
        } else {
            format!("Groq Vision is available (model: {})", self.model)
        }
    }

    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let text = self.run_groq(image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::Groq,
            model: Some(self.model.clone()),
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }

    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        let start = Instant::now();

        let temp_dir = TempDir::new()?;
        let image_path = pdf_utils::pdf_page_to_image(pdf_path, page, temp_dir.path())?;

        let text = self.run_groq(&image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::Groq,
            model: Some(self.model.clone()),
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }
}
