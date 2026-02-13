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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use super::api_backend;
use super::backend::{OcrBackend, OcrBackendType, OcrConfig, OcrError};
use foiacquire::http_client::HttpClient;
use foiacquire::privacy::PrivacyConfig;

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

        let (image_base64, mime_type) = api_backend::encode_image_base64(image_path)?;
        let data_url = format!("data:{};base64,{}", mime_type, image_base64);

        let request = GroqRequest {
            model: self.model.clone(),
            messages: vec![GroqMessage {
                role: "user".to_string(),
                content: vec![
                    GroqContent::Text {
                        text: api_backend::VISION_OCR_PROMPT.to_string(),
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

        api_backend::apply_rate_delay("GROQ_DELAY_MS", 200, "Groq").await;

        let response = api_backend::retry_on_rate_limit(OcrBackendType::Groq, || {
            let h = headers.clone();
            async {
                client
                    .post_json_with_headers(
                        "https://api.groq.com/openai/v1/chat/completions",
                        &request,
                        h,
                    )
                    .await
                    .map_err(|e| OcrError::OcrFailed(format!("HTTP request failed: {}", e)))
            }
        })
        .await?;

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

        groq_response
            .choices
            .and_then(|c| c.into_iter().next())
            .map(|c| c.message.content)
            .ok_or_else(|| OcrError::OcrFailed("Groq returned no choices".to_string()))
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

    fn run_ocr(&self, image_path: &Path) -> Result<String, OcrError> {
        api_backend::block_on_async("Groq", self.run_groq_async(image_path))
    }

    fn model_name(&self) -> Option<String> {
        Some(self.model.clone())
    }
}
