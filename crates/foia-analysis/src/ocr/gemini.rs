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

use serde::{Deserialize, Serialize};
use std::path::Path;

use super::api_backend;
use super::backend::{BackendConfig, OcrBackend, OcrBackendType, OcrConfig, OcrError};

/// Gemini Vision OCR backend using Google's Generative AI API.
pub struct GeminiBackend {
    config: BackendConfig,
    api_key: Option<String>,
    model: String,
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
            config: BackendConfig::new(),
            api_key: std::env::var("GEMINI_API_KEY").ok(),
            model: "gemini-1.5-flash".to_string(),
        }
    }

    /// Create a new Gemini backend with custom configuration.
    pub fn with_config(config: OcrConfig) -> Self {
        Self {
            config: BackendConfig::with_config(config),
            api_key: std::env::var("GEMINI_API_KEY").ok(),
            model: "gemini-1.5-flash".to_string(),
        }
    }

    /// Create a new Gemini backend from a full backend configuration.
    pub fn from_backend_config(config: BackendConfig) -> Self {
        Self {
            config,
            api_key: std::env::var("GEMINI_API_KEY").ok(),
            model: "gemini-1.5-flash".to_string(),
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

    /// Run Gemini OCR on an image (async implementation with rate limiting).
    async fn run_gemini_async(&self, image_path: &Path) -> Result<String, OcrError> {
        let api_key = self.api_key.as_ref().ok_or_else(|| {
            OcrError::BackendNotAvailable(
                "GEMINI_API_KEY not set. Get an API key from https://ai.google.dev/".to_string(),
            )
        })?;

        let (image_base64, mime_type) = api_backend::encode_image_base64(image_path)?;

        let request = GeminiRequest {
            contents: vec![GeminiContent {
                parts: vec![
                    GeminiPart::Text {
                        text: api_backend::VISION_OCR_PROMPT.to_string(),
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

        let client = self.config.create_http_client("gemini-ocr")?;

        api_backend::apply_rate_delay("GEMINI_DELAY_MS", 200, "Gemini").await;

        let response = api_backend::retry_on_rate_limit(OcrBackendType::Gemini, || async {
            client
                .post_json(&url, &request)
                .await
                .map_err(|e| OcrError::OcrFailed(format!("HTTP request failed: {}", e)))
        })
        .await?;

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

        gemini_response
            .candidates
            .and_then(|c| c.into_iter().next())
            .and_then(|c| c.content.parts.into_iter().next())
            .and_then(|p| p.text)
            .ok_or_else(|| OcrError::OcrFailed("Gemini returned no candidates".to_string()))
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

    fn run_ocr(&self, image_path: &Path) -> Result<String, OcrError> {
        api_backend::block_on_async("Gemini", self.run_gemini_async(image_path))
    }

    fn model_name(&self) -> Option<String> {
        Some(self.model.clone())
    }
}
