//! LLM client configuration.

use serde::{Deserialize, Serialize};

use super::prompts::{DEFAULT_SYNOPSIS_PROMPT, DEFAULT_TAGS_PROMPT};

/// Configuration for LLM client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmConfig {
    /// Whether LLM summarization is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Ollama API endpoint (default: http://localhost:11434)
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    /// Model to use for summarization (default: llama3.2:instruct)
    #[serde(default = "default_model")]
    pub model: String,
    /// Maximum tokens in response
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Temperature for generation (0.0 - 1.0)
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    /// Custom prompt for synopsis generation (uses {title} and {content} placeholders)
    #[serde(default)]
    pub synopsis_prompt: Option<String>,
    /// Custom prompt for tag generation (uses {title} and {content} placeholders)
    #[serde(default)]
    pub tags_prompt: Option<String>,
    /// Maximum characters of document content to send to LLM
    #[serde(default = "default_max_content_chars")]
    pub max_content_chars: usize,
}

fn default_enabled() -> bool {
    true
}

fn default_endpoint() -> String {
    "http://localhost:11434".to_string()
}

fn default_model() -> String {
    "dolphin-llama3:8b".to_string()
}

fn default_max_tokens() -> u32 {
    512
}

fn default_temperature() -> f32 {
    0.3
}

fn default_max_content_chars() -> usize {
    12000
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            endpoint: default_endpoint(),
            model: default_model(),
            max_tokens: default_max_tokens(),
            temperature: default_temperature(),
            synopsis_prompt: None,
            tags_prompt: None,
            max_content_chars: default_max_content_chars(),
        }
    }
}

impl LlmConfig {
    /// Check if the config equals the default (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// Apply environment variable overrides.
    ///
    /// Supported env vars:
    /// - `LLM_ENABLED`: "true" or "false"
    /// - `LLM_ENDPOINT`: Ollama API endpoint
    /// - `LLM_MODEL`: Model name
    /// - `LLM_MAX_TOKENS`: Maximum tokens in response
    /// - `LLM_TEMPERATURE`: Generation temperature (0.0-1.0)
    /// - `LLM_MAX_CONTENT_CHARS`: Max document chars to send
    /// - `LLM_SYNOPSIS_PROMPT`: Custom synopsis prompt
    /// - `LLM_TAGS_PROMPT`: Custom tags prompt
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(val) = std::env::var("LLM_ENABLED") {
            self.enabled = val.eq_ignore_ascii_case("true") || val == "1";
        }
        if let Ok(val) = std::env::var("LLM_ENDPOINT") {
            self.endpoint = val;
        }
        if let Ok(val) = std::env::var("LLM_MODEL") {
            self.model = val;
        }
        if let Ok(val) = std::env::var("LLM_MAX_TOKENS") {
            if let Ok(n) = val.parse() {
                self.max_tokens = n;
            }
        }
        if let Ok(val) = std::env::var("LLM_TEMPERATURE") {
            if let Ok(t) = val.parse() {
                self.temperature = t;
            }
        }
        if let Ok(val) = std::env::var("LLM_MAX_CONTENT_CHARS") {
            if let Ok(n) = val.parse() {
                self.max_content_chars = n;
            }
        }
        if let Ok(val) = std::env::var("LLM_SYNOPSIS_PROMPT") {
            self.synopsis_prompt = Some(val);
        }
        if let Ok(val) = std::env::var("LLM_TAGS_PROMPT") {
            self.tags_prompt = Some(val);
        }
        self
    }

    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = endpoint.to_string();
        self
    }

    pub fn with_model(mut self, model: &str) -> Self {
        self.model = model.to_string();
        self
    }

    /// Get the synopsis prompt, using custom or default.
    pub fn get_synopsis_prompt(&self) -> &str {
        self.synopsis_prompt
            .as_deref()
            .unwrap_or(DEFAULT_SYNOPSIS_PROMPT)
    }

    /// Get the tags prompt, using custom or default.
    pub fn get_tags_prompt(&self) -> &str {
        self.tags_prompt.as_deref().unwrap_or(DEFAULT_TAGS_PROMPT)
    }
}
