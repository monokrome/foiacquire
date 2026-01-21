//! LLM client configuration.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use super::prompts::{DEFAULT_SYNOPSIS_PROMPT, DEFAULT_TAGS_PROMPT};

/// LLM provider type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LlmProvider {
    /// Ollama API (local, default)
    #[default]
    Ollama,
    /// OpenAI-compatible API (OpenAI, Groq, Together.ai, etc.)
    OpenAI,
}

impl LlmProvider {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "ollama" => Some(Self::Ollama),
            "openai" | "groq" | "together" => Some(Self::OpenAI),
            _ => None,
        }
    }
}

/// Configuration for LLM client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmConfig {
    /// Whether LLM summarization is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// LLM provider (ollama or openai)
    #[serde(default)]
    pub provider: LlmProvider,
    /// API endpoint (provider-specific defaults apply)
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    /// API key for OpenAI-compatible providers
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Model to use for summarization
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
        Self::base_default().with_env_overrides()
    }
}

impl LlmConfig {
    /// Base default without env overrides (used internally to avoid recursion).
    fn base_default() -> Self {
        Self {
            enabled: default_enabled(),
            provider: LlmProvider::default(),
            endpoint: default_endpoint(),
            api_key: None,
            model: default_model(),
            max_tokens: default_max_tokens(),
            temperature: default_temperature(),
            synopsis_prompt: None,
            tags_prompt: None,
            max_content_chars: default_max_content_chars(),
        }
    }

    /// Check if the config equals the default (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        *self == Self::base_default()
    }

    /// Apply environment variable overrides.
    ///
    /// Supported env vars:
    /// - `LLM_ENABLED`: "true" or "false"
    /// - `LLM_PROVIDER`: "ollama" (default), "openai", "groq", or "together"
    /// - `LLM_ENDPOINT`: API endpoint (defaults based on provider)
    /// - `LLM_API_KEY`: API key for OpenAI-compatible providers
    /// - `LLM_MODEL`: Model name
    /// - `LLM_MAX_TOKENS`: Maximum tokens in response
    /// - `LLM_TEMPERATURE`: Generation temperature (0.0-1.0)
    /// - `LLM_MAX_CONTENT_CHARS`: Max document chars to send
    /// - `LLM_SYNOPSIS_PROMPT`: Custom synopsis prompt
    /// - `LLM_TAGS_PROMPT`: Custom tags prompt
    ///
    /// Priority: LLM_PROVIDER wins over auto-detection from API keys.
    /// If LLM_PROVIDER=openai, uses OPENAI_API_KEY even if GROQ_API_KEY is set.
    ///
    /// For Groq, you can use:
    /// ```sh
    /// LLM_PROVIDER=groq LLM_MODEL=llama-3.1-70b-versatile
    /// # Or just set the key (auto-detects provider):
    /// GROQ_API_KEY=gsk_...
    /// ```
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(val) = std::env::var("LLM_ENABLED") {
            self.enabled = val.eq_ignore_ascii_case("true") || val == "1";
        }

        // Check if provider is explicitly set - this is authoritative
        let explicit_provider = std::env::var("LLM_PROVIDER").ok();
        if let Some(ref val) = explicit_provider {
            if let Some(provider) = LlmProvider::from_str(val) {
                self.provider = provider;
            }
        }

        // Explicit endpoint always wins
        let explicit_endpoint = std::env::var("LLM_ENDPOINT").ok();
        if let Some(ref endpoint) = explicit_endpoint {
            self.endpoint = endpoint.clone();
        }

        // Explicit API key always wins
        if let Ok(val) = std::env::var("LLM_API_KEY") {
            self.api_key = Some(val);
        }

        // If provider was explicitly set, use provider-specific defaults
        if let Some(ref provider_str) = explicit_provider {
            let provider_lower = provider_str.to_lowercase();

            // Set endpoint if not explicitly provided
            if explicit_endpoint.is_none() {
                match provider_lower.as_str() {
                    "groq" => self.endpoint = "https://api.groq.com/openai".to_string(),
                    "openai" => self.endpoint = "https://api.openai.com".to_string(),
                    "together" => self.endpoint = "https://api.together.xyz".to_string(),
                    _ => {} // ollama keeps default
                }
            }

            // Set API key from provider-specific env var if not explicitly provided
            if self.api_key.is_none() {
                match provider_lower.as_str() {
                    "groq" => self.api_key = std::env::var("GROQ_API_KEY").ok(),
                    "openai" => self.api_key = std::env::var("OPENAI_API_KEY").ok(),
                    // together uses LLM_API_KEY which we already checked
                    _ => {}
                }
            }
        } else {
            // No explicit provider - auto-detect from available keys
            if self.api_key.is_none() {
                if let Ok(key) = std::env::var("GROQ_API_KEY") {
                    self.api_key = Some(key);
                    self.provider = LlmProvider::OpenAI;
                    if explicit_endpoint.is_none() {
                        self.endpoint = "https://api.groq.com/openai".to_string();
                    }
                } else if let Ok(key) = std::env::var("OPENAI_API_KEY") {
                    self.api_key = Some(key);
                    self.provider = LlmProvider::OpenAI;
                    if explicit_endpoint.is_none() {
                        self.endpoint = "https://api.openai.com".to_string();
                    }
                }
            }
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
