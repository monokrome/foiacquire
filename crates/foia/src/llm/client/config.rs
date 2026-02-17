//! Annotation LLM client configuration.
//!
//! Split into two tiers:
//! - `LlmAppConfig`: Stored in DB, synced across devices (prompts, generation params)
//! - `LlmDeviceConfig`: From env vars, device-specific (provider, endpoint, model, api_key)
//!
//! Env vars: ANNOTATE_PROVIDER, ANNOTATE_MODEL, ANNOTATE_ENDPOINT, ANNOTATE_API_KEY
//! (legacy LLM_* names also accepted as fallback)

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

impl prefer::FromValue for LlmProvider {
    fn from_value(value: &prefer::ConfigValue) -> prefer::Result<Self> {
        match value.as_str() {
            Some(s) => match s.to_lowercase().as_str() {
                "ollama" => Ok(LlmProvider::Ollama),
                "openai" | "groq" | "together" => Ok(LlmProvider::OpenAI),
                other => Err(prefer::Error::ConversionError {
                    key: String::new(),
                    type_name: "LlmProvider".to_string(),
                    source: format!("unknown provider: {}", other).into(),
                }),
            },
            None => Err(prefer::Error::ConversionError {
                key: String::new(),
                type_name: "LlmProvider".to_string(),
                source: "expected string".into(),
            }),
        }
    }
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

/// Application-level LLM config (stored in DB, synced across devices).
/// Controls what the LLM does, not how to connect to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct LlmAppConfig {
    /// Whether LLM summarization is enabled
    #[serde(default = "default_enabled")]
    #[prefer(default)]
    pub enabled: bool,
    /// Maximum tokens in response
    #[serde(default = "default_max_tokens")]
    #[prefer(default)]
    pub max_tokens: u32,
    /// Temperature for generation (0.0 - 1.0)
    #[serde(default = "default_temperature")]
    #[prefer(default)]
    pub temperature: f32,
    /// Custom prompt for synopsis generation (uses {title} and {content} placeholders)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
    pub synopsis_prompt: Option<String>,
    /// Custom prompt for tag generation (uses {title} and {content} placeholders)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
    pub tags_prompt: Option<String>,
    /// Maximum characters of document content to send to LLM
    #[serde(default = "default_max_content_chars")]
    #[prefer(default)]
    pub max_content_chars: usize,
}

/// Device-level LLM config (from env vars, varies per device).
/// Controls how to connect to the LLM backend.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmDeviceConfig {
    /// LLM provider (ollama or openai)
    pub provider: LlmProvider,
    /// API endpoint (provider-specific defaults apply)
    pub endpoint: String,
    /// Model to use for summarization
    pub model: String,
    /// API key for OpenAI-compatible providers
    pub api_key: Option<String>,
}

/// Combined LLM configuration (runtime).
/// Merges app config (from DB) with device config (from env).
///
/// Serde: Only the app config is serialized/deserialized (DB-stored settings).
/// Device config is populated from environment variables during Default.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, prefer::FromValue)]
pub struct LlmConfig {
    /// Application-level settings (from DB)
    #[serde(flatten)]
    #[prefer(flatten)]
    pub app: LlmAppConfig,
    /// Device-level settings (from env) - not serialized
    #[serde(skip)]
    #[prefer(skip)]
    pub device: LlmDeviceConfig,
}

/// Legacy LlmConfig for serde compatibility during migration.
/// This allows reading old config files that have the flat structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmConfigLegacy {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub provider: LlmProvider,
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synopsis_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags_prompt: Option<String>,
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

// === LlmAppConfig implementations ===

impl Default for LlmAppConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_tokens: default_max_tokens(),
            temperature: default_temperature(),
            synopsis_prompt: None,
            tags_prompt: None,
            max_content_chars: default_max_content_chars(),
        }
    }
}

impl LlmAppConfig {
    /// Check if the config equals the default (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        *self == Self::default()
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

// === LlmDeviceConfig implementations ===

impl Default for LlmDeviceConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl LlmDeviceConfig {
    /// Create device config from environment variables.
    ///
    /// Env vars (ANNOTATE_* preferred, LLM_* accepted as fallback):
    /// - ANNOTATE_PROVIDER / LLM_PROVIDER: ollama, groq, openai, together
    /// - ANNOTATE_MODEL / LLM_MODEL: model ID
    /// - ANNOTATE_ENDPOINT / LLM_ENDPOINT: API base URL
    /// - ANNOTATE_API_KEY / LLM_API_KEY: API key
    pub fn from_env() -> Self {
        let mut config = Self {
            provider: LlmProvider::default(),
            endpoint: default_endpoint(),
            model: default_model(),
            api_key: None,
        };

        // Check if provider is explicitly set
        let explicit_provider = std::env::var("ANNOTATE_PROVIDER")
            .or_else(|_| std::env::var("LLM_PROVIDER"))
            .ok();
        if let Some(ref val) = explicit_provider {
            if let Some(provider) = LlmProvider::from_str(val) {
                config.provider = provider;
            }
        }

        // Explicit endpoint always wins, then OLLAMA_HOST for Ollama provider
        let explicit_endpoint = std::env::var("ANNOTATE_ENDPOINT")
            .or_else(|_| std::env::var("LLM_ENDPOINT"))
            .ok();
        if let Some(ref endpoint) = explicit_endpoint {
            config.endpoint = endpoint.clone();
        } else if let Ok(ollama_host) = std::env::var("OLLAMA_HOST") {
            config.endpoint = ollama_host;
        }

        // Explicit API key always wins
        if let Ok(val) = std::env::var("ANNOTATE_API_KEY")
            .or_else(|_| std::env::var("LLM_API_KEY"))
        {
            config.api_key = Some(val);
        }

        // Explicit model
        let explicit_model = std::env::var("ANNOTATE_MODEL")
            .or_else(|_| std::env::var("LLM_MODEL"))
            .ok();

        // If provider was explicitly set, use provider-specific defaults
        if let Some(ref provider_str) = explicit_provider {
            let provider_lower = provider_str.to_lowercase();

            // Set endpoint if not explicitly provided
            if explicit_endpoint.is_none() {
                match provider_lower.as_str() {
                    "groq" => config.endpoint = "https://api.groq.com/openai".to_string(),
                    "openai" => config.endpoint = "https://api.openai.com".to_string(),
                    "together" => config.endpoint = "https://api.together.xyz".to_string(),
                    _ => {}
                }
            }

            // Set API key from provider-specific env var if not explicitly provided
            if config.api_key.is_none() {
                match provider_lower.as_str() {
                    "groq" => config.api_key = std::env::var("GROQ_API_KEY").ok(),
                    "openai" => config.api_key = std::env::var("OPENAI_API_KEY").ok(),
                    _ => {}
                }
            }

            // Set default model for provider if not explicitly provided
            if explicit_model.is_none() {
                match provider_lower.as_str() {
                    "groq" => config.model = "llama-3.3-70b-versatile".to_string(),
                    "openai" => config.model = "gpt-4o-mini".to_string(),
                    "together" => {
                        config.model = "meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo".to_string()
                    }
                    _ => {}
                }
            }
        } else {
            // No explicit provider - auto-detect from available keys
            if config.api_key.is_none() {
                if let Ok(key) = std::env::var("GROQ_API_KEY") {
                    config.api_key = Some(key);
                    config.provider = LlmProvider::OpenAI;
                    if explicit_endpoint.is_none() {
                        config.endpoint = "https://api.groq.com/openai".to_string();
                    }
                    if config.model == default_model() {
                        config.model = "llama-3.3-70b-versatile".to_string();
                    }
                } else if let Ok(key) = std::env::var("OPENAI_API_KEY") {
                    config.api_key = Some(key);
                    config.provider = LlmProvider::OpenAI;
                    if explicit_endpoint.is_none() {
                        config.endpoint = "https://api.openai.com".to_string();
                    }
                    if config.model == default_model() {
                        config.model = "gpt-4o-mini".to_string();
                    }
                }
            }
        }

        if let Some(model) = explicit_model {
            config.model = model;
        }

        config
    }

    /// Get the provider name for display.
    pub fn provider_name(&self) -> &'static str {
        match self.provider {
            LlmProvider::Ollama => "Ollama",
            LlmProvider::OpenAI => {
                if self.endpoint.contains("groq.com") {
                    "Groq"
                } else if self.endpoint.contains("together.xyz") {
                    "Together.ai"
                } else {
                    "OpenAI"
                }
            }
        }
    }

    /// Get a provider-aware availability hint for error messages.
    pub fn availability_hint(&self) -> String {
        match self.provider {
            LlmProvider::Ollama => {
                format!(
                    "Ollama not available at {}. Make sure Ollama is running: ollama serve",
                    self.endpoint
                )
            }
            LlmProvider::OpenAI => {
                if self.api_key.is_none() {
                    "OpenAI API key not set. Set OPENAI_API_KEY or ANNOTATE_API_KEY".to_string()
                } else {
                    format!("OpenAI API not available at {}", self.endpoint)
                }
            }
        }
    }
}

// === LlmConfig (combined) implementations ===

impl LlmConfig {
    /// Create from app config (DB) and device config (env).
    pub fn new(app: LlmAppConfig, device: LlmDeviceConfig) -> Self {
        Self { app, device }
    }

    /// Create with default app config and device config from env.
    pub fn from_env() -> Self {
        Self::default()
    }

    /// Check if the config equals the default (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        self.app.is_default()
    }

    // Convenience accessors that delegate to sub-configs

    pub fn enabled(&self) -> bool {
        self.app.enabled
    }

    pub fn provider(&self) -> &LlmProvider {
        &self.device.provider
    }

    pub fn endpoint(&self) -> &str {
        &self.device.endpoint
    }

    pub fn model(&self) -> &str {
        &self.device.model
    }

    pub fn api_key(&self) -> Option<&str> {
        self.device.api_key.as_deref()
    }

    pub fn max_tokens(&self) -> u32 {
        self.app.max_tokens
    }

    pub fn temperature(&self) -> f32 {
        self.app.temperature
    }

    pub fn max_content_chars(&self) -> usize {
        self.app.max_content_chars
    }

    pub fn get_synopsis_prompt(&self) -> &str {
        self.app.get_synopsis_prompt()
    }

    pub fn get_tags_prompt(&self) -> &str {
        self.app.get_tags_prompt()
    }

    pub fn provider_name(&self) -> &'static str {
        self.device.provider_name()
    }

    pub fn availability_hint(&self) -> String {
        self.device.availability_hint()
    }

    // Setters for CLI override use cases

    pub fn set_endpoint(&mut self, endpoint: String) {
        self.device.endpoint = endpoint;
    }

    pub fn set_model(&mut self, model: String) {
        self.device.model = model;
    }
}

// === LlmConfigLegacy implementations ===

impl Default for LlmConfigLegacy {
    fn default() -> Self {
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
}

impl LlmConfigLegacy {
    /// Check if the config equals the default (for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// Convert legacy config to split config.
    /// App settings come from the legacy config, device settings from env.
    pub fn into_split_config(self) -> LlmConfig {
        let app = LlmAppConfig {
            enabled: self.enabled,
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            synopsis_prompt: self.synopsis_prompt,
            tags_prompt: self.tags_prompt,
            max_content_chars: self.max_content_chars,
        };
        // Device config always comes from env, ignoring legacy provider/endpoint/model/key
        let device = LlmDeviceConfig::from_env();
        LlmConfig::new(app, device)
    }

    /// Extract just the app config portion (for DB storage).
    pub fn to_app_config(&self) -> LlmAppConfig {
        LlmAppConfig {
            enabled: self.enabled,
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            synopsis_prompt: self.synopsis_prompt.clone(),
            tags_prompt: self.tags_prompt.clone(),
            max_content_chars: self.max_content_chars,
        }
    }
}
