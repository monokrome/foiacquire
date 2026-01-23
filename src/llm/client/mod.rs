//! LLM client for document summarization and tagging.
//!
//! Supports both Ollama (local) and OpenAI-compatible APIs (Groq, Together.ai, OpenAI).

#![allow(dead_code)]

mod config;
mod prompts;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info};

use crate::privacy::PrivacyConfig;
use crate::scrapers::HttpClient;

pub use config::{LlmConfig, LlmProvider};

/// Result of summarizing a document.
#[derive(Debug, Clone)]
pub struct SummarizeResult {
    /// Brief synopsis of the document content.
    pub synopsis: String,
    /// List of tags describing the document.
    pub tags: Vec<String>,
}

/// LLM client for document processing.
pub struct LlmClient {
    config: LlmConfig,
    privacy: PrivacyConfig,
}

// ============================================================================
// Ollama API types
// ============================================================================

/// Ollama API request format.
#[derive(Debug, Serialize)]
struct OllamaRequest {
    model: String,
    prompt: String,
    stream: bool,
    options: OllamaOptions,
}

#[derive(Debug, Serialize)]
struct OllamaOptions {
    temperature: f32,
    num_predict: u32,
}

/// Ollama API response format.
#[derive(Debug, Deserialize)]
struct OllamaResponse {
    response: String,
    #[allow(dead_code)]
    done: bool,
}

// ============================================================================
// OpenAI-compatible API types (Groq, Together.ai, OpenAI, etc.)
// ============================================================================

#[derive(Debug, Serialize)]
struct OpenAIRequest {
    model: String,
    messages: Vec<OpenAIMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Debug, Serialize)]
struct OpenAIMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct OpenAIResponse {
    choices: Vec<OpenAIChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAIChoice {
    message: OpenAIMessageResponse,
}

#[derive(Debug, Deserialize)]
struct OpenAIMessageResponse {
    content: String,
}

impl LlmClient {
    /// Create a new LLM client with the given configuration.
    ///
    /// Uses default privacy configuration (no Tor/proxy).
    pub fn new(config: LlmConfig) -> Self {
        Self {
            config,
            privacy: PrivacyConfig::default(),
        }
    }

    /// Create a new LLM client with privacy configuration.
    ///
    /// External LLM services (OpenAI, Groq, etc.) will route through Tor/SOCKS if configured.
    /// Local Ollama instances are not affected by privacy settings.
    pub fn with_privacy(config: LlmConfig, privacy: PrivacyConfig) -> Self {
        Self { config, privacy }
    }

    /// Get the config.
    pub fn config(&self) -> &LlmConfig {
        &self.config
    }

    /// Create an HTTP client for LLM requests.
    fn create_client(&self) -> Result<HttpClient, Box<dyn std::error::Error>> {
        HttpClient::with_privacy(
            "llm",
            Duration::from_secs(300), // 5 min timeout for slow models
            Duration::from_millis(0), // No rate limiting for LLM
            None,                     // Use default user agent
            &self.privacy,
        )
        .map_err(|e| e.into())
    }

    /// Check if the LLM service is available.
    ///
    /// For OpenAI-compatible APIs (Groq, OpenAI, Together), this checks that an API key is set.
    /// For Ollama, this makes an HTTP call to verify the service is running.
    pub async fn is_available(&self) -> bool {
        if !self.config.enabled() {
            return false;
        }

        // For OpenAI-compatible APIs, just check that we have an API key
        // Making an HTTP call to /v1/models can be slow and may have rate limits
        if matches!(self.config.provider(), LlmProvider::OpenAI) {
            return self.config.api_key().is_some();
        }

        // For Ollama, check if the service is actually running
        let client = match self.create_client() {
            Ok(c) => c,
            Err(_) => return false,
        };

        let url = format!("{}/api/tags", self.config.endpoint());
        let response = client.get(&url, None, None).await;

        match response {
            Ok(resp) => resp.status.is_success(),
            Err(_) => false,
        }
    }

    /// List available models.
    pub async fn list_models(&self) -> Result<Vec<String>, LlmError> {
        match self.config.provider() {
            LlmProvider::Ollama => self.list_models_ollama().await,
            LlmProvider::OpenAI => self.list_models_openai().await,
        }
    }

    async fn list_models_ollama(&self) -> Result<Vec<String>, LlmError> {
        let client = self
            .create_client()
            .map_err(|e| LlmError::Connection(e.to_string()))?;

        let url = format!("{}/api/tags", self.config.endpoint());
        let resp = client
            .get(&url, None, None)
            .await
            .map_err(|e| LlmError::Connection(e.to_string()))?;

        if !resp.status.is_success() {
            return Err(LlmError::Api(format!("HTTP {}", resp.status)));
        }

        #[derive(Deserialize)]
        struct TagsResponse {
            models: Vec<ModelInfo>,
        }

        #[derive(Deserialize)]
        struct ModelInfo {
            name: String,
        }

        let tags: TagsResponse = resp
            .json()
            .await
            .map_err(|e| LlmError::Parse(e.to_string()))?;

        Ok(tags.models.into_iter().map(|m| m.name).collect())
    }

    async fn list_models_openai(&self) -> Result<Vec<String>, LlmError> {
        let client = self
            .create_client()
            .map_err(|e| LlmError::Connection(e.to_string()))?;

        let url = format!("{}/v1/models", self.config.endpoint());

        let resp = if let Some(api_key) = self.config.api_key() {
            let mut headers = HashMap::new();
            headers.insert("Authorization".to_string(), format!("Bearer {}", api_key));
            client.get_with_headers(&url, headers).await
        } else {
            client.get(&url, None, None).await
        }
        .map_err(|e| LlmError::Connection(e.to_string()))?;

        if !resp.status.is_success() {
            return Err(LlmError::Api(format!("HTTP {}", resp.status)));
        }

        #[derive(Deserialize)]
        struct ModelsResponse {
            data: Vec<ModelInfo>,
        }

        #[derive(Deserialize)]
        struct ModelInfo {
            id: String,
        }

        let models: ModelsResponse = resp
            .json()
            .await
            .map_err(|e| LlmError::Parse(e.to_string()))?;

        Ok(models.data.into_iter().map(|m| m.id).collect())
    }

    /// Generate synopsis for a document.
    pub async fn generate_synopsis(&self, text: &str, title: &str) -> Result<String, LlmError> {
        let truncated = self.truncate_content(text);
        let prompt = self
            .config
            .get_synopsis_prompt()
            .replace("{title}", title)
            .replace("{content}", truncated);

        debug!("Generating synopsis for: {}", title);
        let response = self.call_llm(&prompt).await?;

        // Clean up the response
        let synopsis = response.trim().to_string();
        if synopsis.is_empty() {
            return Err(LlmError::Parse("Empty synopsis response".to_string()));
        }

        Ok(synopsis)
    }

    /// Generate tags for a document.
    pub async fn generate_tags(&self, text: &str, title: &str) -> Result<Vec<String>, LlmError> {
        let truncated = self.truncate_content(text);
        let prompt = self
            .config
            .get_tags_prompt()
            .replace("{title}", title)
            .replace("{content}", truncated);

        debug!("Generating tags for: {}", title);
        let response = self.call_llm(&prompt).await?;

        // Parse tags from response
        let tags = self.parse_tags(&response);
        if tags.is_empty() {
            return Err(LlmError::Parse("No tags parsed from response".to_string()));
        }

        Ok(tags)
    }

    /// Summarize a document (generates both synopsis and tags sequentially).
    pub async fn summarize(&self, text: &str, title: &str) -> Result<SummarizeResult, LlmError> {
        info!("Summarizing document: {}", title);

        // Run synopsis and tags generation sequentially to avoid memory pressure
        let synopsis = self.generate_synopsis(text, title).await?;
        let tags = self.generate_tags(text, title).await?;

        Ok(SummarizeResult { synopsis, tags })
    }

    /// Expand search terms using LLM to generate related terms.
    /// Takes seed terms and a domain description, returns expanded list.
    pub async fn expand_search_terms(
        &self,
        seed_terms: &[String],
        domain: &str,
    ) -> Result<Vec<String>, LlmError> {
        if seed_terms.is_empty() {
            return Ok(Vec::new());
        }

        let seeds = seed_terms.join(", ");
        let prompt = format!(
            r#"You are helping to expand search terms for finding declassified government documents related to: {domain}

Given these seed search terms: {seeds}

Generate an exhaustive comma-separated list of related search terms that would help find more relevant documents. Include:
- Synonyms and alternative phrasings
- Related programs, operations, or projects
- Key people, agencies, or organizations involved
- Related events, locations, or time periods
- Technical terms and code names
- Broader and narrower terms

Focus on terms specifically relevant to {domain}. Return ONLY a comma-separated list of terms, no explanations or categories. Aim for 50-100 terms."#,
            domain = domain,
            seeds = seeds
        );

        debug!("Expanding search terms for: {}", domain);
        let response = self.call_llm(&prompt).await?;

        // Parse the response into individual terms
        let expanded: Vec<String> = response
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty() && t.len() <= 100)
            .collect();

        info!(
            "Expanded {} seed terms to {} total terms",
            seed_terms.len(),
            expanded.len()
        );
        Ok(expanded)
    }

    /// Truncate content to configured maximum (UTF-8 safe).
    fn truncate_content<'a>(&self, text: &'a str) -> &'a str {
        let max_chars = self.config.max_content_chars();
        if text.len() <= max_chars {
            return text;
        }
        // Find a valid UTF-8 boundary at or before max_content_chars
        let mut end = max_chars;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        &text[..end]
    }

    /// Call LLM API with a prompt (provider-aware).
    async fn call_llm(&self, prompt: &str) -> Result<String, LlmError> {
        match self.config.provider() {
            LlmProvider::Ollama => self.call_ollama(prompt).await,
            LlmProvider::OpenAI => self.call_openai(prompt).await,
        }
    }

    /// Call Ollama API with a prompt.
    async fn call_ollama(&self, prompt: &str) -> Result<String, LlmError> {
        let client = self
            .create_client()
            .map_err(|e| LlmError::Connection(e.to_string()))?;

        let request = OllamaRequest {
            model: self.config.model().to_string(),
            prompt: prompt.to_string(),
            stream: false,
            options: OllamaOptions {
                temperature: self.config.temperature(),
                num_predict: self.config.max_tokens(),
            },
        };

        let url = format!("{}/api/generate", self.config.endpoint());
        let resp = client
            .post_json(&url, &request)
            .await
            .map_err(|e| LlmError::Connection(e.to_string()))?;

        if !resp.status.is_success() {
            let status = resp.status;
            let body = resp.text().await.unwrap_or_default();
            return Err(LlmError::Api(format!("HTTP {}: {}", status, body)));
        }

        let ollama_resp: OllamaResponse = resp
            .json()
            .await
            .map_err(|e| LlmError::Parse(e.to_string()))?;

        Ok(ollama_resp.response)
    }

    /// Call OpenAI-compatible API (Groq, Together.ai, OpenAI, etc.)
    async fn call_openai(&self, prompt: &str) -> Result<String, LlmError> {
        let client = self
            .create_client()
            .map_err(|e| LlmError::Connection(e.to_string()))?;

        let request = OpenAIRequest {
            model: self.config.model().to_string(),
            messages: vec![OpenAIMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
            max_tokens: self.config.max_tokens(),
            temperature: self.config.temperature(),
        };

        let url = format!("{}/v1/chat/completions", self.config.endpoint());

        let resp = if let Some(api_key) = self.config.api_key() {
            let mut headers = HashMap::new();
            headers.insert("Authorization".to_string(), format!("Bearer {}", api_key));
            client.post_json_with_headers(&url, &request, headers).await
        } else {
            client.post_json(&url, &request).await
        }
        .map_err(|e| LlmError::Connection(e.to_string()))?;

        if !resp.status.is_success() {
            let status = resp.status;
            let body = resp.text().await.unwrap_or_default();
            return Err(LlmError::Api(format!("HTTP {}: {}", status, body)));
        }

        let openai_resp: OpenAIResponse = resp
            .json()
            .await
            .map_err(|e| LlmError::Parse(e.to_string()))?;

        openai_resp
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| LlmError::Parse("No response choices".to_string()))
    }

    /// Parse tags from LLM response.
    fn parse_tags(&self, response: &str) -> Vec<String> {
        // Remove common prefixes/formatting
        let cleaned = response
            .trim()
            .trim_start_matches("Tags:")
            .trim_start_matches("TAGS:")
            .trim_start_matches('[')
            .trim_end_matches(']')
            .trim();

        cleaned
            .split(',')
            .map(|t| {
                t.trim()
                    .to_lowercase()
                    // Allow colons for hierarchical tags (agency:fbi, topic:surveillance)
                    .trim_matches(|c: char| {
                        !c.is_alphanumeric() && c != '-' && c != '_' && c != ':'
                    })
                    .to_string()
            })
            .filter(|t| !t.is_empty() && t.len() <= 50)
            .take(10) // Max 10 tags
            .collect()
    }
}

/// Errors that can occur during LLM operations.
#[derive(Debug)]
pub enum LlmError {
    /// Failed to connect to LLM service
    Connection(String),
    /// API returned an error
    Api(String),
    /// Failed to parse response
    Parse(String),
    /// Model not available
    ModelNotFound(String),
    /// LLM is disabled
    Disabled,
}

impl std::fmt::Display for LlmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LlmError::Connection(msg) => write!(f, "Connection error: {}", msg),
            LlmError::Api(msg) => write!(f, "API error: {}", msg),
            LlmError::Parse(msg) => write!(f, "Parse error: {}", msg),
            LlmError::ModelNotFound(msg) => write!(f, "Model not found: {}", msg),
            LlmError::Disabled => write!(f, "LLM is disabled"),
        }
    }
}

impl std::error::Error for LlmError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tags() {
        let client = LlmClient::new(LlmConfig::default());

        // Simple comma-separated
        let tags = client.parse_tags("budget, policy, environmental, 2023");
        assert_eq!(tags, vec!["budget", "policy", "environmental", "2023"]);

        // With brackets
        let tags = client.parse_tags("[budget, policy, environmental]");
        assert_eq!(tags, vec!["budget", "policy", "environmental"]);

        // With prefix
        let tags = client.parse_tags("Tags: budget, policy, memo");
        assert_eq!(tags, vec!["budget", "policy", "memo"]);

        // Mixed case
        let tags = client.parse_tags("Budget, POLICY, Environmental");
        assert_eq!(tags, vec!["budget", "policy", "environmental"]);

        // Simple tags (no prefixes)
        let tags = client.parse_tags("fbi, surveillance, memo");
        assert_eq!(tags, vec!["fbi", "surveillance", "memo"]);

        // Hyphenated multi-word tags
        let tags = client.parse_tags("state-dept, cold-war, mind-control");
        assert_eq!(tags, vec!["state-dept", "cold-war", "mind-control"]);

        // Real-world example
        let tags = client.parse_tags("cia, mkultra, cold-war, memo");
        assert_eq!(tags, vec!["cia", "mkultra", "cold-war", "memo"]);
    }

    #[test]
    fn test_default_config() {
        let config = LlmConfig::default();
        assert!(config.enabled());
        assert!(config.model().contains("dolphin"));
        assert!(config.app.synopsis_prompt.is_none());
        assert!(config.get_synopsis_prompt().contains("{title}"));
    }
}
