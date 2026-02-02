//! Configuration management for FOIAcquire using the prefer crate.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::llm::LlmConfig;
use crate::prefer_db::FoiaConfigLoader;
use crate::privacy::PrivacyConfig;
use crate::repository::diesel_context::DieselDbContext;
use crate::repository::util::{is_postgres_url, validate_database_url};
use crate::scrapers::{ScraperConfig, ViaMode};

/// Default refresh TTL in days (14 days).
pub const DEFAULT_REFRESH_TTL_DAYS: u64 = 14;

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
    vec![BackendEntry::Single("tesseract".to_string())]
}

impl Default for OcrConfig {
    fn default() -> Self {
        Self {
            backends: default_ocr_backends(),
        }
    }
}

impl OcrConfig {
    /// Check if this is the default config.
    pub fn is_default(&self) -> bool {
        self.backends.len() == 1
            && matches!(&self.backends[0], BackendEntry::Single(s) if s == "tesseract")
    }
}

/// Analysis configuration for text extraction methods.
#[derive(Debug, Clone, Default, Serialize, Deserialize, prefer::FromValue)]
pub struct AnalysisConfig {
    /// OCR backend configuration with fallback support.
    #[serde(default, skip_serializing_if = "OcrConfig::is_default")]
    #[prefer(default)]
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

/// Default database filename.
pub const DEFAULT_DATABASE_FILENAME: &str = "foiacquire.db";

/// Default documents subdirectory name.
const DOCUMENTS_SUBDIR: &str = "documents";

/// Application settings.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Base data directory.
    pub data_dir: PathBuf,
    /// Database filename.
    pub database_filename: String,
    /// Database URL (overrides data_dir/database_filename if set).
    /// Supports sqlite:// URLs. Set via DATABASE_URL env var or config.
    pub database_url: Option<String>,
    /// Directory for storing documents.
    pub documents_dir: PathBuf,
    /// User agent for HTTP requests.
    pub user_agent: String,
    /// Request timeout in seconds.
    pub request_timeout: u64,
    /// Delay between requests in milliseconds.
    pub request_delay_ms: u64,
    /// Rate limit backend URL (None = in-memory, "sqlite" = local DB, "redis://..." = Redis).
    pub rate_limit_backend: Option<String>,
    /// Worker queue broker URL (None = local DB, "amqp://..." = RabbitMQ).
    pub broker_url: Option<String>,
    /// Disable TLS for PostgreSQL connections.
    pub no_tls: bool,
}

impl Default for Settings {
    fn default() -> Self {
        // Default to ~/Documents/foia/ for user data
        // Falls back gracefully: Documents dir -> Home dir -> Current dir
        let data_dir = dirs::document_dir()
            .or_else(dirs::home_dir)
            .unwrap_or_else(|| PathBuf::from("."))
            .join("foia");

        Self {
            documents_dir: data_dir.join(DOCUMENTS_SUBDIR),
            data_dir,
            database_filename: DEFAULT_DATABASE_FILENAME.to_string(),
            database_url: None,
            user_agent: "FOIAcquire/0.1 (academic research)".to_string(),
            request_timeout: 30,
            request_delay_ms: 500,
            rate_limit_backend: None, // In-memory by default
            broker_url: None,         // Local DB by default
            no_tls: false,
        }
    }
}

impl Settings {
    /// Create settings with a custom data directory.
    #[allow(dead_code)]
    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            documents_dir: data_dir.join(DOCUMENTS_SUBDIR),
            data_dir,
            ..Default::default()
        }
    }

    /// Get the database URL, constructing from path if not explicitly set.
    pub fn database_url(&self) -> String {
        if let Some(ref url) = self.database_url {
            url.clone()
        } else {
            let path = self.data_dir.join(&self.database_filename);
            format!("sqlite:{}", path.display())
        }
    }

    /// Check if using an explicit database URL (vs file path).
    pub fn has_database_url(&self) -> bool {
        self.database_url.is_some()
    }

    /// Check if using PostgreSQL (vs SQLite).
    #[allow(dead_code)]
    pub fn is_postgres(&self) -> bool {
        self.database_url
            .as_ref()
            .is_some_and(|url| is_postgres_url(url))
    }

    /// Get the full path to the database (for SQLite file-based databases).
    pub fn database_path(&self) -> PathBuf {
        self.data_dir.join(&self.database_filename)
    }

    /// Check if the database appears to be initialized.
    /// For SQLite: checks if the database file exists.
    /// For PostgreSQL: always returns true (connection errors handled elsewhere).
    pub fn database_exists(&self) -> bool {
        if self.has_database_url() {
            true // PostgreSQL - assume it exists, connection errors handled elsewhere
        } else {
            self.database_path().exists()
        }
    }

    /// Ensure all directories exist.
    pub fn ensure_directories(&self) -> std::io::Result<()> {
        // Log diagnostics for debugging permission issues in containers (Unix only)
        #[cfg(unix)]
        {
            Self::log_directory_diagnostics(&self.data_dir, "data_dir");
            Self::log_directory_diagnostics(&self.documents_dir, "documents_dir");
        }

        fs::create_dir_all(&self.data_dir).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to create data directory '{}': {}",
                    self.data_dir.display(),
                    e
                ),
            )
        })?;
        fs::create_dir_all(&self.documents_dir).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to create documents directory '{}': {}",
                    self.documents_dir.display(),
                    e
                ),
            )
        })?;
        Ok(())
    }

    /// Log diagnostic information about a directory for debugging (Unix only).
    #[cfg(unix)]
    fn log_directory_diagnostics(path: &Path, label: &str) {
        use std::os::unix::fs::MetadataExt;
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        tracing::debug!(
            "{} check: path={}, running as uid={} gid={}",
            label,
            path.display(),
            uid,
            gid
        );

        if path.exists() {
            if let Ok(meta) = fs::metadata(path) {
                tracing::debug!(
                    "{} exists: owner={}:{}, mode={:o}, is_dir={}",
                    label,
                    meta.uid(),
                    meta.gid(),
                    meta.mode() & 0o7777,
                    meta.is_dir()
                );
            } else {
                tracing::debug!("{} exists but metadata read failed", label);
            }
        } else {
            tracing::debug!("{} does not exist, will attempt to create", label);
            if let Some(parent) = path.parent() {
                if parent.exists() {
                    if let Ok(meta) = fs::metadata(parent) {
                        tracing::debug!(
                            "{} parent exists: path={}, owner={}:{}, mode={:o}",
                            label,
                            parent.display(),
                            meta.uid(),
                            meta.gid(),
                            meta.mode() & 0o7777
                        );
                    }
                } else {
                    tracing::debug!("{} parent does not exist: {}", label, parent.display());
                }
            }
        }
    }

    /// Create a database context using the configured database URL or path.
    ///
    /// This is the preferred way to get a DieselDbContext from settings.
    /// Returns an error if the database URL is invalid.
    pub fn create_db_context(&self) -> Result<DieselDbContext, diesel::result::Error> {
        DieselDbContext::from_url(&self.database_url(), self.no_tls)
    }

    /// Create a database context and verify the connection works.
    ///
    /// This is useful for failing fast at startup if the database is unreachable.
    /// For PostgreSQL, this validates credentials and network connectivity.
    /// For SQLite, this creates the database file if it doesn't exist.
    #[allow(dead_code)]
    pub async fn create_db_context_validated(&self) -> Result<DieselDbContext, String> {
        let ctx = self
            .create_db_context()
            .map_err(|e| format!("Failed to create database context: {}", e))?;
        ctx.test_connection()
            .await
            .map_err(|e| format!("Failed to connect to database: {}", e))?;
        Ok(ctx)
    }
}

/// Configuration file structure.
#[derive(Debug, Clone, Default, Serialize, Deserialize, prefer::FromValue)]
pub struct Config {
    /// Data directory path.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "target")]
    pub data_dir: Option<String>,
    /// Database filename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// User agent string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Request timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout: Option<u64>,
    /// Delay between requests in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_delay_ms: Option<u64>,
    /// Rate limit backend URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_backend: Option<String>,
    /// Worker queue broker URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_url: Option<String>,
    /// Default refresh TTL in days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_refresh_ttl_days: Option<u64>,
    /// Scraper configurations.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[prefer(default)]
    pub scrapers: HashMap<String, ScraperConfig>,
    /// LLM configuration for document summarization.
    #[serde(default, skip_serializing_if = "LlmConfig::is_default")]
    #[prefer(default)]
    pub llm: LlmConfig,
    /// Analysis configuration for text extraction methods.
    #[serde(default, skip_serializing_if = "AnalysisConfig::is_default")]
    #[prefer(default)]
    pub analysis: AnalysisConfig,
    /// Privacy configuration for Tor and proxy routing.
    #[serde(default, skip_serializing_if = "PrivacyConfig::is_default")]
    #[prefer(default)]
    pub privacy: PrivacyConfig,
    /// URL rewriting for caching proxies (CDN bypass).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[prefer(default)]
    pub via: HashMap<String, String>,
    /// Via proxy mode.
    #[serde(default, skip_serializing_if = "is_via_mode_default")]
    #[prefer(default)]
    pub via_mode: ViaMode,
    /// Path to the config file this was loaded from (not serialized).
    #[serde(skip)]
    #[prefer(skip)]
    pub source_path: Option<PathBuf>,
}

fn is_via_mode_default(mode: &ViaMode) -> bool {
    *mode == ViaMode::default()
}

/// App-level configuration snapshot for database storage.
/// Contains only settings that should be synced across devices.
/// Excludes device-specific (data_dir, privacy) and bootstrap (rate_limit_backend, broker_url) settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, prefer::FromValue)]
pub struct AppConfigSnapshot {
    /// User agent string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Request timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout: Option<u64>,
    /// Delay between requests in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_delay_ms: Option<u64>,
    /// Default refresh TTL in days for re-checking fetched URLs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_refresh_ttl_days: Option<u64>,
    /// Scraper configurations.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[prefer(default)]
    pub scrapers: HashMap<String, ScraperConfig>,
    /// LLM configuration (app portion only - device settings excluded via serde skip).
    #[serde(default, skip_serializing_if = "LlmConfig::is_default")]
    #[prefer(default)]
    pub llm: LlmConfig,
    /// Analysis configuration for text extraction methods.
    #[serde(default, skip_serializing_if = "AnalysisConfig::is_default")]
    #[prefer(default)]
    pub analysis: AnalysisConfig,
    /// URL rewriting for caching proxies (CDN bypass).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[prefer(default)]
    pub via: HashMap<String, String>,
    /// Via proxy mode - controls when via mappings are used for requests.
    #[serde(default, skip_serializing_if = "is_via_mode_default")]
    #[prefer(default)]
    pub via_mode: ViaMode,
}

impl Config {
    /// Load configuration using prefer crate for discovery.
    /// Automatically discovers foiacquire config files in standard locations.
    pub async fn load() -> Self {
        // Use prefer for file discovery, then parse with serde
        match prefer::load("foiacquire").await {
            Ok(pref_config) => {
                // Get the discovered file path and load with serde
                if let Some(path) = pref_config.source_path() {
                    match Self::load_from_path(path).await {
                        Ok(config) => config,
                        Err(_) => Self::default_with_env(),
                    }
                } else {
                    Self::default_with_env()
                }
            }
            Err(_) => {
                // No config file found, use defaults with env overrides
                Self::default_with_env()
            }
        }
    }

    /// Create a default config with environment variable overrides applied.
    /// Note: This is now equivalent to `Self::default()` since sub-configs
    /// apply env overrides in their own Default implementations.
    pub fn default_with_env() -> Self {
        Self::default()
    }

    /// Load configuration from a specific file path.
    /// Supports JSON, TOML, YAML, and other formats based on file extension.
    pub async fn load_from_path(path: &Path) -> Result<Self, String> {
        let contents = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| format!("Failed to read config file: {}", e))?;

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("json");

        let mut config: Config = match ext {
            "toml" => toml::from_str(&contents)
                .map_err(|e| format!("Failed to parse TOML config: {}", e))?,
            "yaml" | "yml" => serde_yaml::from_str(&contents)
                .map_err(|e| format!("Failed to parse YAML config: {}", e))?,
            _ => serde_json::from_str(&contents)
                .map_err(|e| format!("Failed to parse JSON config: {}", e))?,
        };

        config.source_path = Some(path.to_path_buf());
        // Note: LlmConfig device settings are auto-populated from env via Default
        config.privacy = config.privacy.with_env_overrides();
        Ok(config)
    }

    /// Get the base directory for resolving relative paths.
    /// Returns the config file's parent directory if available, otherwise None.
    pub fn base_dir(&self) -> Option<PathBuf> {
        self.source_path
            .as_ref()
            .and_then(|p| p.parent().map(|p| p.to_path_buf()))
    }

    /// Resolve a path that may be relative to the config file.
    /// - Absolute paths are returned as-is
    /// - Paths starting with ~ are expanded
    /// - Relative paths are resolved relative to `base_dir` (config file location or CWD)
    pub fn resolve_path(&self, path_str: &str, base_dir: &Path) -> PathBuf {
        let expanded = shellexpand::tilde(path_str);
        let path = Path::new(expanded.as_ref());

        if path.is_absolute() {
            path.to_path_buf()
        } else {
            base_dir.join(path)
        }
    }

    /// Apply configuration to settings.
    /// `base_dir` is used to resolve relative paths (typically config file dir or CWD).
    pub fn apply_to_settings(&self, settings: &mut Settings, base_dir: &Path) {
        if let Some(ref data_dir) = self.data_dir {
            settings.data_dir = self.resolve_path(data_dir, base_dir);
            settings.documents_dir = settings.data_dir.join(DOCUMENTS_SUBDIR);
        }
        if let Some(ref database) = self.database {
            settings.database_filename = database.clone();
        }
        if let Some(ref user_agent) = self.user_agent {
            settings.user_agent = user_agent.clone();
        }
        if let Some(timeout) = self.request_timeout {
            settings.request_timeout = timeout;
        }
        if let Some(delay) = self.request_delay_ms {
            settings.request_delay_ms = delay;
        }
        if let Some(ref backend) = self.rate_limit_backend {
            settings.rate_limit_backend = Some(backend.clone());
        }
        if let Some(ref broker) = self.broker_url {
            settings.broker_url = Some(broker.clone());
        }
    }

    /// Get the effective refresh TTL in days for a scraper.
    /// Priority: scraper config > global config > default constant.
    pub fn get_refresh_ttl_days(&self, source_id: &str) -> u64 {
        // First check scraper-specific config
        if let Some(scraper_config) = self.scrapers.get(source_id) {
            if let Some(ttl) = scraper_config.refresh_ttl_days {
                return ttl;
            }
        }
        // Fall back to global config or default
        self.default_refresh_ttl_days
            .unwrap_or(DEFAULT_REFRESH_TTL_DAYS)
    }

    /// Compute SHA-256 hash of the serialized config.
    pub fn hash(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Serialize config to JSON with paths converted to relative.
    /// Any paths pointing to `base_dir` are converted to relative paths.
    /// Note: This serializes the full config (for config files). For DB storage, use `to_app_snapshot()`.
    #[allow(dead_code)]
    pub fn to_json_relative(&self, base_dir: &Path) -> String {
        let mut config = self.clone();
        config.source_path = None; // Don't serialize the source path

        // Convert data_dir path to relative if it points to base_dir
        if let Some(ref data_dir) = config.data_dir {
            let data_path = Path::new(data_dir);
            if let Ok(canonical_data) = fs::canonicalize(data_path) {
                if let Ok(canonical_base) = fs::canonicalize(base_dir) {
                    if canonical_data == canonical_base {
                        config.data_dir = Some(".".to_string());
                    } else if let Ok(rel) = canonical_data.strip_prefix(&canonical_base) {
                        config.data_dir = Some(format!("./{}", rel.display()));
                    }
                }
            }
        }

        // Convert database path to relative
        if let Some(ref database) = config.database {
            let db_path = Path::new(database);
            if db_path.is_absolute() {
                if let Ok(canonical_db) = fs::canonicalize(db_path) {
                    if let Ok(canonical_base) = fs::canonicalize(base_dir) {
                        if let Ok(rel) = canonical_db.strip_prefix(&canonical_base) {
                            config.database = Some(format!("./{}", rel.display()));
                        }
                    }
                }
            }
        }

        serde_json::to_string_pretty(&config).unwrap_or_default()
    }

    /// Extract app-level settings for database storage.
    /// Excludes device-specific and bootstrap settings that shouldn't be synced.
    pub fn to_app_snapshot(&self) -> AppConfigSnapshot {
        AppConfigSnapshot {
            user_agent: self.user_agent.clone(),
            request_timeout: self.request_timeout,
            request_delay_ms: self.request_delay_ms,
            default_refresh_ttl_days: self.default_refresh_ttl_days,
            scrapers: self.scrapers.clone(),
            llm: self.llm.clone(),
            analysis: self.analysis.clone(),
            via: self.via.clone(),
            via_mode: self.via_mode,
        }
    }

    /// Apply app-level settings from a snapshot (loaded from DB).
    pub fn apply_app_snapshot(&mut self, snapshot: AppConfigSnapshot) {
        self.user_agent = snapshot.user_agent;
        self.request_timeout = snapshot.request_timeout;
        self.request_delay_ms = snapshot.request_delay_ms;
        self.default_refresh_ttl_days = snapshot.default_refresh_ttl_days;
        self.scrapers = snapshot.scrapers;
        self.llm = snapshot.llm;
        self.analysis = snapshot.analysis;
        self.via = snapshot.via;
        self.via_mode = snapshot.via_mode;
    }

    /// Load configuration from database history.
    /// Loads app-level settings only and merges with default config.
    /// Device-specific and bootstrap settings are not stored in DB.
    ///
    /// Uses prefer_db's FoiaConfigLoader for serde-based deserialization.
    pub async fn load_from_db(db_path: &Path) -> Option<Self> {
        let loader = FoiaConfigLoader::new(db_path);
        let snapshot = loader.load_snapshot().await?;

        // Start with default config (gets device settings from env)
        let mut config = Config::default();
        // Apply app settings from DB
        config.apply_app_snapshot(snapshot);
        Some(config)
    }
}

/// Options for loading settings.
#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// Explicit config file path (overrides auto-discovery).
    pub config_path: Option<PathBuf>,
    /// Use CWD for relative paths instead of config file directory.
    pub use_cwd: bool,
    /// Data directory or database file (--data flag).
    /// Can be a directory containing foiacquire.db or a .db file directly.
    pub data: Option<PathBuf>,
}

/// Resolved data path information for SQLite databases.
/// Only used when DATABASE_URL is NOT set to postgres.
#[derive(Debug, Clone)]
pub struct ResolvedData {
    /// The database filename.
    pub database_filename: String,
    /// Full path to the database.
    pub database_path: PathBuf,
}

impl ResolvedData {
    /// Resolve a data path to database filename and path.
    /// - If path is a .db file, extract filename and use as path
    /// - If path is a directory, look for foiacquire.db inside
    pub fn from_path(path: &Path) -> Self {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        };

        // Check if it's a file (by extension or existence)
        let is_db_file = path
            .extension()
            .is_some_and(|ext| ext == "db" || ext == "sqlite" || ext == "sqlite3")
            || (path.exists() && path.is_file());

        if is_db_file {
            let database_filename = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(DEFAULT_DATABASE_FILENAME)
                .to_string();
            Self {
                database_filename,
                database_path: path,
            }
        } else {
            // It's a directory
            let database_filename = DEFAULT_DATABASE_FILENAME.to_string();
            let database_path = path.join(&database_filename);
            Self {
                database_filename,
                database_path,
            }
        }
    }
}

/// Look for a config file next to the database.
/// Checks for foiacquire.{ext} and config.{ext} for all formats prefer supports.
fn find_config_next_to_db(data_dir: &Path) -> Option<PathBuf> {
    // All extensions supported by prefer
    let extensions = ["json", "json5", "yaml", "yml", "toml", "ini", "xml"];
    let basenames = ["foiacquire", "config"];

    for basename in basenames {
        for ext in extensions {
            let path = data_dir.join(format!("{}.{}", basename, ext));
            if path.exists() {
                return Some(path);
            }
        }
    }
    None
}

/// Database URL from environment, if set and valid.
struct DatabaseUrlEnv {
    url: Option<String>,
    is_postgres: bool,
}

impl DatabaseUrlEnv {
    /// Check DATABASE_URL environment variable.
    /// Panics if URL is postgres but feature not enabled.
    fn from_env() -> Self {
        let url = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty());
        let is_postgres = url.as_ref().is_some_and(|u| is_postgres_url(u));

        if let Some(ref u) = url {
            if let Err(e) = validate_database_url(u) {
                panic!(
                    "{}\n\nEither:\n  \
                     - Use a build with the 'postgres' feature enabled\n  \
                     - Use a sqlite:// URL instead\n  \
                     - Remove DATABASE_URL to use the default SQLite database",
                    e
                );
            }
        }

        Self { url, is_postgres }
    }
}

/// Resolve data path to a directory.
/// If path points to a .db file, returns its parent directory.
fn resolve_data_path_to_dir(path: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };

    if path
        .extension()
        .is_some_and(|ext| ext == "db" || ext == "sqlite" || ext == "sqlite3")
    {
        path.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        path
    }
}

/// Load config from the appropriate source based on options.
/// Merges file config with DB app settings for cross-device sync.
async fn load_config_from_sources(
    options: &LoadOptions,
    data_dir_override: Option<&PathBuf>,
    resolved_data: Option<&ResolvedData>,
) -> Config {
    // Step 1: Load file-based config
    let mut config = load_file_config(options, data_dir_override).await;

    // Step 2: Merge with DB app settings (synced across devices)
    // DB provides baseline, file overrides take priority
    if let Some(resolved) = resolved_data {
        if let Some(db_config) = Config::load_from_db(&resolved.database_path).await {
            tracing::debug!(
                "Merging DB app settings from: {}",
                resolved.database_path.display()
            );
            // Apply DB app settings as baseline, then re-apply file overrides
            let file_snapshot = config.to_app_snapshot();
            config.apply_app_snapshot(db_config.to_app_snapshot());
            // Re-apply file settings on top (file takes priority)
            merge_app_snapshots(&mut config, &file_snapshot);
        }
    }

    config
}

/// Load config from file sources only (no DB merge).
async fn load_file_config(options: &LoadOptions, data_dir_override: Option<&PathBuf>) -> Config {
    // Priority 1: Explicit --config flag
    if let Some(ref config_path) = options.config_path {
        return Config::load_from_path(config_path)
            .await
            .unwrap_or_else(|_| Config::default_with_env());
    }

    // Priority 2: Config next to data dir
    if let Some(data_dir) = data_dir_override {
        if let Some(config_path) = find_config_next_to_db(data_dir) {
            tracing::debug!("Found config next to data dir: {}", config_path.display());
            return Config::load_from_path(&config_path)
                .await
                .unwrap_or_else(|_| Config::default_with_env());
        }
    }

    // Priority 3: Auto-discover via prefer
    Config::load().await
}

/// Merge non-default values from snapshot into config.
/// Only applies values that differ from defaults (preserves explicit settings).
fn merge_app_snapshots(config: &mut Config, overlay: &AppConfigSnapshot) {
    let defaults = AppConfigSnapshot::default();

    // Merge each field if it differs from default
    if overlay.user_agent != defaults.user_agent {
        config.user_agent = overlay.user_agent.clone();
    }
    if overlay.request_timeout != defaults.request_timeout {
        config.request_timeout = overlay.request_timeout;
    }
    if overlay.request_delay_ms != defaults.request_delay_ms {
        config.request_delay_ms = overlay.request_delay_ms;
    }
    if overlay.default_refresh_ttl_days != defaults.default_refresh_ttl_days {
        config.default_refresh_ttl_days = overlay.default_refresh_ttl_days;
    }
    if overlay.scrapers != defaults.scrapers {
        // Merge scrapers - overlay entries replace base entries
        for (key, value) in &overlay.scrapers {
            config.scrapers.insert(key.clone(), value.clone());
        }
    }
    if !overlay.llm.is_default() {
        // Apply LLM app settings (device settings come from env)
        config.llm.app = overlay.llm.app.clone();
    }
    if !overlay.analysis.is_default() {
        config.analysis = overlay.analysis.clone();
    }
    if overlay.via != defaults.via {
        for (key, value) in &overlay.via {
            config.via.insert(key.clone(), value.clone());
        }
    }
    if overlay.via_mode != defaults.via_mode {
        config.via_mode = overlay.via_mode;
    }
}

/// Load settings with explicit options.
/// Returns (Settings, Config) tuple.
pub async fn load_settings_with_options(options: LoadOptions) -> (Settings, Config) {
    let db_env = DatabaseUrlEnv::from_env();

    let data_dir_override = options.data.as_ref().map(|d| resolve_data_path_to_dir(d));

    // Only resolve SQLite database paths when NOT using postgres
    let resolved_data = if !db_env.is_postgres {
        options.data.as_ref().map(|d| ResolvedData::from_path(d))
    } else {
        None
    };

    let config =
        load_config_from_sources(&options, data_dir_override.as_ref(), resolved_data.as_ref())
            .await;

    let mut settings = Settings::default();

    // Determine base directory for resolving relative paths
    let base_dir = if options.use_cwd {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    } else {
        config
            .base_dir()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    };

    config.apply_to_settings(&mut settings, &base_dir);

    // --data override takes precedence for data_dir and documents_dir
    if let Some(data_dir) = data_dir_override {
        settings.data_dir = data_dir;
        settings.documents_dir = settings.data_dir.join("documents");
    }

    // Apply SQLite-specific settings if resolved (not using postgres)
    if let Some(resolved) = resolved_data {
        settings.database_filename = resolved.database_filename;
    }

    // DATABASE_URL environment variable takes highest precedence
    if let Some(database_url) = db_env.url {
        tracing::debug!("Using DATABASE_URL from environment: {}", database_url);
        settings.database_url = Some(database_url);
    }

    // RATE_LIMIT_BACKEND environment variable takes precedence over config
    if let Some(backend) = std::env::var("RATE_LIMIT_BACKEND")
        .ok()
        .filter(|s| !s.is_empty())
    {
        tracing::debug!("Using RATE_LIMIT_BACKEND from environment: {}", backend);
        settings.rate_limit_backend = Some(backend);
    }

    // BROKER_URL environment variable takes precedence over config
    if let Some(broker) = std::env::var("BROKER_URL").ok().filter(|s| !s.is_empty()) {
        tracing::debug!("Using BROKER_URL from environment: {}", broker);
        settings.broker_url = Some(broker);
    }

    // FOIACQUIRE_NO_TLS disables TLS for PostgreSQL connections
    if std::env::var("FOIACQUIRE_NO_TLS")
        .unwrap_or_default()
        .eq_ignore_ascii_case("1")
        || std::env::var("FOIACQUIRE_NO_TLS")
            .unwrap_or_default()
            .eq_ignore_ascii_case("true")
    {
        settings.no_tls = true;
    }

    (settings, config)
}
