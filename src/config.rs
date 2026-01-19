//! Configuration management for FOIAcquire using the prefer crate.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::llm::LlmConfig;
use crate::privacy::PrivacyConfig;
use crate::repository::diesel_context::DieselDbContext;
use crate::repository::util::{is_postgres_url, validate_database_url};
use crate::scrapers::{ScraperConfig, ViaMode};

/// Default refresh TTL in days (14 days).
pub const DEFAULT_REFRESH_TTL_DAYS: u64 = 14;

/// Analysis configuration for text extraction methods.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnalysisConfig {
    /// Named analysis methods (custom commands).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub methods: HashMap<String, AnalysisMethodConfig>,
    /// Default methods to run if --method flag not specified.
    /// Defaults to ["ocr"] if empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_methods: Vec<String>,
}

impl AnalysisConfig {
    /// Check if this is the default (empty) config.
    pub fn is_default(&self) -> bool {
        self.methods.is_empty() && self.default_methods.is_empty()
    }
}

/// Configuration for a single analysis method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisMethodConfig {
    /// Command to execute (required for custom commands, optional for built-ins).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Arguments (can include {file} and {page} placeholders).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Mimetypes this method applies to (supports wildcards like "audio/*").
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mimetypes: Vec<String>,
    /// Analysis granularity: "page" or "document" (default: "document").
    #[serde(default = "default_granularity")]
    pub granularity: String,
    /// Whether command outputs to stdout (true) or a file (false).
    #[serde(default = "default_true")]
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
        DieselDbContext::from_url(&self.database_url())
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Target directory for data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
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
    /// - None or "memory": In-memory (single process only)
    /// - "sqlite": Use local SQLite database (multi-process safe)
    /// - "redis://host:port": Use Redis (distributed)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_backend: Option<String>,
    /// Worker queue broker URL.
    /// - None or "database": Use local SQLite database
    /// - "amqp://host:port": Use RabbitMQ
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_url: Option<String>,
    /// Default refresh TTL in days for re-checking fetched URLs.
    /// Individual scrapers can override this with their own refresh_ttl_days.
    /// Defaults to 14 days if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_refresh_ttl_days: Option<u64>,
    /// Scraper configurations.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub scrapers: HashMap<String, ScraperConfig>,
    /// LLM configuration for document summarization.
    #[serde(default, skip_serializing_if = "LlmConfig::is_default")]
    pub llm: LlmConfig,
    /// Analysis configuration for text extraction methods.
    #[serde(default, skip_serializing_if = "AnalysisConfig::is_default")]
    pub analysis: AnalysisConfig,

    /// Privacy configuration for Tor and proxy routing.
    #[serde(default, skip_serializing_if = "PrivacyConfig::is_default")]
    pub privacy: PrivacyConfig,

    /// URL rewriting for caching proxies (CDN bypass).
    /// Maps original base URLs to proxy URLs.
    /// Example: "https://www.cia.gov" = "https://cia.monokro.me"
    /// Requests to cia.gov will be fetched via the CloudFront proxy instead.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub via: HashMap<String, String>,

    /// Via proxy mode - controls when via mappings are used for requests.
    /// - "strict" (default): Never use via for requests, only for URL detection
    /// - "fallback": Use via as fallback when rate limited (429/503)
    /// - "priority": Use via first, fall back to original URL on failure
    #[serde(default, skip_serializing_if = "is_via_mode_default")]
    pub via_mode: ViaMode,

    /// Path to the config file this was loaded from (not serialized).
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
}

fn is_via_mode_default(mode: &ViaMode) -> bool {
    *mode == ViaMode::default()
}

impl Config {
    /// Load configuration using prefer crate.
    /// Automatically discovers foiacquire config files in standard locations.
    pub async fn load() -> Self {
        match prefer::load("foiacquire").await {
            Ok(pref_config) => {
                // Extract values from prefer config using dot notation
                let target: Option<String> = pref_config.get("target").await.ok();
                let database: Option<String> = pref_config.get("database").await.ok();
                let user_agent: Option<String> = pref_config.get("user_agent").await.ok();
                let request_timeout: Option<u64> = pref_config.get("request_timeout").await.ok();
                let request_delay_ms: Option<u64> = pref_config.get("request_delay_ms").await.ok();
                let rate_limit_backend: Option<String> =
                    pref_config.get("rate_limit_backend").await.ok();
                let broker_url: Option<String> = pref_config.get("broker_url").await.ok();
                let default_refresh_ttl_days: Option<u64> =
                    pref_config.get("default_refresh_ttl_days").await.ok();
                let scrapers: HashMap<String, ScraperConfig> =
                    pref_config.get("scrapers").await.unwrap_or_default();
                let llm: LlmConfig = pref_config
                    .get::<LlmConfig>("llm")
                    .await
                    .unwrap_or_default()
                    .with_env_overrides();
                let analysis: AnalysisConfig =
                    pref_config.get("analysis").await.unwrap_or_default();
                let privacy: PrivacyConfig = pref_config
                    .get::<PrivacyConfig>("privacy")
                    .await
                    .unwrap_or_default()
                    .with_env_overrides();
                let via: HashMap<String, String> =
                    pref_config.get("via").await.unwrap_or_default();
                let via_mode: ViaMode = pref_config.get("via_mode").await.unwrap_or_default();

                // Get the source path from prefer
                let source_path = pref_config.source_path().cloned();

                Config {
                    target,
                    database,
                    user_agent,
                    request_timeout,
                    request_delay_ms,
                    rate_limit_backend,
                    broker_url,
                    default_refresh_ttl_days,
                    scrapers,
                    llm,
                    analysis,
                    privacy,
                    via,
                    via_mode,
                    source_path,
                }
            }
            Err(_) => {
                // No config file found, use defaults
                Self::default()
            }
        }
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
        config.llm = config.llm.with_env_overrides();
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
        if let Some(ref target) = self.target {
            settings.data_dir = self.resolve_path(target, base_dir);
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
    /// Any paths pointing to `target_dir` are converted to relative paths.
    pub fn to_json_relative(&self, target_dir: &Path) -> String {
        let mut config = self.clone();
        config.source_path = None; // Don't serialize the source path

        // Convert target path to relative if it points to target_dir
        if let Some(ref target) = config.target {
            let target_path = Path::new(target);
            if let Ok(canonical_target) = fs::canonicalize(target_path) {
                if let Ok(canonical_dir) = fs::canonicalize(target_dir) {
                    if canonical_target == canonical_dir {
                        config.target = Some(".".to_string());
                    } else if let Ok(rel) = canonical_target.strip_prefix(&canonical_dir) {
                        config.target = Some(format!("./{}", rel.display()));
                    }
                }
            }
        }

        // Convert database path to relative
        if let Some(ref database) = config.database {
            let db_path = Path::new(database);
            if db_path.is_absolute() {
                if let Ok(canonical_db) = fs::canonicalize(db_path) {
                    if let Ok(canonical_dir) = fs::canonicalize(target_dir) {
                        if let Ok(rel) = canonical_db.strip_prefix(&canonical_dir) {
                            config.database = Some(format!("./{}", rel.display()));
                        }
                    }
                }
            }
        }

        serde_json::to_string_pretty(&config).unwrap_or_default()
    }

    /// Load configuration from database history.
    pub async fn load_from_db(db_path: &Path) -> Option<Self> {
        let ctx = DieselDbContext::from_sqlite_path(db_path).ok()?;
        let entry = ctx.config_history().get_latest().await.ok()??;

        let mut config: Config = match entry.format.to_lowercase().as_str() {
            "json" => serde_json::from_str(&entry.data).ok()?,
            "toml" => toml::from_str(&entry.data).ok()?,
            _ => serde_json::from_str(&entry.data).ok()?,
        };

        // Apply environment variable overrides
        config.llm = config.llm.with_env_overrides();
        config.privacy = config.privacy.with_env_overrides();
        Some(config)
    }

    /// Save configuration to database history if it has changed.
    /// Returns true if saved, false if unchanged, or logs warning on error.
    pub async fn save_to_db_if_changed(&self, settings: &Settings) {
        let hash = self.hash();
        let data = self.to_json_relative(&settings.data_dir);
        let format = "json";

        let ctx = match settings.create_db_context() {
            Ok(ctx) => ctx,
            Err(e) => {
                tracing::warn!("Could not save config to history (db context error): {}", e);
                return;
            }
        };
        let repo = ctx.config_history();

        match repo.insert_if_new(&data, format, &hash).await {
            Ok(true) => {
                tracing::debug!("Saved new config to history");
            }
            Ok(false) => {
                tracing::debug!("Config unchanged, not saving to history");
            }
            Err(e) => {
                // Check for lock errors and warn
                let msg = e.to_string();
                if msg.contains("locked") || msg.contains("SQLITE_BUSY") {
                    tracing::warn!("Could not save config to history (database locked): {}", e);
                } else {
                    tracing::warn!("Could not save config to history: {}", e);
                }
            }
        }
    }
}

/// Options for loading settings.
#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// Explicit config file path (overrides auto-discovery).
    pub config_path: Option<PathBuf>,
    /// Use CWD for relative paths instead of config file directory.
    pub use_cwd: bool,
    /// Target directory or database file (--target flag).
    /// Can be a directory containing foiacquire.db or a .db file directly.
    pub target: Option<PathBuf>,
}

/// Resolved target information for SQLite databases.
/// Only used when DATABASE_URL is NOT set to postgres.
#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    /// The database filename.
    pub database_filename: String,
    /// Full path to the database.
    pub database_path: PathBuf,
}

impl ResolvedTarget {
    /// Resolve a target path to database filename and path.
    /// - If target is a .db file, extract filename and use as path
    /// - If target is a directory, look for foiacquire.db inside
    pub fn from_path(target: &Path) -> Self {
        let target = if target.is_absolute() {
            target.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(target)
        };

        // Check if it's a file (by extension or existence)
        let is_db_file = target
            .extension()
            .is_some_and(|ext| ext == "db" || ext == "sqlite" || ext == "sqlite3")
            || (target.exists() && target.is_file());

        if is_db_file {
            let database_filename = target
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(DEFAULT_DATABASE_FILENAME)
                .to_string();
            Self {
                database_filename,
                database_path: target,
            }
        } else {
            // It's a directory
            let database_filename = DEFAULT_DATABASE_FILENAME.to_string();
            let database_path = target.join(&database_filename);
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

/// Resolve target path to a data directory.
/// If path points to a .db file, returns its parent directory.
fn resolve_target_to_data_dir(target: &Path) -> PathBuf {
    let target = if target.is_absolute() {
        target.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(target)
    };

    if target
        .extension()
        .is_some_and(|ext| ext == "db" || ext == "sqlite" || ext == "sqlite3")
    {
        target.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        target
    }
}

/// Load config from the appropriate source based on options.
async fn load_config_from_sources(
    options: &LoadOptions,
    target_data_dir: Option<&PathBuf>,
    resolved_target: Option<&ResolvedTarget>,
) -> Config {
    // Priority 1: Explicit --config flag
    if let Some(ref config_path) = options.config_path {
        return Config::load_from_path(config_path)
            .await
            .unwrap_or_default();
    }

    // Priority 2-3: Config next to target, or from database history
    if let Some(data_dir) = target_data_dir {
        if let Some(config_path) = find_config_next_to_db(data_dir) {
            tracing::debug!("Found config next to target: {}", config_path.display());
            return Config::load_from_path(&config_path)
                .await
                .unwrap_or_default();
        }

        if let Some(resolved) = resolved_target {
            tracing::debug!(
                "No config file found, trying database history: {}",
                resolved.database_path.display()
            );
            if let Some(config) = Config::load_from_db(&resolved.database_path).await {
                return config;
            }
            tracing::debug!("No config in database history, using defaults");
        }
    }

    // Priority 4: Auto-discover via prefer
    Config::load().await
}

/// Load settings with explicit options.
/// Returns (Settings, Config) tuple.
pub async fn load_settings_with_options(options: LoadOptions) -> (Settings, Config) {
    let db_env = DatabaseUrlEnv::from_env();

    let target_data_dir = options
        .target
        .as_ref()
        .map(|t| resolve_target_to_data_dir(t));

    // Only resolve SQLite database paths when NOT using postgres
    let resolved_target = if !db_env.is_postgres {
        options
            .target
            .as_ref()
            .map(|t| ResolvedTarget::from_path(t))
    } else {
        None
    };

    let config =
        load_config_from_sources(&options, target_data_dir.as_ref(), resolved_target.as_ref())
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

    // --target override takes precedence for data_dir and documents_dir
    if let Some(data_dir) = target_data_dir {
        settings.data_dir = data_dir;
        settings.documents_dir = settings.data_dir.join("documents");
    }

    // Apply SQLite-specific settings if resolved (not using postgres)
    if let Some(resolved) = resolved_target {
        settings.database_filename = resolved.database_filename;
    }

    // DATABASE_URL environment variable takes highest precedence
    if let Some(database_url) = db_env.url {
        tracing::debug!("Using DATABASE_URL from environment: {}", database_url);
        settings.database_url = Some(database_url);
    }

    // Save config to database history (errors logged gracefully)
    config.save_to_db_if_changed(&settings).await;

    (settings, config)
}
