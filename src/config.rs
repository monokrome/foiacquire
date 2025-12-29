//! Configuration management for FOIAcquire using the prefer crate.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::llm::LlmConfig;
use crate::repository::diesel_context::DieselDbContext;
use crate::repository::util::{is_postgres_url, validate_database_url};
use crate::scrapers::ScraperConfig;

/// Default refresh TTL in days (14 days).
pub const DEFAULT_REFRESH_TTL_DAYS: u64 = 14;

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
        fs::create_dir_all(&self.data_dir)?;
        fs::create_dir_all(&self.documents_dir)?;
        Ok(())
    }

    /// Create a database context using the configured database URL or path.
    ///
    /// This is the preferred way to get a DieselDbContext from settings.
    /// Panics if the database URL is invalid (e.g., invalid PostgreSQL URL).
    pub fn create_db_context(&self) -> DieselDbContext {
        DieselDbContext::from_url(&self.database_url(), &self.documents_dir)
            .expect("Failed to create database context")
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

    /// Path to the config file this was loaded from (not serialized).
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
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
    pub async fn load_from_path(path: &Path) -> Result<Self, String> {
        let contents = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| format!("Failed to read config file: {}", e))?;

        let mut config: Config = serde_json::from_str(&contents)
            .map_err(|e| format!("Failed to parse config file: {}", e))?;

        config.source_path = Some(path.to_path_buf());
        config.llm = config.llm.with_env_overrides();
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
        // Use a dummy documents dir since we only need config_history
        let docs_dir = db_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(DOCUMENTS_SUBDIR);
        let ctx = DieselDbContext::from_sqlite_path(db_path, &docs_dir).ok()?;
        let entry = ctx.config_history().get_latest().await.ok()??;

        match entry.format.to_lowercase().as_str() {
            "json" => serde_json::from_str(&entry.data).ok(),
            "toml" => toml::from_str(&entry.data).ok(),
            _ => serde_json::from_str(&entry.data).ok(),
        }
    }

    /// Save configuration to database history if it has changed.
    /// Returns true if saved, false if unchanged, or logs warning on error.
    pub async fn save_to_db_if_changed(&self, settings: &Settings) {
        let hash = self.hash();
        let data = self.to_json_relative(&settings.data_dir);
        let format = "json";

        let ctx = settings.create_db_context();
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
/// Checks for foiacquire.json, foiacquire.toml, config.json, config.toml.
fn find_config_next_to_db(data_dir: &Path) -> Option<PathBuf> {
    let candidates = [
        "foiacquire.json",
        "foiacquire.toml",
        "config.json",
        "config.toml",
    ];

    for name in candidates {
        let path = data_dir.join(name);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

/// Load settings with explicit options.
/// Returns (Settings, Config) tuple.
pub async fn load_settings_with_options(options: LoadOptions) -> (Settings, Config) {
    // Check DATABASE_URL first - this affects whether we should touch SQLite files
    let database_url_override = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty());
    let using_postgres = database_url_override
        .as_ref()
        .is_some_and(|url| is_postgres_url(url));

    // Fail immediately if PostgreSQL URL is provided but postgres feature is not compiled in
    if let Some(ref url) = database_url_override {
        if let Err(e) = validate_database_url(url) {
            panic!(
                "{}\n\nEither:\n  \
                 - Use a build with the 'postgres' feature enabled\n  \
                 - Use a sqlite:// URL instead\n  \
                 - Remove DATABASE_URL to use the default SQLite database",
                e
            );
        }
    }

    // If target is specified, resolve the data directory
    // When using postgres, we only need the data_dir for documents - skip SQLite path resolution
    let target_data_dir = options.target.as_ref().map(|t| {
        let target = if t.is_absolute() {
            t.clone()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(t)
        };
        // If it looks like a .db file, use its parent directory
        if target
            .extension()
            .is_some_and(|ext| ext == "db" || ext == "sqlite" || ext == "sqlite3")
        {
            target.parent().unwrap_or(Path::new(".")).to_path_buf()
        } else {
            target
        }
    });

    // Only resolve SQLite database paths when NOT using postgres
    let resolved_target = if !using_postgres {
        options
            .target
            .as_ref()
            .map(|t| ResolvedTarget::from_path(t))
    } else {
        None
    };

    // Determine config loading order when --target is specified:
    // 1. Explicit --config flag
    // 2. Config file next to the target directory
    // 3. Config from database history (skip when using PostgreSQL)
    // 4. Auto-discover via prefer
    let config = if let Some(ref config_path) = options.config_path {
        // Explicit config path takes priority
        Config::load_from_path(config_path)
            .await
            .unwrap_or_default()
    } else if let Some(ref data_dir) = target_data_dir {
        // Check for config file next to target
        if let Some(config_path) = find_config_next_to_db(data_dir) {
            tracing::debug!("Found config next to target: {}", config_path.display());
            Config::load_from_path(&config_path)
                .await
                .unwrap_or_default()
        } else if let Some(ref resolved) = resolved_target {
            // Try SQLite database history - just attempt it and handle failure
            tracing::debug!(
                "No config file found, trying database history: {}",
                resolved.database_path.display()
            );
            Config::load_from_db(&resolved.database_path)
                .await
                .unwrap_or_else(|| {
                    tracing::debug!("No config in database history, using defaults");
                    Config::default()
                })
        } else {
            // Using PostgreSQL - skip SQLite database history, use auto-discovery
            Config::load().await
        }
    } else {
        // No target specified, use auto-discovery
        Config::load().await
    };

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
    // Also apply SQLite-specific settings if resolved (not using postgres)
    if let Some(resolved) = resolved_target {
        settings.database_filename = resolved.database_filename;
    }

    // DATABASE_URL environment variable takes highest precedence
    if let Some(database_url) = database_url_override {
        tracing::debug!("Using DATABASE_URL from environment: {}", database_url);
        settings.database_url = Some(database_url);
    }

    // Save config to database history (errors logged gracefully)
    config.save_to_db_if_changed(&settings).await;

    (settings, config)
}
