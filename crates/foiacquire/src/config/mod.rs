//! Configuration management for FOIAcquire using the prefer crate.

mod analysis;
pub mod browser;
pub mod discovery;
mod loader;
pub mod scraper;
mod settings;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::llm::LlmConfig;
use crate::privacy::PrivacyConfig;
use crate::repository::util::validate_database_url;

pub use analysis::{AnalysisConfig, AnalysisMethodConfig, OcrConfig};
pub use browser::{BrowserEngineConfig, BrowserEngineType, SelectionStrategyType};
pub use loader::{load_settings_with_options, LoadOptions};
pub use scraper::{ScraperConfig, ViaMode};
pub use settings::Settings;

/// Default refresh TTL in days (14 days).
pub const DEFAULT_REFRESH_TTL_DAYS: u64 = 14;

/// Default database filename.
pub const DEFAULT_DATABASE_FILENAME: &str = "foiacquire.db";

/// Default documents subdirectory name.
const DOCUMENTS_SUBDIR: &str = "documents";

/// Configuration file structure.
#[derive(Debug, Clone, Default, Serialize, Deserialize, prefer::FromValue)]
pub struct Config {
    /// Data directory path.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "target")]
    pub data_dir: Option<String>,
    /// Database filename or URL.
    /// Accepts a plain filename (e.g. "foiacquire.db") which is joined with data_dir,
    /// or a full database URL (e.g. "sqlite:///path/to/db", "postgres://user:pass@host/db").
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

/// Legacy source interaction settings for backwards-compatible deserialization.
/// Used only for migration from configuration_history to scraper_configs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourcesConfig {
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub request_timeout: Option<u64>,
    #[serde(default)]
    pub request_delay_ms: Option<u64>,
    #[serde(default)]
    pub default_refresh_ttl_days: Option<u64>,
    #[serde(default)]
    pub scrapers: HashMap<String, ScraperConfig>,
    #[serde(default)]
    pub via: HashMap<String, String>,
    #[serde(default)]
    pub via_mode: ViaMode,
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
            if database.contains("://") {
                if let Err(e) = validate_database_url(database) {
                    tracing::error!("Invalid database URL in config: {}", e);
                } else {
                    settings.database_url = Some(database.clone());
                }
            } else {
                settings.database_filename = database.clone();
            }
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

        // Convert database path to relative (skip URL values)
        if let Some(ref database) = config.database {
            if !database.contains("://") {
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
        }

        serde_json::to_string_pretty(&config).unwrap_or_default()
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn default_settings() -> Settings {
        Settings {
            data_dir: PathBuf::from("/tmp/test"),
            documents_dir: PathBuf::from("/tmp/test/documents"),
            database_filename: DEFAULT_DATABASE_FILENAME.to_string(),
            database_url: None,
            user_agent: "test".to_string(),
            request_timeout: 30,
            request_delay_ms: 500,
            rate_limit_backend: None,
            broker_url: None,
            no_tls: false,
        }
    }

    #[test]
    fn apply_database_filename_sets_database_filename() {
        let config = Config {
            database: Some("custom.db".to_string()),
            ..Config::default()
        };
        let mut settings = default_settings();
        let base = PathBuf::from("/tmp");
        config.apply_to_settings(&mut settings, &base);

        assert_eq!(settings.database_filename, "custom.db");
        assert!(settings.database_url.is_none());
    }

    #[test]
    fn apply_sqlite_url_sets_database_url() {
        let config = Config {
            database: Some("sqlite:///tmp/test.db".to_string()),
            ..Config::default()
        };
        let mut settings = default_settings();
        let base = PathBuf::from("/tmp");
        config.apply_to_settings(&mut settings, &base);

        assert_eq!(
            settings.database_url,
            Some("sqlite:///tmp/test.db".to_string())
        );
        assert_eq!(settings.database_filename, DEFAULT_DATABASE_FILENAME);
    }

    #[test]
    fn apply_postgres_url_without_feature() {
        let config = Config {
            database: Some("postgres://user:pass@host/db".to_string()),
            ..Config::default()
        };
        let mut settings = default_settings();
        let base = PathBuf::from("/tmp");
        config.apply_to_settings(&mut settings, &base);

        // Without postgres feature, validation fails and URL is not set
        #[cfg(not(feature = "postgres"))]
        {
            assert!(settings.database_url.is_none());
            assert_eq!(settings.database_filename, DEFAULT_DATABASE_FILENAME);
        }

        // With postgres feature, URL is set
        #[cfg(feature = "postgres")]
        {
            assert_eq!(
                settings.database_url,
                Some("postgres://user:pass@host/db".to_string())
            );
        }
    }

    #[test]
    fn to_json_relative_preserves_url_values() {
        let config = Config {
            database: Some("sqlite:///absolute/path/to/db".to_string()),
            ..Config::default()
        };
        let base = PathBuf::from("/tmp");
        let json = config.to_json_relative(&base);

        assert!(json.contains("sqlite:///absolute/path/to/db"));
    }

    #[test]
    fn apply_no_database_leaves_defaults() {
        let config = Config::default();
        let mut settings = default_settings();
        let base = PathBuf::from("/tmp");
        config.apply_to_settings(&mut settings, &base);

        assert_eq!(settings.database_filename, DEFAULT_DATABASE_FILENAME);
        assert!(settings.database_url.is_none());
    }
}
