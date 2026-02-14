//! Configuration loading and merging logic.

use std::path::{Path, PathBuf};

use crate::repository::util::{is_postgres_url, validate_database_url};

use super::{Config, ResolvedData, Settings, SourcesConfig};

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
            let file_snapshot = config.to_sources_config();
            config.apply_sources_config(db_config.to_sources_config());
            // Re-apply file settings on top (file takes priority)
            merge_sources_configs(&mut config, &file_snapshot);
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

/// Merge non-default values from a sources config overlay into config.
/// Only applies values that differ from defaults (preserves explicit settings).
fn merge_sources_configs(config: &mut Config, overlay: &SourcesConfig) {
    let defaults = SourcesConfig::default();

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
        for (key, value) in &overlay.scrapers {
            config.scrapers.insert(key.clone(), value.clone());
        }
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
        tracing::debug!(
            "Using DATABASE_URL from environment: {}",
            crate::repository::util::redact_url_password(&database_url)
        );
        settings.database_url = Some(database_url);
    }

    // RATE_LIMIT_BACKEND environment variable takes precedence over config
    if let Some(backend) = std::env::var("RATE_LIMIT_BACKEND")
        .ok()
        .filter(|s| !s.is_empty())
    {
        tracing::debug!(
            "Using RATE_LIMIT_BACKEND from environment: {}",
            crate::repository::util::redact_url_password(&backend)
        );
        settings.rate_limit_backend = Some(backend);
    }

    // BROKER_URL environment variable takes precedence over config
    if let Some(broker) = std::env::var("BROKER_URL").ok().filter(|s| !s.is_empty()) {
        tracing::debug!(
            "Using BROKER_URL from environment: {}",
            crate::repository::util::redact_url_password(&broker)
        );
        settings.broker_url = Some(broker);
    }

    // FOIACQUIRE_NO_TLS disables TLS for PostgreSQL connections
    let no_tls_env = std::env::var("FOIACQUIRE_NO_TLS").unwrap_or_default();
    if no_tls_env.eq_ignore_ascii_case("1") || no_tls_env.eq_ignore_ascii_case("true") {
        settings.no_tls = true;
    }

    (settings, config)
}
