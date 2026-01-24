//! Database-backed configuration loader for foiacquire.
//!
//! This module uses prefer_db to load config from the database,
//! using prefer's native FromValue for type conversion.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use prefer_db::{ConfigEntry, ConfigLoader, DbSource};

use crate::config::{AppConfigSnapshot, Config};
use crate::repository::diesel_context::DieselDbContext;

/// Foiacquire database configuration loader.
///
/// Loads configuration from the config_history table.
pub struct FoiaConfigLoader {
    db_path: PathBuf,
}

impl FoiaConfigLoader {
    /// Create a new loader for the given database path.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Self {
        Self {
            db_path: db_path.as_ref().to_path_buf(),
        }
    }

    /// Load app config snapshot from the database using native prefer.
    ///
    /// Uses prefer's ConfigBuilder with DbSource for native FromValue conversion.
    /// Falls back to full Config for backwards compatibility with old DB entries.
    pub async fn load_snapshot(&self) -> Option<AppConfigSnapshot> {
        use prefer::FromValue;

        let source = DbSource::new(SelfLoader {
            db_path: self.db_path.clone(),
        });

        let config = prefer::Config::builder()
            .add_source(source)
            .build()
            .await
            .ok()?;

        // Try AppConfigSnapshot first (new format)
        if let Ok(snapshot) = AppConfigSnapshot::from_value(config.data()) {
            return Some(snapshot);
        }

        // Fall back to full Config (old format) and extract app portion
        if let Ok(full_config) = Config::from_value(config.data()) {
            return Some(full_config.to_app_snapshot());
        }

        None
    }
}

/// Internal loader that implements ConfigLoader.
/// Separate from FoiaConfigLoader to avoid self-referential async issues.
struct SelfLoader {
    db_path: PathBuf,
}

#[async_trait]
impl ConfigLoader for SelfLoader {
    async fn load_config(&self) -> Option<ConfigEntry> {
        let ctx = DieselDbContext::from_sqlite_path(&self.db_path).ok()?;
        let entry = ctx.config_history().get_latest().await.ok()??;

        Some(ConfigEntry {
            format: entry.format,
            data: entry.data,
        })
    }

    fn name(&self) -> &str {
        "foiacquire_db"
    }
}
