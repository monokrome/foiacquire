//! Database-backed configuration loader for foia.
//!
//! This module uses prefer_db to load config from the database,
//! using prefer's native FromValue for type conversion.

use std::path::{Path, PathBuf};

use ::prefer_db::{ConfigEntry, ConfigLoader, DbSource};
use async_trait::async_trait;

use crate::config::SourcesConfig;
use crate::repository::diesel_context::DieselDbContext;

/// Foia database configuration loader.
///
/// Loads configuration from the scraper_configs table,
/// assembling per-source entries into a SourcesConfig.
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

    /// Load source interaction settings from the database using native prefer.
    ///
    /// Reads per-source scraper configs from the scraper_configs table and
    /// assembles them into a SourcesConfig for the prefer pipeline.
    pub async fn load_snapshot(&self) -> Option<SourcesConfig> {
        use prefer::FromValue;

        let source = DbSource::new(SelfLoader {
            db_path: self.db_path.clone(),
        });

        let config = prefer::Config::builder()
            .add_source(source)
            .build()
            .await
            .ok()?;

        SourcesConfig::from_value(config.data()).ok()
    }
}

/// Internal loader that implements ConfigLoader.
/// Reads scraper_configs table and assembles into a SourcesConfig JSON blob.
struct SelfLoader {
    db_path: PathBuf,
}

#[async_trait]
impl ConfigLoader for SelfLoader {
    async fn load_config(&self) -> Option<ConfigEntry> {
        let ctx = DieselDbContext::from_sqlite_path(&self.db_path).ok()?;
        let configs = ctx.scraper_configs().get_all().await.ok()?;

        if configs.is_empty() {
            return None;
        }

        let sources = SourcesConfig {
            scrapers: configs.into_iter().collect(),
            ..SourcesConfig::default()
        };

        let data = serde_json::to_string(&sources).ok()?;

        Some(ConfigEntry {
            format: "json".to_string(),
            data,
        })
    }

    fn name(&self) -> &str {
        "foia_db"
    }
}
