//! Application settings.

use std::fs;
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;

use crate::repository::diesel_context::DieselDbContext;
use crate::repository::util::is_postgres_url;
use crate::repository::Repositories;

use super::DEFAULT_DATABASE_FILENAME;

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
    /// Supports sqlite:// and postgres:// URLs.
    /// Set via DATABASE_URL env var or the `database` field in config files.
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
            user_agent: "foia/0.1 (academic research)".to_string(),
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

    /// Create bundled repositories for all database operations.
    ///
    /// Preferred over `create_db_context()` in CLI commands — provides direct
    /// field access to all repository types without intermediate context.
    pub fn repositories(&self) -> Result<Repositories, diesel::result::Error> {
        let ctx = self.create_db_context()?;
        Ok(Repositories::new(ctx))
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
