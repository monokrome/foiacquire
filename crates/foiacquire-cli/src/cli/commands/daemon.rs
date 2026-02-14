//! Shared daemon loop infrastructure for config watching and sleep/reload.

use console::style;
use tokio::sync::mpsc;

use foiacquire::repository::{DieselConfigHistoryRepository, DieselScraperConfigRepository};

/// Reload mode for daemon operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ReloadMode {
    /// Reload config at the start of each daemon cycle
    #[default]
    NextRun,
    /// Exit process when config file changes (for process manager restart)
    StopProcess,
    /// Watch config file and reload immediately when it changes
    Inplace,
}

/// Action returned by [`ConfigWatcher::sleep_or_reload`].
pub enum DaemonAction {
    /// Continue to the next daemon cycle (timer expired, no config change).
    Continue,
    /// Exit the process (config changed in stop-process mode).
    Exit,
    /// Config changed in-place; the caller should reload.
    Reload,
}

/// Manages file- and DB-based config watching for daemon loops.
pub struct ConfigWatcher {
    watcher: Option<mpsc::Receiver<prefer::Config>>,
    config_history: DieselConfigHistoryRepository,
    scraper_configs: DieselScraperConfigRepository,
    current_hash: String,
    reload: ReloadMode,
    daemon: bool,
}

impl ConfigWatcher {
    /// Create a new config watcher.
    ///
    /// If `daemon` is true and the reload mode requires watching, a file watcher
    /// is set up via `prefer::watch`. Falls back to DB polling when no config
    /// file is available.
    pub async fn new(
        daemon: bool,
        reload: ReloadMode,
        config_history: DieselConfigHistoryRepository,
        scraper_configs: DieselScraperConfigRepository,
        initial_hash: String,
    ) -> Self {
        let watcher = if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            prefer::watch("foiacquire").await.ok()
        } else {
            None
        };

        // Use scraper_configs MAX(updated_at) as the initial hash so change
        // detection compares like with like. Fall back to the config file
        // hash when the table is empty.
        let current_hash = if let Ok(Some(ts)) = scraper_configs.max_updated_at().await {
            ts
        } else {
            initial_hash
        };

        Self {
            watcher,
            config_history,
            scraper_configs,
            current_hash,
            reload,
            daemon,
        }
    }

    /// Update the stored config hash (used when the caller reloads config at
    /// the top of its loop).
    pub fn update_hash(&mut self, hash: String) {
        self.current_hash = hash;
    }

    /// Sleep for `interval` seconds, watching for config changes.
    ///
    /// `inplace_label` is the verb shown in log output when an in-place reload
    /// triggers (e.g. "reloading" or "continuing").
    pub async fn sleep_or_reload(&mut self, interval: u64, inplace_label: &str) -> DaemonAction {
        println!(
            "{} Sleeping for {}s before next check...",
            style("→").dim(),
            interval
        );

        if let Some(ref mut watcher) = self.watcher {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(interval)) => {}
                result = watcher.recv() => {
                    if result.is_some() {
                        match self.reload {
                            ReloadMode::StopProcess => {
                                println!(
                                    "{} Config file changed, exiting for restart...",
                                    style("↻").cyan()
                                );
                                return DaemonAction::Exit;
                            }
                            ReloadMode::Inplace => {
                                println!(
                                    "{} Config file changed, {}...",
                                    style("↻").cyan(),
                                    inplace_label
                                );
                                return DaemonAction::Reload;
                            }
                            ReloadMode::NextRun => {}
                        }
                    }
                }
            }
        } else if self.daemon
            && matches!(self.reload, ReloadMode::StopProcess | ReloadMode::Inplace)
        {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

            // Detect changes via scraper_configs MAX(updated_at)
            let changed = if let Ok(Some(max_updated)) = self.scraper_configs.max_updated_at().await
            {
                max_updated != self.current_hash
            } else {
                // Fall back to configuration_history hash
                self.config_history
                    .get_latest_hash()
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|h| h != self.current_hash)
            };

            if changed {
                match self.reload {
                    ReloadMode::StopProcess => {
                        println!(
                            "{} Config changed in database, exiting for restart...",
                            style("↻").cyan()
                        );
                        return DaemonAction::Exit;
                    }
                    ReloadMode::Inplace => {
                        println!(
                            "{} Config changed in database, {}...",
                            style("↻").cyan(),
                            inplace_label
                        );
                        if let Ok(Some(max)) = self.scraper_configs.max_updated_at().await {
                            self.current_hash = max;
                        }
                        return DaemonAction::Reload;
                    }
                    ReloadMode::NextRun => {}
                }
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        }

        DaemonAction::Continue
    }
}
