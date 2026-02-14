//! Shared daemon loop infrastructure for config watching and sleep/reload.

use console::style;
use tokio::sync::mpsc;

use foiacquire::repository::DieselConfigHistoryRepository;

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
        initial_hash: String,
    ) -> Self {
        let watcher = if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            prefer::watch("foiacquire").await.ok()
        } else {
            None
        };

        Self {
            watcher,
            config_history,
            current_hash: initial_hash,
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

            if let Ok(Some(latest_hash)) = self.config_history.get_latest_hash().await {
                if latest_hash != self.current_hash {
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
                            self.current_hash = latest_hash;
                            return DaemonAction::Reload;
                        }
                        ReloadMode::NextRun => {}
                    }
                }
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        }

        DaemonAction::Continue
    }
}
