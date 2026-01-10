//! C-Tor hidden service integration.
//!
//! This module provides integration with the C-Tor (reference Tor implementation)
//! for hosting onion services. C-Tor is the recommended choice for production
//! hidden services as it has the most battle-tested security properties.
//!
//! # How it works
//!
//! 1. We spawn a tor process with a configured hidden service directory
//! 2. Tor generates the onion address and keys
//! 3. Tor connects to the Tor network and advertises the service
//! 4. We read the hostname file to get the .onion address
//! 5. Incoming connections are proxied from Tor to our local server

use std::fs;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use super::HiddenServiceConfig;

/// Default Tor control port.
const DEFAULT_CONTROL_PORT: u16 = 9051;

/// Default Tor SOCKS port.
const DEFAULT_SOCKS_PORT: u16 = 9050;

/// C-Tor hidden service manager.
pub struct CTorHiddenService {
    /// The Tor process (if we spawned it).
    process: Option<Child>,
    /// Hidden service directory.
    hs_dir: PathBuf,
    /// The .onion address (once available).
    onion_address: Arc<Mutex<Option<String>>>,
    /// Local port the hidden service points to.
    local_port: u16,
    /// Tor SOCKS port (for outbound connections).
    socks_port: u16,
}

impl CTorHiddenService {
    /// Find the tor binary in PATH or at a specific location.
    pub fn find_tor_binary(config: &HiddenServiceConfig) -> Option<PathBuf> {
        // Check explicit path first
        if let Some(ref path) = config.tor_binary {
            if path.exists() {
                return Some(path.clone());
            }
        }

        // Search PATH
        let candidates = if cfg!(windows) {
            vec!["tor.exe", "Tor\\tor.exe"]
        } else {
            vec!["tor", "/usr/bin/tor", "/usr/local/bin/tor"]
        };

        for candidate in candidates {
            if let Ok(path) = which::which(candidate) {
                return Some(path);
            }
        }

        None
    }

    /// Check if C-Tor is available on this system.
    pub fn is_available(config: &HiddenServiceConfig) -> bool {
        Self::find_tor_binary(config).is_some()
    }

    /// Start a new hidden service.
    ///
    /// # Arguments
    /// * `config` - Hidden service configuration
    /// * `data_dir` - Base data directory (tor data goes in data_dir/tor)
    /// * `local_addr` - Local address the server is listening on
    pub async fn start(
        config: &HiddenServiceConfig,
        data_dir: &Path,
        local_addr: SocketAddr,
    ) -> anyhow::Result<Self> {
        let tor_binary = Self::find_tor_binary(config).ok_or_else(|| {
            anyhow::anyhow!("Tor binary not found. Install tor or set tor_binary in config.")
        })?;

        // Set up directories
        let tor_data_dir = config
            .tor_data_dir
            .clone()
            .unwrap_or_else(|| data_dir.join("tor"));
        let hs_dir = tor_data_dir.join("hidden_service");

        fs::create_dir_all(&tor_data_dir)?;
        fs::create_dir_all(&hs_dir)?;

        // On Unix, hidden service directory must have restricted permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&hs_dir, fs::Permissions::from_mode(0o700))?;
        }

        let local_port = local_addr.port();
        let hs_port = config.hidden_service_port;

        // Generate torrc
        let torrc_path = tor_data_dir.join("torrc");
        let torrc_content = Self::generate_torrc(
            &tor_data_dir,
            &hs_dir,
            hs_port,
            local_addr,
            DEFAULT_SOCKS_PORT,
            DEFAULT_CONTROL_PORT,
        );
        fs::write(&torrc_path, &torrc_content)?;

        info!("Starting Tor with hidden service...");
        debug!("Tor config: {}", torrc_path.display());
        debug!("Hidden service dir: {}", hs_dir.display());

        // Start Tor process
        let mut process = Command::new(&tor_binary)
            .arg("-f")
            .arg(&torrc_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Wait for Tor to bootstrap and create the hostname file
        let onion_address = Arc::new(Mutex::new(None));
        let hostname_path = hs_dir.join("hostname");

        // Spawn a task to monitor Tor output and detect bootstrap completion
        let stderr = process.stderr.take();
        if let Some(stderr) = stderr {
            let onion_addr = onion_address.clone();
            let hostname = hostname_path.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    if line.contains("Bootstrapped 100%") {
                        info!("Tor bootstrap complete");
                        // Read the hostname file
                        if let Ok(addr) = tokio::fs::read_to_string(&hostname).await {
                            let addr = addr.trim().to_string();
                            info!("Hidden service available at: {}", addr);
                            *onion_addr.lock().await = Some(addr);
                        }
                    } else if line.contains("[warn]") || line.contains("[err]") {
                        warn!("Tor: {}", line);
                    } else {
                        debug!("Tor: {}", line);
                    }
                }
            });
        }

        // Wait for the hostname file to appear (with timeout)
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(120);

        while start.elapsed() < timeout {
            if hostname_path.exists() {
                let addr = fs::read_to_string(&hostname_path)?.trim().to_string();
                if !addr.is_empty() {
                    *onion_address.lock().await = Some(addr.clone());
                    info!("Hidden service ready: {}", addr);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if onion_address.lock().await.is_none() {
            error!("Tor failed to create hidden service within timeout");
            if let Some(mut proc) = Some(process) {
                let _ = proc.kill();
            }
            return Err(anyhow::anyhow!(
                "Tor failed to create hidden service within {} seconds",
                timeout.as_secs()
            ));
        }

        Ok(Self {
            process: Some(process),
            hs_dir,
            onion_address,
            local_port,
            socks_port: DEFAULT_SOCKS_PORT,
        })
    }

    /// Generate a torrc configuration file.
    fn generate_torrc(
        data_dir: &Path,
        hs_dir: &Path,
        hs_port: u16,
        local_addr: SocketAddr,
        socks_port: u16,
        control_port: u16,
    ) -> String {
        format!(
            r#"# FOIAcquire Tor Configuration
# Auto-generated - do not edit manually

DataDirectory {data_dir}
SocksPort {socks_port}
ControlPort {control_port}

# Hidden Service Configuration
HiddenServiceDir {hs_dir}
HiddenServicePort {hs_port} {local_addr}

# Logging
Log notice stderr

# Safety settings
SafeLogging 1
"#,
            data_dir = data_dir.display(),
            hs_dir = hs_dir.display(),
            socks_port = socks_port,
            control_port = control_port,
            hs_port = hs_port,
            local_addr = local_addr,
        )
    }

    /// Get the .onion address for this hidden service.
    pub async fn onion_address(&self) -> Option<String> {
        self.onion_address.lock().await.clone()
    }

    /// Get the full .onion URL for this hidden service.
    pub async fn onion_url(&self) -> Option<String> {
        self.onion_address().await.map(|addr| {
            let port = if self.local_port == 80 {
                String::new()
            } else {
                format!(":{}", self.local_port)
            };
            format!("http://{}{}", addr, port)
        })
    }

    /// Get the SOCKS proxy URL for outbound connections.
    pub fn socks_url(&self) -> String {
        format!("socks5://127.0.0.1:{}", self.socks_port)
    }

    /// Get the hidden service directory path.
    pub fn hs_dir(&self) -> &Path {
        &self.hs_dir
    }

    /// Check if the hidden service is ready.
    pub async fn is_ready(&self) -> bool {
        self.onion_address.lock().await.is_some()
    }

    /// Shutdown the Tor process.
    pub fn shutdown(&mut self) {
        if let Some(ref mut process) = self.process {
            info!("Shutting down Tor process...");
            let _ = process.kill();
            let _ = process.wait();
        }
        self.process = None;
    }
}

impl Drop for CTorHiddenService {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_torrc() {
        let data_dir = PathBuf::from("/tmp/tor");
        let hs_dir = PathBuf::from("/tmp/tor/hidden_service");
        let local_addr: SocketAddr = "127.0.0.1:3030".parse().unwrap();

        let torrc =
            CTorHiddenService::generate_torrc(&data_dir, &hs_dir, 80, local_addr, 9050, 9051);

        assert!(torrc.contains("HiddenServicePort 80 127.0.0.1:3030"));
        assert!(torrc.contains("SocksPort 9050"));
        assert!(torrc.contains("ControlPort 9051"));
    }
}
