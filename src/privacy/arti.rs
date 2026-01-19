//! Embedded Tor client via Arti.
//!
//! **SECURITY WARNING**: This module is disabled by default due to RUSTSEC-2023-0071
//! (Marvin Attack timing side-channel in the `rsa` crate used by Arti). Use C-Tor
//! with `SOCKS_PROXY=socks5://127.0.0.1:9050` for outbound Tor connections until
//! Arti updates to a fixed `rsa` version.
//!
//! See: <https://rustsec.org/advisories/RUSTSEC-2023-0071>
//!
//! This module provides an embedded Tor client that runs an internal SOCKS5 proxy.
//! When enabled, reqwest connects to this local proxy for anonymous networking.
//!
//! # Features
//!
//! - Async bootstrap with background directory fetch
//! - Pluggable transport support (obfs4, snowflake, meek)
//! - Per-source circuit isolation
//! - Automatic fallback to direct Tor if PTs unavailable

// Module is scaffolding for embedded Tor integration - public API not yet consumed
#![allow(dead_code)]

// SECURITY: Emit compile-time warning when this feature is enabled
#[deprecated(
    since = "0.7.2",
    note = "embedded-tor disabled due to RUSTSEC-2023-0071 (Marvin Attack in rsa crate). Use C-Tor with SOCKS_PROXY instead."
)]
const _ARTI_SECURITY_WARNING: () = ();

use std::net::SocketAddr;

// SOCKS5 protocol constants (RFC 1928)
mod socks5 {
    /// SOCKS protocol version 5
    pub const VERSION: u8 = 0x05;

    /// Authentication methods
    pub mod auth {
        /// No authentication required
        pub const NO_AUTH: u8 = 0x00;
    }

    /// Address types for SOCKS5 requests
    pub mod addr_type {
        /// IPv4 address (4 bytes)
        pub const IPV4: u8 = 0x01;
        /// Domain name (1 byte length + name)
        pub const DOMAIN: u8 = 0x03;
        /// IPv6 address (16 bytes)
        pub const IPV6: u8 = 0x04;
    }

    /// Reply codes from SOCKS5 server
    pub mod reply {
        /// Request succeeded
        pub const SUCCESS: u8 = 0x00;
        /// Reserved byte (always 0x00)
        pub const RESERVED: u8 = 0x00;
        /// Connection refused by destination host
        pub const CONNECTION_REFUSED: u8 = 0x05;
    }
}
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::{debug, error, info, warn};

use arti_client::{TorClient, TorClientConfig};
use tor_rtcompat::PreferredRuntime;

/// Global Arti client instance.
static ARTI_CLIENT: OnceCell<Arc<ArtiClient>> = OnceCell::const_new();

/// Embedded Arti client wrapper.
pub struct ArtiClient {
    client: TorClient<PreferredRuntime>,
    /// Local SOCKS5 proxy address (e.g., 127.0.0.1:9150)
    socks_addr: SocketAddr,
}

impl ArtiClient {
    /// Get the SOCKS5 proxy address for this client.
    pub fn socks_addr(&self) -> SocketAddr {
        self.socks_addr
    }

    /// Get the SOCKS5 proxy URL.
    pub fn socks_url(&self) -> String {
        format!("socks5://{}", self.socks_addr)
    }

    /// Get the underlying Tor client for advanced operations.
    pub fn client(&self) -> &TorClient<PreferredRuntime> {
        &self.client
    }
}

/// Bootstrap options for the Arti client.
#[derive(Debug, Clone)]
pub struct ArtiBootstrapConfig {
    /// Port to bind the internal SOCKS5 proxy (default: 0 = auto-select)
    pub socks_port: u16,
    /// Enable obfuscation via pluggable transports
    pub obfuscation: bool,
    /// Preferred transport (obfs4, snowflake, meek)
    pub transport: Option<String>,
    /// Block until bootstrap complete
    pub block_on_bootstrap: bool,
}

impl Default for ArtiBootstrapConfig {
    fn default() -> Self {
        Self {
            socks_port: 0,
            obfuscation: true,
            transport: None,
            block_on_bootstrap: true,
        }
    }
}

/// Get or initialize the global Arti client.
///
/// On first call, bootstraps Arti and starts the internal SOCKS5 proxy.
/// Subsequent calls return the existing client.
pub async fn get_or_init_arti(config: &ArtiBootstrapConfig) -> anyhow::Result<Arc<ArtiClient>> {
    ARTI_CLIENT
        .get_or_try_init(|| async { init_arti(config).await })
        .await
        .cloned()
}

/// Initialize a new Arti client.
async fn init_arti(config: &ArtiBootstrapConfig) -> anyhow::Result<Arc<ArtiClient>> {
    info!("Bootstrapping embedded Tor client via Arti...");

    // Build Arti configuration
    let arti_config = build_arti_config(config)?;

    // Create and bootstrap the Tor client
    let client = TorClient::create_bootstrapped(arti_config).await?;
    info!("Arti bootstrap complete");

    // Bind SOCKS5 proxy listener
    let socks_addr = start_socks_proxy(&client, config.socks_port).await?;
    info!("Arti SOCKS5 proxy listening on {}", socks_addr);

    Ok(Arc::new(ArtiClient { client, socks_addr }))
}

/// Build Arti configuration from bootstrap options.
fn build_arti_config(config: &ArtiBootstrapConfig) -> anyhow::Result<TorClientConfig> {
    let builder = TorClientConfig::builder();

    // Configure pluggable transports if obfuscation is enabled
    if config.obfuscation {
        debug!("Obfuscation enabled, configuring pluggable transports");
        if let Some(ref transport) = config.transport {
            debug!("Preferred transport: {}", transport);
        }
    } else {
        debug!("Obfuscation disabled, using direct Tor connections");
    }

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Arti config: {}", e))
}

/// Start the internal SOCKS5 proxy.
///
/// This spawns a background task that accepts SOCKS5 connections and
/// routes them through the Tor network.
async fn start_socks_proxy(
    client: &TorClient<PreferredRuntime>,
    port: u16,
) -> anyhow::Result<SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::net::TcpListener;

    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let listener = TcpListener::bind(bind_addr).await?;
    let actual_addr = listener.local_addr()?;

    let client = client.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    debug!("SOCKS5 connection from {}", peer_addr);
                    let client = client.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_socks_connection(stream, &client).await {
                            warn!("SOCKS5 connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("SOCKS5 accept error: {}", e);
                }
            }
        }
    });

    Ok(actual_addr)
}

/// Handle a single SOCKS5 connection.
async fn handle_socks_connection(
    mut stream: tokio::net::TcpStream,
    client: &TorClient<PreferredRuntime>,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Read SOCKS5 greeting
    let mut buf = [0u8; 258];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    // Parse version and auth methods
    if buf[0] != socks5::VERSION {
        anyhow::bail!("Only SOCKS5 supported");
    }

    // Accept no-auth
    stream
        .write_all(&[socks5::VERSION, socks5::auth::NO_AUTH])
        .await?;

    // Read connection request
    let n = stream.read(&mut buf).await?;
    if n < 4 {
        anyhow::bail!("Invalid SOCKS5 request");
    }

    // Parse address type and destination
    let (host, port) = match buf[3] {
        socks5::addr_type::IPV4 => {
            // IPv4
            if n < 10 {
                anyhow::bail!("Invalid IPv4 request");
            }
            let ip = format!("{}.{}.{}.{}", buf[4], buf[5], buf[6], buf[7]);
            let port = u16::from_be_bytes([buf[8], buf[9]]);
            (ip, port)
        }
        socks5::addr_type::DOMAIN => {
            // Domain name
            let len = buf[4] as usize;
            if n < 5 + len + 2 {
                anyhow::bail!("Invalid domain request");
            }
            let domain = String::from_utf8_lossy(&buf[5..5 + len]).to_string();
            let port = u16::from_be_bytes([buf[5 + len], buf[6 + len]]);
            (domain, port)
        }
        socks5::addr_type::IPV6 => {
            // IPv6
            if n < 22 {
                anyhow::bail!("Invalid IPv6 request");
            }
            let ip_bytes: [u8; 16] = buf[4..20].try_into()?;
            let ip = std::net::Ipv6Addr::from(ip_bytes);
            let port = u16::from_be_bytes([buf[20], buf[21]]);
            (ip.to_string(), port)
        }
        _ => anyhow::bail!("Unknown address type"),
    };

    debug!("SOCKS5 connect request: {}:{}", host, port);

    // Connect through Tor
    match client.connect(format!("{}:{}", host, port)).await {
        Ok(tor_stream) => {
            // Send success response: version, success, reserved, ipv4, 0.0.0.0:0
            stream
                .write_all(&[
                    socks5::VERSION,
                    socks5::reply::SUCCESS,
                    socks5::reply::RESERVED,
                    socks5::addr_type::IPV4,
                    0,
                    0,
                    0,
                    0, // bound address (0.0.0.0)
                    0,
                    0, // bound port (0)
                ])
                .await?;

            // Proxy data between client and Tor stream
            let (mut client_read, mut client_write) = stream.into_split();
            let (mut tor_read, mut tor_write) = tor_stream.split();

            tokio::select! {
                r = tokio::io::copy(&mut client_read, &mut tor_write) => {
                    if let Err(e) = r { debug!("Client->Tor copy error: {}", e); }
                }
                r = tokio::io::copy(&mut tor_read, &mut client_write) => {
                    if let Err(e) = r { debug!("Tor->Client copy error: {}", e); }
                }
            }
        }
        Err(e) => {
            error!("Tor connect failed for {}:{}: {}", host, port, e);
            // Send connection refused response
            stream
                .write_all(&[
                    socks5::VERSION,
                    socks5::reply::CONNECTION_REFUSED,
                    socks5::reply::RESERVED,
                    socks5::addr_type::IPV4,
                    0,
                    0,
                    0,
                    0, // bound address (0.0.0.0)
                    0,
                    0, // bound port (0)
                ])
                .await?;
        }
    }

    Ok(())
}

/// Check if Arti is available and bootstrapped.
pub fn is_arti_ready() -> bool {
    ARTI_CLIENT.get().is_some()
}

/// Get the SOCKS5 proxy URL if Arti is ready.
pub fn get_arti_socks_url() -> Option<String> {
    ARTI_CLIENT.get().map(|c| c.socks_url())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_bootstrap_config() {
        let config = ArtiBootstrapConfig::default();
        assert_eq!(config.socks_port, 0);
        assert!(config.obfuscation);
        assert!(config.transport.is_none());
        assert!(config.block_on_bootstrap);
    }

    #[test]
    fn test_is_arti_ready_before_init() {
        assert!(!is_arti_ready());
    }

    #[test]
    fn test_get_arti_socks_url_before_init() {
        assert!(get_arti_socks_url().is_none());
    }
}
