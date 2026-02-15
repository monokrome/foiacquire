//! Privacy configuration for Tor and SOCKS proxy routing.

use serde::{Deserialize, Serialize};
use std::env;

/// Read SOCKS proxy URL from environment.
pub fn socks_proxy_from_env() -> Option<String> {
    env::var("SOCKS_PROXY").ok().filter(|s| !s.is_empty())
}

#[cfg(not(feature = "embedded-tor"))]
use std::net::TcpStream;
#[cfg(not(feature = "embedded-tor"))]
use std::time::Duration;

pub use super::hidden_service::{
    HiddenServiceConfig, HiddenServiceProvider, HiddenServiceSecurityLevel,
};

/// Pluggable transport type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// obfs4 - looks like random noise (default when obfuscation enabled)
    #[default]
    Obfs4,
    /// snowflake - WebRTC-based, uses volunteer proxies
    Snowflake,
    /// meek - domain fronting, looks like cloud service traffic
    Meek,
    /// Direct Tor connection (no pluggable transport)
    Direct,
}

impl prefer::FromValue for Transport {
    fn from_value(value: &prefer::ConfigValue) -> prefer::Result<Self> {
        match value.as_str() {
            Some(s) => match s.to_lowercase().as_str() {
                "obfs4" => Ok(Transport::Obfs4),
                "snowflake" => Ok(Transport::Snowflake),
                "meek" => Ok(Transport::Meek),
                "direct" => Ok(Transport::Direct),
                other => Err(prefer::Error::ConversionError {
                    key: String::new(),
                    type_name: "Transport".to_string(),
                    source: format!("unknown transport: {}", other).into(),
                }),
            },
            None => Err(prefer::Error::ConversionError {
                key: String::new(),
                type_name: "Transport".to_string(),
                source: "expected string".into(),
            }),
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Obfs4 => write!(f, "obfs4"),
            Transport::Snowflake => write!(f, "snowflake"),
            Transport::Meek => write!(f, "meek"),
            Transport::Direct => write!(f, "direct"),
        }
    }
}

/// Privacy mode determining how requests are routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivacyMode {
    /// Route through Tor with pluggable transport obfuscation (default).
    /// Blocks until PT is available.
    TorObfuscated(Transport),
    /// Route through Tor without obfuscation (detectable as Tor).
    TorDirect,
    /// Route through user-provided SOCKS proxy.
    ExternalProxy,
    /// Direct connection without Tor (prints security warning).
    Direct,
}

impl Default for PrivacyMode {
    fn default() -> Self {
        // Default: Tor with obfs4 obfuscation
        PrivacyMode::TorObfuscated(Transport::Obfs4)
    }
}

impl std::fmt::Display for PrivacyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrivacyMode::TorObfuscated(t) => write!(f, "tor+{}", t),
            PrivacyMode::TorDirect => write!(f, "tor"),
            PrivacyMode::ExternalProxy => write!(f, "socks-proxy"),
            PrivacyMode::Direct => write!(f, "direct"),
        }
    }
}

/// Per-source privacy configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, prefer::FromValue)]
pub struct SourcePrivacyConfig {
    /// Skip Tor entirely for this source.
    #[serde(default)]
    #[prefer(default)]
    pub direct: bool,

    /// Use Tor without pluggable transports (detectable as Tor traffic).
    /// Ignored if `direct` is true.
    #[serde(default = "default_true")]
    #[prefer(default)]
    pub obfuscation: bool,

    /// Force a specific transport (obfs4, snowflake, meek).
    /// Ignored if `obfuscation` is false or `direct` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
    pub transport: Option<Transport>,

    /// Use a dedicated Tor circuit for this source (different exit IP).
    #[serde(default)]
    #[prefer(default)]
    pub isolate: bool,
}

impl Default for SourcePrivacyConfig {
    fn default() -> Self {
        Self {
            direct: false,
            obfuscation: true, // Default to obfuscated
            transport: None,
            isolate: false,
        }
    }
}

fn default_true() -> bool {
    true
}

impl SourcePrivacyConfig {
    /// Check if this is the default (empty) config.
    pub fn is_default(&self) -> bool {
        !self.direct && self.obfuscation && self.transport.is_none() && !self.isolate
    }
}

impl SourcePrivacyConfig {
    /// Resolve the effective privacy mode for this source.
    #[allow(dead_code)] // Public API for future per-source mode resolution
    pub fn resolve_mode(&self, global: &PrivacyConfig) -> PrivacyMode {
        // Per-source direct overrides everything
        if self.direct {
            return PrivacyMode::Direct;
        }

        // External proxy takes precedence over embedded Tor
        if global.socks_proxy.is_some() {
            return PrivacyMode::ExternalProxy;
        }

        // Global direct mode
        if global.direct {
            return PrivacyMode::Direct;
        }

        // Determine obfuscation
        let use_obfuscation = self.obfuscation && global.obfuscation;

        if use_obfuscation {
            let transport = self
                .transport
                .or(global.transport)
                .unwrap_or(Transport::Obfs4);
            PrivacyMode::TorObfuscated(transport)
        } else {
            PrivacyMode::TorDirect
        }
    }

    /// Create an effective PrivacyConfig by combining global settings with source overrides.
    /// This returns a PrivacyConfig that can be passed to HttpClient.
    pub fn apply_to(&self, global: &PrivacyConfig) -> PrivacyConfig {
        let mut effective = global.clone();

        // Per-source direct overrides global
        if self.direct {
            effective.direct = true;
        }

        // Per-source can disable obfuscation
        if !self.obfuscation {
            effective.obfuscation = false;
        }

        // Per-source transport overrides global
        if self.transport.is_some() {
            effective.transport = self.transport;
        }

        effective
    }
}

/// Default warning delay in seconds.
const DEFAULT_WARNING_DELAY: u64 = 15;

/// Minimum warning delay in seconds (enforced regardless of config).
const MIN_WARNING_DELAY: u64 = 3;

/// Global privacy configuration.
#[derive(Debug, Clone, Serialize, Deserialize, prefer::FromValue)]
pub struct PrivacyConfig {
    /// Disable Tor entirely (direct connections).
    /// Set via `--direct` flag or `FOIA_DIRECT=1`.
    #[serde(default)]
    #[prefer(default)]
    pub direct: bool,

    /// Enable pluggable transport obfuscation (default: true).
    /// Set to false via `--no-obfuscation` or `FOIA_NO_OBFUSCATION=1`.
    #[serde(default = "default_true")]
    #[prefer(default)]
    pub obfuscation: bool,

    /// External SOCKS5 proxy URL (bypasses embedded Arti).
    /// Set via `SOCKS_PROXY` environment variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
    pub socks_proxy: Option<String>,

    /// Default transport when obfuscation is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
    pub transport: Option<Transport>,

    /// Bridge configuration for obfs4.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[prefer(default)]
    pub bridges: Vec<String>,

    /// Delay in seconds before proceeding when insecure (default: 15).
    /// Set via `--privacy-warning-delay` flag.
    /// Warning is always shown; clamped to a minimum of 3 seconds.
    #[serde(
        default = "default_warning_delay",
        skip_serializing_if = "is_default_warning_delay"
    )]
    #[prefer(default = "15")]
    pub warning_delay: u64,

    /// Show Tor legality warning (default: true).
    /// Can be disabled via `--no-tor-warning`.
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    #[prefer(default)]
    pub tor_legal_warning: bool,

    /// Hidden service configuration for server mode.
    #[serde(default, skip_serializing_if = "HiddenServiceConfig::is_default")]
    #[prefer(default)]
    pub hidden_service: HiddenServiceConfig,
}

fn is_true(v: &bool) -> bool {
    *v
}

fn default_warning_delay() -> u64 {
    DEFAULT_WARNING_DELAY
}

fn is_default_warning_delay(v: &u64) -> bool {
    *v == DEFAULT_WARNING_DELAY
}

impl Default for PrivacyConfig {
    fn default() -> Self {
        Self::base_default().with_env_overrides()
    }
}

impl PrivacyConfig {
    /// Base default without env overrides (used internally to avoid recursion).
    fn base_default() -> Self {
        Self {
            direct: false,
            obfuscation: true,
            socks_proxy: None,
            transport: None,
            bridges: Vec::new(),
            warning_delay: DEFAULT_WARNING_DELAY,
            tor_legal_warning: true,
            hidden_service: HiddenServiceConfig::default(),
        }
    }

    /// Check if this is the default config.
    pub fn is_default(&self) -> bool {
        !self.direct
            && self.obfuscation
            && self.socks_proxy.is_none()
            && self.transport.is_none()
            && self.bridges.is_empty()
            && self.warning_delay == DEFAULT_WARNING_DELAY
            && self.hidden_service.is_default()
            && self.tor_legal_warning
    }

    /// Apply environment variable overrides.
    pub fn with_env_overrides(mut self) -> Self {
        // SOCKS_PROXY takes highest precedence
        if let Some(proxy) = socks_proxy_from_env() {
            self.socks_proxy = Some(proxy);
        }

        // FOIA_DIRECT=1 disables Tor
        if env::var("FOIA_DIRECT")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            self.direct = true;
        }

        // FOIA_NO_OBFUSCATION=1 disables PTs
        if env::var("FOIA_NO_OBFUSCATION")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            self.obfuscation = false;
        }

        // Apply hidden service env overrides
        self.hidden_service = std::mem::take(&mut self.hidden_service).with_env_overrides();

        self
    }

    /// Apply CLI flag overrides.
    pub fn with_cli_overrides(
        mut self,
        direct: bool,
        no_obfuscation: bool,
        warning_delay: Option<u64>,
        no_tor_warning: bool,
    ) -> Self {
        if direct {
            self.direct = true;
        }
        if no_obfuscation {
            self.obfuscation = false;
        }
        if let Some(delay) = warning_delay {
            self.warning_delay = delay;
        }
        if no_tor_warning {
            self.tor_legal_warning = false;
        }
        self
    }

    /// Get the effective privacy mode.
    pub fn mode(&self) -> PrivacyMode {
        // External proxy takes precedence
        if self.socks_proxy.is_some() {
            return PrivacyMode::ExternalProxy;
        }

        // Direct mode
        if self.direct {
            return PrivacyMode::Direct;
        }

        // Tor with or without obfuscation
        if self.obfuscation {
            let transport = self.transport.unwrap_or(Transport::Obfs4);
            PrivacyMode::TorObfuscated(transport)
        } else {
            PrivacyMode::TorDirect
        }
    }

    /// Get the SOCKS proxy URL if configured (external proxy only).
    pub fn proxy_url(&self) -> Option<&str> {
        self.socks_proxy.as_deref()
    }

    /// Get the effective proxy URL for external commands (like yt-dlp).
    ///
    /// Returns the proxy URL to use, checking in order:
    /// 1. External SOCKS proxy from config
    /// 2. Embedded Arti proxy (if running)
    /// 3. None if direct mode
    pub fn effective_proxy_url(&self) -> Option<String> {
        // Direct mode = no proxy
        if self.direct {
            return None;
        }

        // External proxy takes precedence
        if let Some(ref proxy) = self.socks_proxy {
            return Some(proxy.clone());
        }

        // Try embedded Arti if available
        #[cfg(feature = "embedded-tor")]
        if let Some(url) = crate::privacy::get_arti_socks_url() {
            return Some(url);
        }

        None
    }

    /// Check if using embedded Arti (vs external proxy or direct).
    #[allow(dead_code)] // Public API for embedded-tor feature integration
    pub fn uses_embedded_tor(&self) -> bool {
        !self.direct && self.socks_proxy.is_none()
    }

    /// Check if Tor is enabled (embedded or external proxy).
    pub fn uses_tor(&self) -> bool {
        !self.direct
    }

    /// Default C-Tor SOCKS port.
    #[cfg(not(feature = "embedded-tor"))]
    const DEFAULT_TOR_SOCKS_PORT: u16 = 9050;

    /// Check if Tor is available when needed.
    ///
    /// Returns Ok(()) if:
    /// - Direct mode (no Tor needed)
    /// - External SOCKS proxy configured
    /// - Embedded Tor feature enabled
    /// - C-Tor is running at 127.0.0.1:9050
    ///
    /// Returns Err with setup instructions if Tor is needed but unavailable.
    pub fn check_tor_availability(&self) -> Result<(), String> {
        // Direct mode - no Tor needed
        if self.direct {
            return Ok(());
        }

        // External proxy configured - user manages their own Tor
        if self.socks_proxy.is_some() {
            return Ok(());
        }

        // Embedded Tor available
        #[cfg(feature = "embedded-tor")]
        {
            Ok(())
        }

        // No embedded Tor - check if C-Tor is running
        #[cfg(not(feature = "embedded-tor"))]
        {
            let addr = format!("127.0.0.1:{}", Self::DEFAULT_TOR_SOCKS_PORT);
            match TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(2)) {
                Ok(_) => {
                    // C-Tor is running, auto-configure SOCKS proxy
                    Ok(())
                }
                Err(_) => Err(r#"Tor is required but not available.

The embedded Tor client (Arti) is disabled due to a security vulnerability
(RUSTSEC-2023-0071: Marvin Attack in rsa crate).

To use Tor, please set up C-Tor:

1. Install Tor:
   - Debian/Ubuntu: sudo apt install tor
   - macOS: brew install tor
   - Arch: sudo pacman -S tor

2. Start the Tor daemon:
   tor &

   foia will automatically use the SOCKS proxy at 127.0.0.1:9050.

Alternatively, use --direct flag to skip Tor (not recommended for sensitive work).

See https://rustsec.org/advisories/RUSTSEC-2023-0071 for details."#
                    .to_string()),
            }
        }
    }

    /// Get the effective SOCKS proxy URL, auto-detecting C-Tor if needed.
    #[allow(dead_code)] // Public API for Tor proxy configuration
    pub fn get_socks_proxy_url(&self) -> Option<String> {
        // Explicit proxy configured
        if let Some(ref proxy) = self.socks_proxy {
            return Some(proxy.clone());
        }

        // Direct mode - no proxy
        if self.direct {
            return None;
        }

        // Check for embedded Tor
        #[cfg(feature = "embedded-tor")]
        {
            crate::privacy::get_arti_socks_url()
        }

        // Fall back to default C-Tor port
        #[cfg(not(feature = "embedded-tor"))]
        {
            Some(format!(
                "socks5://127.0.0.1:{}",
                Self::DEFAULT_TOR_SOCKS_PORT
            ))
        }
    }

    /// Display Tor legality warning if enabled and Tor is in use.
    pub fn show_tor_legal_warning(&self) {
        use std::io::{self, Write};

        if !self.tor_legal_warning || !self.uses_tor() {
            return;
        }

        eprintln!();
        eprintln!("Note: Tor may be illegal or monitored in some jurisdictions.");
        eprintln!("      Know your local laws before proceeding.");
        eprintln!("      Disable this warning with --no-tor-warning");
        eprintln!();
        let _ = io::stderr().flush();
    }

    /// Check if any insecure configuration is active.
    /// Returns the security level: Secure, NoObfuscation, or Direct.
    #[allow(dead_code)]
    pub fn security_level(&self) -> SecurityLevel {
        if self.direct {
            SecurityLevel::Direct
        } else if !self.obfuscation && self.socks_proxy.is_none() {
            // Using Tor without obfuscation (and not external proxy)
            SecurityLevel::NoObfuscation
        } else {
            SecurityLevel::Secure
        }
    }

    /// Display mandatory security warning and countdown if insecure.
    /// The warning is always shown; the countdown can be adjusted with warning_delay.
    /// The delay is clamped to a minimum of 3 seconds.
    ///
    /// When compiled with `unsafe-dev` feature, warnings are skipped entirely.
    pub async fn enforce_security_warning(&self) {
        tracing::debug!(
            "Security check: direct={}, obfuscation={}, warning_delay={}",
            self.direct,
            self.obfuscation,
            self.warning_delay
        );

        // Skip all warnings in unsafe-dev mode (for development only)
        #[cfg(feature = "unsafe-dev")]
        return;

        #[cfg(not(feature = "unsafe-dev"))]
        match self.security_level() {
            SecurityLevel::Secure => {}
            SecurityLevel::NoObfuscation => {
                self.display_warning(
                    "This deployment uses Tor but it is not obfuscated.",
                    &[
                        "Your traffic is identifiable as Tor by network observers.",
                        "Some networks block or flag Tor connections.",
                    ],
                )
                .await;
            }
            SecurityLevel::Direct => {
                self.display_warning(
                    "This deployment is insecure. Tor is disabled.",
                    &[
                        "Your IP address will be visible to target servers.",
                        "DNS queries may reveal your investigation subjects.",
                    ],
                )
                .await;
            }
        }
    }

    /// Display a warning with configurable countdown.
    #[cfg_attr(feature = "unsafe-dev", allow(dead_code))]
    async fn display_warning(&self, message: &str, details: &[&str]) {
        display_security_warning(message, details, self.warning_delay).await;
    }
}

/// Security level indicating how protected the user is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "unsafe-dev", allow(dead_code))]
pub enum SecurityLevel {
    /// Fully secure: Tor with obfuscation or external proxy.
    Secure,
    /// Tor enabled but not obfuscated (detectable as Tor traffic).
    NoObfuscation,
    /// Direct connection without Tor (fully exposed).
    Direct,
}

/// Display a security warning with countdown. Single implementation used by
/// both privacy warnings and hidden service warnings.
///
/// The delay is clamped to `MIN_WARNING_DELAY` — callers cannot skip the countdown.
pub(crate) async fn display_security_warning(message: &str, details: &[&str], raw_delay: u64) {
    use std::io::{self, Write};

    eprintln!();
    eprintln!("WARNING: {}", message);
    eprintln!("Press CTRL+C to abort.");
    eprintln!();
    for detail in details {
        eprintln!("{}", detail);
    }
    eprintln!();
    eprintln!("For security reasons, this message cannot be disabled.");
    eprintln!();
    let _ = io::stderr().flush();

    let delay = raw_delay.max(MIN_WARNING_DELAY);
    for i in (1..=delay).rev() {
        eprint!("\rContinuing in {} seconds...  ", i);
        let _ = io::stderr().flush();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    eprintln!();
    let _ = io::stderr().flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_mode() {
        let config = PrivacyConfig::default();
        assert!(matches!(
            config.mode(),
            PrivacyMode::TorObfuscated(Transport::Obfs4)
        ));
    }

    #[test]
    fn test_direct_mode() {
        let config = PrivacyConfig {
            direct: true,
            ..Default::default()
        };
        assert!(matches!(config.mode(), PrivacyMode::Direct));
    }

    #[test]
    fn test_external_proxy_mode() {
        let config = PrivacyConfig {
            socks_proxy: Some("socks5://127.0.0.1:9050".into()),
            ..Default::default()
        };
        assert!(matches!(config.mode(), PrivacyMode::ExternalProxy));
    }

    #[test]
    fn test_no_obfuscation_mode() {
        let config = PrivacyConfig {
            obfuscation: false,
            ..Default::default()
        };
        assert!(matches!(config.mode(), PrivacyMode::TorDirect));
    }

    #[test]
    fn test_source_config_direct() {
        let global = PrivacyConfig::default();
        let source = SourcePrivacyConfig {
            direct: true,
            ..Default::default()
        };
        assert!(matches!(source.resolve_mode(&global), PrivacyMode::Direct));
    }

    #[test]
    fn test_source_config_inherits_global() {
        let global = PrivacyConfig {
            obfuscation: false,
            ..Default::default()
        };
        let source = SourcePrivacyConfig::default();
        assert!(matches!(
            source.resolve_mode(&global),
            PrivacyMode::TorDirect
        ));
    }

    #[test]
    fn test_source_config_specific_transport() {
        let global = PrivacyConfig::default();
        let source = SourcePrivacyConfig {
            transport: Some(Transport::Snowflake),
            ..Default::default()
        };
        assert!(matches!(
            source.resolve_mode(&global),
            PrivacyMode::TorObfuscated(Transport::Snowflake)
        ));
    }

    #[test]
    fn test_external_proxy_overrides_source() {
        let global = PrivacyConfig {
            socks_proxy: Some("socks5://127.0.0.1:9050".into()),
            ..Default::default()
        };
        let source = SourcePrivacyConfig {
            obfuscation: false,
            ..Default::default()
        };
        // External proxy takes precedence over source config
        assert!(matches!(
            source.resolve_mode(&global),
            PrivacyMode::ExternalProxy
        ));
    }

    #[test]
    fn test_apply_to_direct_override() {
        let global = PrivacyConfig::default();
        let source = SourcePrivacyConfig {
            direct: true,
            ..Default::default()
        };
        let effective = source.apply_to(&global);
        assert!(effective.direct);
        assert!(matches!(effective.mode(), PrivacyMode::Direct));
    }

    #[test]
    fn test_apply_to_obfuscation_override() {
        let global = PrivacyConfig::default();
        assert!(global.obfuscation); // Verify default is true

        let source = SourcePrivacyConfig {
            obfuscation: false,
            ..Default::default()
        };
        let effective = source.apply_to(&global);
        assert!(!effective.obfuscation);
        assert!(matches!(effective.mode(), PrivacyMode::TorDirect));
    }

    #[test]
    fn test_apply_to_transport_override() {
        let global = PrivacyConfig::default();
        let source = SourcePrivacyConfig {
            transport: Some(Transport::Snowflake),
            ..Default::default()
        };
        let effective = source.apply_to(&global);
        assert_eq!(effective.transport, Some(Transport::Snowflake));
        assert!(matches!(
            effective.mode(),
            PrivacyMode::TorObfuscated(Transport::Snowflake)
        ));
    }

    #[test]
    fn test_apply_to_preserves_socks_proxy() {
        let global = PrivacyConfig {
            socks_proxy: Some("socks5://127.0.0.1:9050".into()),
            ..Default::default()
        };
        let source = SourcePrivacyConfig::default();
        let effective = source.apply_to(&global);
        assert_eq!(
            effective.socks_proxy,
            Some("socks5://127.0.0.1:9050".into())
        );
        assert!(matches!(effective.mode(), PrivacyMode::ExternalProxy));
    }

    #[test]
    fn test_apply_to_default_source_no_change() {
        let global = PrivacyConfig {
            obfuscation: true,
            transport: Some(Transport::Meek),
            ..Default::default()
        };
        let source = SourcePrivacyConfig::default();
        let effective = source.apply_to(&global);
        // Default source config should not change global settings
        assert_eq!(effective.obfuscation, global.obfuscation);
        assert_eq!(effective.transport, global.transport);
        assert_eq!(effective.direct, global.direct);
    }
}
