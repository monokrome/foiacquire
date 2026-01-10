//! Privacy configuration for Tor and SOCKS proxy routing.

use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourcePrivacyConfig {
    /// Skip Tor entirely for this source.
    #[serde(default)]
    pub direct: bool,

    /// Use Tor without pluggable transports (detectable as Tor traffic).
    /// Ignored if `direct` is true.
    #[serde(default = "default_true")]
    pub obfuscation: bool,

    /// Force a specific transport (obfs4, snowflake, meek).
    /// Ignored if `obfuscation` is false or `direct` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<Transport>,

    /// Use a dedicated Tor circuit for this source (different exit IP).
    #[serde(default)]
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

/// Hidden service provider type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HiddenServiceProvider {
    /// C-Tor - the reference implementation (default, most secure)
    #[default]
    CTor,
    /// Arti - Rust Tor implementation (experimental for hidden services)
    Arti,
    /// No hidden service - direct HTTP only
    None,
}

impl std::fmt::Display for HiddenServiceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HiddenServiceProvider::CTor => write!(f, "c-tor"),
            HiddenServiceProvider::Arti => write!(f, "arti"),
            HiddenServiceProvider::None => write!(f, "none"),
        }
    }
}

/// Configuration for hidden service (onion service) hosting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HiddenServiceConfig {
    /// Hidden service provider (c-tor, arti, or none).
    /// Default: c-tor (most secure, recommended by Tor Project)
    #[serde(default)]
    pub provider: HiddenServiceProvider,

    /// Allow potentially insecure experimental circuits.
    /// Required to use Arti for hidden services (Arti warns their onion
    /// service implementation is "not yet as secure as C-Tor").
    /// Default: false (safe default)
    #[serde(default)]
    pub allow_potentially_insecure_circuits: bool,

    /// Path to tor binary (default: search PATH for "tor")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tor_binary: Option<PathBuf>,

    /// Directory for Tor data (default: data_dir/tor)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tor_data_dir: Option<PathBuf>,

    /// Hidden service port (what the onion service advertises)
    /// Default: 80
    #[serde(default = "default_hidden_service_port")]
    pub hidden_service_port: u16,

    /// Also listen on clearnet (in addition to hidden service)
    /// Default: false (hidden service only for maximum privacy)
    #[serde(default)]
    pub also_listen_clearnet: bool,

    /// Clearnet bind address (only used if also_listen_clearnet is true)
    /// Default: 127.0.0.1:3030
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clearnet_bind: Option<String>,
}

fn default_hidden_service_port() -> u16 {
    80
}

impl Default for HiddenServiceConfig {
    fn default() -> Self {
        Self {
            provider: HiddenServiceProvider::CTor,
            allow_potentially_insecure_circuits: false,
            tor_binary: None,
            tor_data_dir: None,
            hidden_service_port: 80,
            also_listen_clearnet: false,
            clearnet_bind: None,
        }
    }
}

impl HiddenServiceConfig {
    /// Check if this is the default config (used for skip_serializing_if).
    pub fn is_default(&self) -> bool {
        self.provider == HiddenServiceProvider::CTor
            && !self.allow_potentially_insecure_circuits
            && self.tor_binary.is_none()
            && self.tor_data_dir.is_none()
            && self.hidden_service_port == 80
            && !self.also_listen_clearnet
            && self.clearnet_bind.is_none()
    }

    /// Apply environment variable overrides.
    pub fn with_env_overrides(mut self) -> Self {
        // FOIACQUIRE_HS_PROVIDER - hidden service provider
        if let Ok(provider) = env::var("FOIACQUIRE_HS_PROVIDER") {
            match provider.to_lowercase().as_str() {
                "c-tor" | "ctor" | "tor" => self.provider = HiddenServiceProvider::CTor,
                "arti" => self.provider = HiddenServiceProvider::Arti,
                "none" | "disabled" => self.provider = HiddenServiceProvider::None,
                _ => {}
            }
        }

        // FOIACQUIRE_ALLOW_INSECURE_CIRCUITS=1 - allow experimental Arti onion services
        if env::var("FOIACQUIRE_ALLOW_INSECURE_CIRCUITS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            self.allow_potentially_insecure_circuits = true;
        }

        // FOIACQUIRE_TOR_BINARY - path to tor binary
        if let Ok(path) = env::var("FOIACQUIRE_TOR_BINARY") {
            if !path.is_empty() {
                self.tor_binary = Some(PathBuf::from(path));
            }
        }

        self
    }

    /// Validate the configuration, returning an error if invalid.
    pub fn validate(&self) -> Result<(), String> {
        match self.provider {
            HiddenServiceProvider::Arti if !self.allow_potentially_insecure_circuits => {
                Err(
                    "Arti hidden services require explicit opt-in due to security concerns.\n\n\
                     Arti's onion service implementation is experimental and not yet as secure as C-Tor.\n\n\
                     To proceed with Arti (NOT RECOMMENDED for production):\n\
                     - Set 'allow_potentially_insecure_circuits = true' in [privacy.hidden_service] config\n\
                     - Or set FOIACQUIRE_ALLOW_INSECURE_CIRCUITS=1 environment variable\n\n\
                     For production deployments, use C-Tor (the default)."
                        .to_string(),
                )
            }
            _ => Ok(()),
        }
    }

    /// Check if hidden service is enabled.
    pub fn is_enabled(&self) -> bool {
        self.provider != HiddenServiceProvider::None
    }

    /// Get the security level for this hidden service configuration.
    pub fn security_level(&self) -> HiddenServiceSecurityLevel {
        match self.provider {
            HiddenServiceProvider::CTor => HiddenServiceSecurityLevel::Secure,
            HiddenServiceProvider::Arti => HiddenServiceSecurityLevel::Experimental,
            HiddenServiceProvider::None => HiddenServiceSecurityLevel::Disabled,
        }
    }

    /// Display mandatory security warning if using experimental hidden services.
    pub async fn enforce_security_warning(&self, warning_delay: u64) {
        match self.security_level() {
            HiddenServiceSecurityLevel::Secure => {}
            HiddenServiceSecurityLevel::Experimental => {
                display_hs_warning(
                    "EXPERIMENTAL: Using Arti for hidden services.",
                    &[
                        "Arti's onion service implementation is not yet as secure as C-Tor.",
                        "This configuration should NOT be used for production deployments.",
                        "Consider using C-Tor (the default) for maximum security.",
                    ],
                    warning_delay,
                )
                .await;
            }
            HiddenServiceSecurityLevel::Disabled => {
                display_hs_warning(
                    "Hidden services DISABLED. Server will be accessible via clearnet only.",
                    &[
                        "Your server's IP address will be visible to all clients.",
                        "Connections are not anonymized through Tor.",
                        "Consider enabling hidden services for privacy protection.",
                    ],
                    warning_delay,
                )
                .await;
            }
        }
    }
}

/// Hidden service security level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiddenServiceSecurityLevel {
    /// Secure: Using C-Tor (reference implementation)
    Secure,
    /// Experimental: Using Arti (not yet as secure as C-Tor)
    Experimental,
    /// Disabled: No hidden service, clearnet only
    Disabled,
}

/// Display a hidden service warning with configurable countdown.
async fn display_hs_warning(message: &str, details: &[&str], warning_delay: u64) {
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

    if warning_delay > 0 {
        for i in (1..=warning_delay).rev() {
            eprint!("\rServer will start in {} seconds...  ", i);
            let _ = io::stderr().flush();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        eprintln!();
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

/// Global privacy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyConfig {
    /// Disable Tor entirely (direct connections).
    /// Set via `--direct` flag or `FOIACQUIRE_DIRECT=1`.
    #[serde(default)]
    pub direct: bool,

    /// Enable pluggable transport obfuscation (default: true).
    /// Set to false via `--no-obfuscation` or `FOIACQUIRE_NO_OBFUSCATION=1`.
    #[serde(default = "default_true")]
    pub obfuscation: bool,

    /// External SOCKS5 proxy URL (bypasses embedded Arti).
    /// Set via `SOCKS_PROXY` environment variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socks_proxy: Option<String>,

    /// Default transport when obfuscation is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<Transport>,

    /// Bridge configuration for obfs4.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bridges: Vec<String>,

    /// Delay in seconds before proceeding when insecure (default: 15).
    /// Set via `--privacy-warning-delay` flag.
    /// Warning is always shown; only the countdown can be skipped with 0.
    #[serde(
        default = "default_warning_delay",
        skip_serializing_if = "is_default_warning_delay"
    )]
    pub warning_delay: u64,

    /// Show Tor legality warning (default: true).
    /// Can be disabled via `--no-tor-warning` or `FOIACQUIRE_NO_TOR_WARNING=1`.
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub tor_legal_warning: bool,

    /// Hidden service configuration for server mode.
    #[serde(default, skip_serializing_if = "HiddenServiceConfig::is_default")]
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
}

impl PrivacyConfig {
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
        if let Ok(proxy) = env::var("SOCKS_PROXY") {
            if !proxy.is_empty() {
                self.socks_proxy = Some(proxy);
            }
        }

        // FOIACQUIRE_DIRECT=1 disables Tor
        if env::var("FOIACQUIRE_DIRECT")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            self.direct = true;
        }

        // FOIACQUIRE_NO_OBFUSCATION=1 disables PTs
        if env::var("FOIACQUIRE_NO_OBFUSCATION")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            self.obfuscation = false;
        }

        // FOIACQUIRE_NO_TOR_WARNING=1 disables Tor legality warning
        if env::var("FOIACQUIRE_NO_TOR_WARNING")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            self.tor_legal_warning = false;
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

    /// Display Tor legality warning if enabled and Tor is in use.
    pub fn show_tor_legal_warning(&self) {
        if !self.tor_legal_warning || !self.uses_tor() {
            return;
        }

        eprintln!();
        eprintln!("Note: Tor may be illegal or monitored in some jurisdictions.");
        eprintln!("      Know your local laws before proceeding.");
        eprintln!(
            "      Disable this warning with --no-tor-warning or FOIACQUIRE_NO_TOR_WARNING=1"
        );
        eprintln!();
    }

    /// Check if any insecure configuration is active.
    /// Returns the security level: Secure, NoObfuscation, or Direct.
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
    /// Set warning_delay to 0 to skip the countdown (warning still shown).
    pub async fn enforce_security_warning(&self) {
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
    async fn display_warning(&self, message: &str, details: &[&str]) {
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

        if self.warning_delay > 0 {
            for i in (1..=self.warning_delay).rev() {
                eprint!("\rYour command will execute in {} seconds...  ", i);
                let _ = io::stderr().flush();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            eprintln!();
        }
    }
}

/// Security level indicating how protected the user is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityLevel {
    /// Fully secure: Tor with obfuscation or external proxy.
    Secure,
    /// Tor enabled but not obfuscated (detectable as Tor traffic).
    NoObfuscation,
    /// Direct connection without Tor (fully exposed).
    Direct,
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

    #[test]
    fn test_hidden_service_default_is_ctor() {
        let config = HiddenServiceConfig::default();
        assert_eq!(config.provider, HiddenServiceProvider::CTor);
        assert!(!config.allow_potentially_insecure_circuits);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_hidden_service_arti_requires_opt_in() {
        let config = HiddenServiceConfig {
            provider: HiddenServiceProvider::Arti,
            allow_potentially_insecure_circuits: false,
            ..Default::default()
        };
        // Should fail validation without opt-in
        assert!(config.validate().is_err());
        let err = config.validate().unwrap_err();
        assert!(err.contains("allow_potentially_insecure_circuits"));
    }

    #[test]
    fn test_hidden_service_arti_with_opt_in() {
        let config = HiddenServiceConfig {
            provider: HiddenServiceProvider::Arti,
            allow_potentially_insecure_circuits: true,
            ..Default::default()
        };
        // Should pass validation with opt-in
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_hidden_service_none_always_valid() {
        let config = HiddenServiceConfig {
            provider: HiddenServiceProvider::None,
            allow_potentially_insecure_circuits: false,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_hidden_service_is_enabled() {
        let ctor = HiddenServiceConfig::default();
        assert!(ctor.is_enabled());

        let none = HiddenServiceConfig {
            provider: HiddenServiceProvider::None,
            ..Default::default()
        };
        assert!(!none.is_enabled());
    }
}
