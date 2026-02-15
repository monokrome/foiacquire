//! Hidden service (onion service) configuration.

use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;

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

impl prefer::FromValue for HiddenServiceProvider {
    fn from_value(value: &prefer::ConfigValue) -> prefer::Result<Self> {
        match value.as_str() {
            Some(s) => match s.to_lowercase().as_str() {
                "c-tor" | "ctor" | "tor" => Ok(HiddenServiceProvider::CTor),
                "arti" => Ok(HiddenServiceProvider::Arti),
                "none" | "disabled" => Ok(HiddenServiceProvider::None),
                other => Err(prefer::Error::ConversionError {
                    key: String::new(),
                    type_name: "HiddenServiceProvider".to_string(),
                    source: format!("unknown provider: {}", other).into(),
                }),
            },
            None => Err(prefer::Error::ConversionError {
                key: String::new(),
                type_name: "HiddenServiceProvider".to_string(),
                source: "expected string".into(),
            }),
        }
    }
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
#[derive(Debug, Clone, Serialize, Deserialize, prefer::FromValue)]
pub struct HiddenServiceConfig {
    /// Hidden service provider (c-tor, arti, or none).
    /// Default: c-tor (most secure, recommended by Tor Project)
    #[serde(default)]
    #[prefer(default)]
    pub provider: HiddenServiceProvider,

    /// Allow potentially insecure experimental circuits.
    /// Required to use Arti for hidden services (Arti warns their onion
    /// service implementation is "not yet as secure as C-Tor").
    /// Default: false (safe default)
    #[serde(default)]
    #[prefer(default)]
    pub allow_potentially_insecure_circuits: bool,

    /// Path to tor binary (default: search PATH for "tor")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
    pub tor_binary: Option<PathBuf>,

    /// Directory for Tor data (default: data_dir/tor)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
    pub tor_data_dir: Option<PathBuf>,

    /// Hidden service port (what the onion service advertises)
    /// Default: 80
    #[serde(default = "default_hidden_service_port")]
    #[prefer(default)]
    pub hidden_service_port: u16,

    /// Also listen on clearnet (in addition to hidden service)
    /// Default: false (hidden service only for maximum privacy)
    #[serde(default)]
    #[prefer(default)]
    pub also_listen_clearnet: bool,

    /// Clearnet bind address (only used if also_listen_clearnet is true)
    /// Default: 127.0.0.1:3030
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[prefer(default)]
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
        // FOIA_HS_PROVIDER - hidden service provider
        if let Ok(provider) = env::var("FOIA_HS_PROVIDER") {
            match provider.to_lowercase().as_str() {
                "c-tor" | "ctor" | "tor" => self.provider = HiddenServiceProvider::CTor,
                "arti" => self.provider = HiddenServiceProvider::Arti,
                "none" | "disabled" => self.provider = HiddenServiceProvider::None,
                _ => {}
            }
        }

        // FOIA_ALLOW_INSECURE_CIRCUITS=1 - allow experimental Arti onion services
        if env::var("FOIA_ALLOW_INSECURE_CIRCUITS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            self.allow_potentially_insecure_circuits = true;
        }

        // FOIA_TOR_BINARY - path to tor binary
        if let Ok(path) = env::var("FOIA_TOR_BINARY") {
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
                     - Or set FOIA_ALLOW_INSECURE_CIRCUITS=1 environment variable\n\n\
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
    super::config::display_security_warning(message, details, warning_delay).await;
}

#[cfg(test)]
mod tests {
    use super::*;

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
