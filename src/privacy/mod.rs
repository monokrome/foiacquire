//! Privacy subsystem for routing requests through Tor or SOCKS proxies.
//!
//! This module provides privacy-by-default networking for investigative journalism.
//! All requests are routed through Tor with pluggable transports by default.
//!
//! # Privacy Modes
//!
//! - **Default**: Tor + obfuscation (obfs4/snowflake) - blocks until PT ready
//! - **No obfuscation**: Direct Tor (detectable as Tor traffic)
//! - **Direct**: No Tor (security warning printed)
//! - **External proxy**: User-provided SOCKS5 proxy
//!
//! # Configuration
//!
//! Global settings via environment or CLI:
//! - `SOCKS_PROXY=socks5://...` - Use external proxy instead of embedded Arti
//! - `FOIACQUIRE_DIRECT=1` - Disable Tor entirely
//! - `FOIACQUIRE_NO_OBFUSCATION=1` - Use Tor without pluggable transports
//! - `--direct` / `-D` - CLI flag to disable Tor
//! - `--no-obfuscation` - CLI flag to skip PTs
//!
//! Per-source settings in scraper config:
//! ```json
//! {
//!   "privacy": {
//!     "direct": true,           // Skip Tor for this source
//!     "obfuscation": false,     // Use direct Tor (no PT)
//!     "transport": "obfs4",     // Force specific transport
//!     "isolate": true           // Dedicated circuit
//!   }
//! }
//! ```

mod config;
mod ctor;

#[cfg(feature = "embedded-tor")]
mod arti;

#[allow(unused_imports)] // HiddenServiceSecurityLevel is public API
pub use config::{
    HiddenServiceConfig, HiddenServiceProvider, HiddenServiceSecurityLevel, PrivacyConfig,
    PrivacyMode, SourcePrivacyConfig,
};
pub use ctor::CTorHiddenService;

#[cfg(feature = "embedded-tor")]
#[allow(unused_imports)] // Public API for embedded Tor integration
pub use arti::{
    get_arti_socks_url, get_or_init_arti, is_arti_ready, ArtiBootstrapConfig, ArtiClient,
};
