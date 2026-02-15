//! Service layer for foia business logic.
//!
//! This module contains domain logic separated from UI concerns.
//! Services can be used by CLI, web server, or other interfaces.

#[cfg(feature = "gis")]
pub mod geolookup;
