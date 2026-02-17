//! foia - FOIA document acquisition and research system.
//!
//! Core library exposing domain modules for workspace crates.

// Model types use `from_str` methods that return Self (infallible parse),
// not Result<Self, Error> as std::str::FromStr requires.
#![allow(clippy::should_implement_trait)]

#[cfg(feature = "browser")]
pub mod browser;
pub mod config;
#[cfg(feature = "gis")]
pub mod gis_data;
pub mod http_client;
pub mod llm;
pub mod migrations;
pub mod models;
pub mod prefer_db;
pub mod privacy;
pub mod rate_limit;
pub mod repository;
pub mod schema;
pub mod services;
pub mod storage;
pub mod utils;
pub mod work_queue;
