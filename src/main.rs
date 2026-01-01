//! FOIAcquire - FOIA document acquisition and research system.
//!
//! A tool for acquiring, storing, and researching Freedom of Information Act
//! documents from various government sources.

mod analysis;
mod cli;
mod config;
mod discovery;
mod llm;
mod models;
mod ocr;
mod repository;
mod schema;
mod scrapers;
mod server;
mod services;
mod utils;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if present (before anything else)
    let _ = dotenvy::dotenv();

    // Initialize logging based on verbosity
    let default_filter = if cli::is_verbose() {
        "foiacquire=info"
    } else {
        "foiacquire=warn"
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Run CLI
    cli::run().await
}
