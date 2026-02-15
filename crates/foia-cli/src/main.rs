//! foia - FOIA document acquisition and research system.
//!
//! A tool for acquiring, storing, and researching Freedom of Information Act
//! documents from various government sources.

mod cli;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if present (before anything else)
    let _ = dotenvy::dotenv();

    // Initialize logging based on verbosity
    let default_filter = if cli::is_verbose() {
        "foia=info"
    } else {
        "foia=warn"
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
