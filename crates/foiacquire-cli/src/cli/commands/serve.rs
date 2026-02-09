//! Web server command.

use std::net::SocketAddr;

use console::style;

use foiacquire::config::{Config, Settings};
use foiacquire::privacy::{CTorHiddenService, HiddenServiceProvider};
use foiacquire::repository::migrations;

/// Start the web server.
pub async fn cmd_serve(
    settings: &Settings,
    config: &Config,
    bind: &str,
    no_migrate: bool,
    no_hidden_service: bool,
    use_arti: bool,
) -> anyhow::Result<()> {
    let (host, port) = parse_bind_address(bind)?;

    let ctx = settings.create_db_context()?;

    if no_migrate {
        // Check schema version but don't migrate
        match ctx.get_schema_version().await {
            Ok(Some(version)) => {
                println!(
                    "  {} Database schema version: {}",
                    style("→").cyan(),
                    version
                );
            }
            Ok(None) => {
                eprintln!(
                    "{} Database not initialized. Run 'foiacquire db migrate' first.",
                    style("!").yellow()
                );
                return Err(anyhow::anyhow!("Database not initialized"));
            }
            Err(e) => {
                eprintln!("  {} Failed to check schema: {}", style("!").yellow(), e);
            }
        }
    } else {
        // Run database migrations
        println!("{} Running database migrations...", style("→").cyan(),);
        match migrations::run_migrations(&settings.database_url(), settings.no_tls).await {
            Ok(()) => {
                println!("  {} Database ready", style("✓").green(),);
            }
            Err(e) => {
                eprintln!("  {} Migration failed: {}", style("✗").red(), e);
                return Err(anyhow::anyhow!("Database migration failed: {}", e));
            }
        }
    }

    // Determine hidden service configuration
    let mut hs_config = config.privacy.hidden_service.clone();

    // CLI flags and direct mode override config
    if no_hidden_service || config.privacy.direct {
        hs_config.provider = HiddenServiceProvider::None;
    } else if use_arti {
        hs_config.provider = HiddenServiceProvider::Arti;
    }

    // Validate configuration
    if let Err(e) = hs_config.validate() {
        eprintln!("{} {}", style("!").red(), e);
        return Err(anyhow::anyhow!("Invalid hidden service configuration"));
    }

    // Show security warning for non-secure configurations
    hs_config
        .enforce_security_warning(config.privacy.warning_delay)
        .await;

    // Start server based on hidden service configuration
    if !hs_config.is_enabled() {
        // Clearnet only
        println!(
            "{} Starting FOIAcquire server at http://{}:{}",
            style("→").cyan(),
            host,
            port
        );
        println!("  Press Ctrl+C to stop");
        return foiacquire::server::serve(settings, &host, port).await;
    }

    match hs_config.provider {
        HiddenServiceProvider::CTor => {
            start_with_ctor(settings, config, &hs_config, &host, port).await
        }
        HiddenServiceProvider::Arti => {
            start_with_arti(settings, config, &hs_config, &host, port).await
        }
        HiddenServiceProvider::None => {
            unreachable!("already handled by is_enabled() check")
        }
    }
}

/// Start server with C-Tor hidden service.
async fn start_with_ctor(
    settings: &Settings,
    _config: &Config,
    hs_config: &foiacquire::privacy::HiddenServiceConfig,
    host: &str,
    port: u16,
) -> anyhow::Result<()> {
    // Check if C-Tor is available
    if !CTorHiddenService::is_available(hs_config) {
        eprintln!(
            "{} Tor not found. Install tor or set tor_binary in config.",
            style("!").red()
        );
        eprintln!("  On Debian/Ubuntu: sudo apt install tor");
        eprintln!("  On macOS: brew install tor");
        eprintln!("  On Windows: Download from https://www.torproject.org/");
        eprintln!();
        eprintln!("  To run without hidden service (clearnet only), use --no-hidden-service");
        return Err(anyhow::anyhow!("Tor not available"));
    }

    println!(
        "{} Starting FOIAcquire with Tor hidden service...",
        style("→").cyan()
    );

    // Parse local address
    let local_addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    // Start C-Tor hidden service
    let mut hs = CTorHiddenService::start(hs_config, &settings.data_dir, local_addr).await?;

    // Wait for hidden service to be ready
    if !hs.is_ready().await {
        return Err(anyhow::anyhow!("Failed to start hidden service"));
    }

    // Get the onion URL
    let onion_url = hs
        .onion_url()
        .await
        .ok_or_else(|| anyhow::anyhow!("Failed to get onion address"))?;

    let onion_addr = hs.onion_address().await.unwrap_or_default();

    println!();
    println!("  {} Hidden service ready!", style("✓").green().bold());
    println!();
    println!(
        "  {} {}",
        style("Onion address:").cyan().bold(),
        style(&onion_addr).yellow().bold()
    );
    println!(
        "  {} {}",
        style("Full URL:").cyan(),
        style(&onion_url).yellow()
    );
    println!();
    println!(
        "  {} http://{}:{} (local only)",
        style("Clearnet:").dim(),
        host,
        port
    );
    println!("  {} {}", style("Tor SOCKS:").dim(), hs.socks_url());
    println!("  {} {}", style("Data dir:").dim(), hs.hs_dir().display());
    println!();
    println!("  Press Ctrl+C to stop");
    println!();

    // Start the actual server
    let result = foiacquire::server::serve(settings, host, port).await;

    // Shutdown hidden service when server stops
    hs.shutdown();

    result
}

/// Start server with Arti hidden service (experimental).
async fn start_with_arti(
    settings: &Settings,
    _config: &Config,
    _hs_config: &foiacquire::privacy::HiddenServiceConfig,
    host: &str,
    port: u16,
) -> anyhow::Result<()> {
    // Arti hidden service support is experimental and not yet implemented
    // For now, we just show a message and fall back to clearnet
    eprintln!(
        "{} Arti hidden service support is not yet implemented.",
        style("!").yellow()
    );
    eprintln!("  Arti's onion service API is still in development.");
    eprintln!("  For production use, please use C-Tor (the default).");
    eprintln!();
    eprintln!("  Falling back to clearnet server...");
    eprintln!();

    println!(
        "{} Starting FOIAcquire server at http://{}:{}",
        style("→").cyan(),
        host,
        port
    );
    println!("  Press Ctrl+C to stop");
    foiacquire::server::serve(settings, host, port).await
}

/// Parse a bind address that can be:
/// - Just a port: "3030" -> 127.0.0.1:3030
/// - Just a host: "0.0.0.0" -> 0.0.0.0:3030
/// - Host and port: "0.0.0.0:3030" -> 0.0.0.0:3030
fn parse_bind_address(bind: &str) -> anyhow::Result<(String, u16)> {
    // Try parsing as just a port number
    if let Ok(port) = bind.parse::<u16>() {
        return Ok(("127.0.0.1".to_string(), port));
    }

    // Try parsing as host:port
    if let Some((host, port_str)) = bind.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            return Ok((host.to_string(), port));
        }
    }

    // Must be just a host, use default port
    Ok((bind.to_string(), 3030))
}
