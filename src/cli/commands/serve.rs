//! Web server command.

use console::style;

use crate::config::Settings;

/// Start the web server.
pub async fn cmd_serve(settings: &Settings, bind: &str) -> anyhow::Result<()> {
    let (host, port) = parse_bind_address(bind)?;

    // Run database migrations first
    println!("{} Running database migrations...", style("→").cyan(),);
    let ctx = settings.create_db_context();
    match ctx.init_schema().await {
        Ok(()) => {
            println!(
                "  {} Database ready",
                style("✓").green(),
            );
        }
        Err(e) => {
            eprintln!("  {} Migration failed: {}", style("✗").red(), e);
            return Err(anyhow::anyhow!("Database migration failed: {}", e));
        }
    }

    println!(
        "{} Starting FOIAcquire server at http://{}:{}",
        style("→").cyan(),
        host,
        port
    );
    println!("  Press Ctrl+C to stop");

    crate::server::serve(settings, &host, port).await
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
