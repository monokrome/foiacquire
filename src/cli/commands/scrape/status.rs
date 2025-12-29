//! Status command for showing system state.

use std::io::{stdout, Write};

use chrono::Local;
use console::style;
use crossterm::{cursor, execute, terminal};

use crate::config::Settings;
use crate::models::DocumentStatus;
use crate::repository::util::redact_url_password;

/// Show overall system status.
pub async fn cmd_status(settings: &Settings, live: bool, interval: u64) -> anyhow::Result<()> {
    if !settings.database_exists() {
        println!(
            "{} System not initialized. Run 'foiacquire init' first.",
            style("!").yellow()
        );
        return Ok(());
    }

    if live {
        run_live_status(settings, interval).await
    } else {
        display_status(settings).await
    }
}

/// Display status once.
async fn display_status(settings: &Settings) -> anyhow::Result<()> {
    let ctx = settings.create_db_context()?;
    let doc_repo = ctx.documents();
    let source_repo = ctx.sources();
    let crawl_repo = ctx.crawl();

    let sources = source_repo.get_all().await?;
    let total_docs = doc_repo.count().await?;
    let status_counts = doc_repo.count_all_by_status().await?;

    // Get queue counts
    let pending_downloads = crawl_repo.count_pending_downloads().await.unwrap_or(0) as u64;

    let now = Local::now();
    let separator = "─".repeat(70);

    println!();
    println!(
        "{:<50} Last updated: {}",
        style("foiacquire status").bold(),
        now.format("%Y-%m-%d %H:%M:%S")
    );
    println!("{}", separator);

    // Database info
    println!(
        "Database: {}",
        redact_url_password(&settings.database_url())
    );
    println!("Data Dir: {}", settings.data_dir.display());
    println!();

    // Document counts
    println!("{}", style("DOCUMENTS").cyan().bold());
    println!("  {:<20} {:>10}", "Total:", format_number(total_docs));

    for status in [
        DocumentStatus::Pending,
        DocumentStatus::Downloaded,
        DocumentStatus::OcrComplete,
        DocumentStatus::Indexed,
        DocumentStatus::Failed,
    ] {
        if let Some(&count) = status_counts.get(status.as_str()) {
            println!(
                "  {:<20} {:>10}",
                format!("{}:", status.as_str()),
                format_number(count)
            );
        }
    }
    println!();

    // Queue info
    println!("{}", style("QUEUES").cyan().bold());
    println!(
        "  {:<20} {:>10} pending",
        "Download queue:",
        format_number(pending_downloads)
    );

    let ocr_pending = status_counts
        .get(DocumentStatus::Downloaded.as_str())
        .copied()
        .unwrap_or(0);
    println!(
        "  {:<20} {:>10} awaiting",
        "OCR queue:",
        format_number(ocr_pending)
    );
    println!();

    // Per-source breakdown
    if !sources.is_empty() {
        println!(
            "{:<26} {:>10} {:>10} {:>10} {:>10}",
            style("SOURCES").cyan().bold(),
            "Total",
            "Pending",
            "Downloaded",
            "OCR Done"
        );

        let source_counts = doc_repo.get_all_source_counts().await?;
        let source_status_counts = doc_repo.get_source_status_counts().await?;

        for source in &sources {
            let total = source_counts.get(&source.id).copied().unwrap_or(0);
            let statuses = source_status_counts.get(&source.id);

            let pending = statuses
                .and_then(|s| s.get(DocumentStatus::Pending.as_str()))
                .copied()
                .unwrap_or(0);
            let downloaded = statuses
                .and_then(|s| s.get(DocumentStatus::Downloaded.as_str()))
                .copied()
                .unwrap_or(0);
            let ocr_done = statuses
                .and_then(|s| s.get(DocumentStatus::OcrComplete.as_str()))
                .copied()
                .unwrap_or(0);

            println!(
                "  {:<24} {:>10} {:>10} {:>10} {:>10}",
                truncate_string(&source.id, 24),
                format_number(total),
                format_number(pending),
                format_number(downloaded),
                format_number(ocr_done),
            );
        }
    }

    println!("{}", separator);

    Ok(())
}

/// Run status display in live mode with periodic refresh.
async fn run_live_status(settings: &Settings, interval: u64) -> anyhow::Result<()> {
    let mut stdout = stdout();

    // Setup terminal
    execute!(stdout, terminal::Clear(terminal::ClearType::All))?;

    println!("Press Ctrl+C to exit\n");

    loop {
        // Move cursor to top
        execute!(stdout, cursor::MoveTo(0, 1))?;

        // Clear from cursor to end of screen
        execute!(stdout, terminal::Clear(terminal::ClearType::FromCursorDown))?;

        // Display status
        if let Err(e) = display_status(settings).await {
            eprintln!("{} Error: {}", style("✗").red(), e);
        }

        println!("\nPress Ctrl+C to exit");
        stdout.flush()?;

        // Wait for interval
        tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
    }
}

/// Format a number with thousand separators.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let bytes: Vec<_> = s.bytes().rev().collect();
    let chunks: Vec<_> = bytes
        .chunks(3)
        .map(|chunk| chunk.iter().rev().map(|&b| b as char).collect::<String>())
        .collect();
    chunks.into_iter().rev().collect::<Vec<_>>().join(",")
}

/// Truncate a string to max length with ellipsis.
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
