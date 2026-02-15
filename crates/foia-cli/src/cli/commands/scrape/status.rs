//! Status command for showing system state.

use std::collections::HashMap;
use std::io::{stdout, Stdout};
use std::time::Duration;

use chrono::Local;
use console::style;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use foia::config::Settings;
use foia::models::{DocumentStatus, ServiceStatus};
use foia::repository::util::redact_url_password;

/// Show overall system status.
pub async fn cmd_status(
    settings: &Settings,
    url: Option<String>,
    source_id: Option<String>,
    live: bool,
    interval: u64,
    json: bool,
) -> anyhow::Result<()> {
    // If URL is provided (via --url or FOIA_API_URL), fetch from API
    if let Some(base_url) = url {
        return fetch_and_display_api_status(&base_url, source_id.as_deref(), json).await;
    }

    // Otherwise use local database
    if !settings.database_exists() {
        println!(
            "{} System not initialized. Run 'foia init' first.",
            style("!").yellow()
        );
        println!(
            "  Or set {} to connect to a remote server.",
            style("FOIA_API_URL").cyan()
        );
        return Ok(());
    }

    if json {
        return display_status_json(settings, source_id.as_deref()).await;
    }

    if live {
        run_live_status(settings, interval).await
    } else {
        display_status_simple(settings).await
    }
}

/// Fetch status from API and display it.
async fn fetch_and_display_api_status(
    base_url: &str,
    source_id: Option<&str>,
    json: bool,
) -> anyhow::Result<()> {
    // ALLOWED: Fetching from local foia API server (localhost)
    // Privacy/Tor routing is not applicable for local API calls
    #[allow(clippy::disallowed_methods)]
    let client = reqwest::Client::new();

    let url = if let Some(sid) = source_id {
        format!("{}/api/status/{}", base_url.trim_end_matches('/'), sid)
    } else {
        format!("{}/api/status", base_url.trim_end_matches('/'))
    };

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", url, e))?;

    if !response.status().is_success() {
        anyhow::bail!("Server returned {}: {}", response.status(), url);
    }

    let data: serde_json::Value = response
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to parse response: {}", e))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
    } else {
        display_api_status(&data, base_url);
    }

    Ok(())
}

/// Display API status response in human-readable format.
fn display_api_status(data: &serde_json::Value, url: &str) {
    let separator = "─".repeat(70);

    println!();
    println!("{:<50} {}", style("foia status").bold(), style(url).dim());
    println!("{}", separator);

    // Documents section
    if let Some(docs) = data.get("documents") {
        println!("{}", style("DOCUMENTS").cyan().bold());
        if let Some(total) = docs.get("total").and_then(|v| v.as_u64()) {
            println!("  {:<20} {:>10}", "Total:", format_number(total));
        }
        if let Some(needing_ocr) = docs.get("needing_ocr").and_then(|v| v.as_u64()) {
            println!(
                "  {:<20} {:>10}",
                "Needing OCR:",
                format_number(needing_ocr)
            );
        }
        if let Some(needing_sum) = docs.get("needing_summarization").and_then(|v| v.as_u64()) {
            println!(
                "  {:<20} {:>10}",
                "Needing Summary:",
                format_number(needing_sum)
            );
        }
        println!();
    }

    // Crawl section
    if let Some(crawl) = data.get("crawl") {
        println!("{}", style("CRAWL QUEUE").cyan().bold());
        if let Some(pending) = crawl.get("total_pending").and_then(|v| v.as_u64()) {
            println!("  {:<20} {:>10}", "Pending:", format_number(pending));
        }
        if let Some(failed) = crawl.get("total_failed").and_then(|v| v.as_u64()) {
            println!("  {:<20} {:>10}", "Failed:", format_number(failed));
        }
        if let Some(discovered) = crawl.get("total_discovered").and_then(|v| v.as_u64()) {
            println!("  {:<20} {:>10}", "Discovered:", format_number(discovered));
        }
        println!();

        // Sources
        if let Some(sources) = crawl.get("sources").and_then(|v| v.as_array()) {
            if !sources.is_empty() {
                println!(
                    "{:<26} {:>10} {:>10} {:>10}",
                    style("SOURCES").cyan().bold(),
                    "Pending",
                    "Fetched",
                    "Failed"
                );
                for source in sources {
                    let id = source
                        .get("source_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    let pending = source.get("pending").and_then(|v| v.as_u64()).unwrap_or(0);
                    let fetched = source.get("fetched").and_then(|v| v.as_u64()).unwrap_or(0);
                    let failed = source.get("failed").and_then(|v| v.as_u64()).unwrap_or(0);
                    println!(
                        "  {:<24} {:>10} {:>10} {:>10}",
                        truncate_string(id, 24),
                        format_number(pending),
                        format_number(fetched),
                        format_number(failed),
                    );
                }
                println!();
            }
        }
    }

    // Type stats
    if let Some(types) = data.get("type_stats").and_then(|v| v.as_array()) {
        if !types.is_empty() {
            println!("{}", style("FILE TYPES").cyan().bold());
            for t in types.iter().take(10) {
                let mime = t.get("mime_type").and_then(|v| v.as_str()).unwrap_or("?");
                let count = t.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                println!(
                    "  {:<40} {:>10}",
                    truncate_string(mime, 40),
                    format_number(count)
                );
            }
            println!();
        }
    }

    println!("{}", separator);
}

/// Display status as JSON from local database.
async fn display_status_json(settings: &Settings, _source_id: Option<&str>) -> anyhow::Result<()> {
    let data = fetch_status_data(settings).await?;

    let json = serde_json::json!({
        "documents": {
            "total": data.total_docs,
            "by_status": data.status_counts,
        },
        "crawl": {
            "pending_downloads": data.pending_downloads,
        },
        "sources": data.sources.iter().map(|s| serde_json::json!({
            "source_id": s.id,
            "total": s.total,
            "pending": s.pending,
            "downloaded": s.downloaded,
            "ocr_done": s.ocr_done,
        })).collect::<Vec<_>>(),
        "database": data.database_url,
        "data_dir": data.data_dir,
    });

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}

/// Status data collected from the database.
struct StatusData {
    database_url: String,
    data_dir: String,
    total_docs: u64,
    status_counts: HashMap<String, u64>,
    pending_downloads: u64,
    sources: Vec<SourceStats>,
    services: Vec<ServiceStatus>,
    last_updated: String,
}

struct SourceStats {
    id: String,
    total: u64,
    pending: u64,
    downloaded: u64,
    ocr_done: u64,
}

/// Fetch all status data from the database.
async fn fetch_status_data(settings: &Settings) -> anyhow::Result<StatusData> {
    let repos = settings.repositories()?;
    let doc_repo = repos.documents;
    let source_repo = repos.sources;
    let crawl_repo = repos.crawl;
    let service_repo = repos.service_status;

    let sources_list = source_repo.get_all().await?;
    let total_docs = doc_repo.count().await?;
    let status_counts = doc_repo.count_all_by_status().await?;
    let pending_downloads = crawl_repo.count_pending_downloads().await.unwrap_or(0) as u64;
    let source_counts = doc_repo.get_all_source_counts().await?;
    let source_status_counts = doc_repo.get_source_status_counts().await?;
    let services = service_repo.get_all().await.unwrap_or_default();

    // Only include sources that have at least one document
    let sources: Vec<SourceStats> = sources_list
        .iter()
        .filter_map(|source| {
            let total = source_counts.get(&source.id).copied().unwrap_or(0);
            if total == 0 {
                return None;
            }
            let statuses = source_status_counts.get(&source.id);
            Some(SourceStats {
                id: source.id.clone(),
                total,
                pending: statuses
                    .and_then(|s| s.get(DocumentStatus::Pending.as_str()))
                    .copied()
                    .unwrap_or(0),
                downloaded: statuses
                    .and_then(|s| s.get(DocumentStatus::Downloaded.as_str()))
                    .copied()
                    .unwrap_or(0),
                ocr_done: statuses
                    .and_then(|s| s.get(DocumentStatus::OcrComplete.as_str()))
                    .copied()
                    .unwrap_or(0),
            })
        })
        .collect();

    Ok(StatusData {
        database_url: redact_url_password(&settings.database_url()),
        data_dir: settings.data_dir.display().to_string(),
        total_docs,
        status_counts,
        pending_downloads,
        sources,
        services,
        last_updated: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    })
}

/// Display status once (non-TUI mode).
async fn display_status_simple(settings: &Settings) -> anyhow::Result<()> {
    let data = fetch_status_data(settings).await?;
    let separator = "─".repeat(70);

    println!();
    println!(
        "{:<50} Last updated: {}",
        style("foia status").bold(),
        data.last_updated
    );
    println!("{}", separator);

    println!("Database: {}", data.database_url);
    println!("Data Dir: {}", data.data_dir);
    println!();

    println!("{}", style("DOCUMENTS").cyan().bold());
    println!("  {:<20} {:>10}", "Total:", format_number(data.total_docs));

    for status in [
        DocumentStatus::Pending,
        DocumentStatus::Downloaded,
        DocumentStatus::OcrComplete,
        DocumentStatus::Indexed,
        DocumentStatus::Failed,
    ] {
        if let Some(&count) = data.status_counts.get(status.as_str()) {
            println!(
                "  {:<20} {:>10}",
                format!("{}:", status.as_str()),
                format_number(count)
            );
        }
    }
    println!();

    println!("{}", style("QUEUES").cyan().bold());
    println!(
        "  {:<20} {:>10} pending",
        "Download queue:",
        format_number(data.pending_downloads)
    );

    let ocr_pending = data
        .status_counts
        .get(DocumentStatus::Downloaded.as_str())
        .copied()
        .unwrap_or(0);
    println!(
        "  {:<20} {:>10} awaiting",
        "OCR queue:",
        format_number(ocr_pending)
    );
    println!();

    // Show running services
    let active_services: Vec<_> = data
        .services
        .iter()
        .filter(|s| s.status != foia::models::ServiceState::Stopped)
        .collect();

    if !active_services.is_empty() {
        println!("{}", style("SERVICES").cyan().bold());
        for svc in &active_services {
            let status_style = match svc.status {
                foia::models::ServiceState::Running => style(svc.status.as_str()).green(),
                foia::models::ServiceState::Starting => style(svc.status.as_str()).yellow(),
                foia::models::ServiceState::Error => style(svc.status.as_str()).red(),
                foia::models::ServiceState::Idle => style(svc.status.as_str()).dim(),
                _ => style(svc.status.as_str()),
            };
            let task = svc.current_task.as_deref().unwrap_or("-");
            let age = chrono::Utc::now() - svc.last_heartbeat;
            let age_str = if age.num_seconds() < 60 {
                format!("{}s ago", age.num_seconds())
            } else {
                format!("{}m ago", age.num_minutes())
            };
            println!(
                "  {:<24} {} {:>8} {}",
                truncate_string(&svc.id, 24),
                status_style,
                age_str,
                truncate_string(task, 30)
            );
        }
        println!();
    }

    if !data.sources.is_empty() {
        println!(
            "{:<26} {:>10} {:>10} {:>10} {:>10}",
            style("SOURCES").cyan().bold(),
            "Total",
            "Pending",
            "Downloaded",
            "OCR Done"
        );

        for source in &data.sources {
            println!(
                "  {:<24} {:>10} {:>10} {:>10} {:>10}",
                truncate_string(&source.id, 24),
                format_number(source.total),
                format_number(source.pending),
                format_number(source.downloaded),
                format_number(source.ocr_done),
            );
        }
    }

    println!("{}", separator);

    Ok(())
}

/// Run status display in live TUI mode.
async fn run_live_status(settings: &Settings, interval: u64) -> anyhow::Result<()> {
    // Fetch initial data before entering TUI mode
    let mut data = fetch_status_data(settings).await?;

    // Setup terminal
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let result = run_tui_loop(&mut terminal, settings, &mut data, interval).await;

    // Restore terminal
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;

    result
}

/// Main TUI event loop.
async fn run_tui_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    settings: &Settings,
    data: &mut StatusData,
    interval: u64,
) -> anyhow::Result<()> {
    let refresh_duration = Duration::from_secs(interval);
    let poll_duration = Duration::from_millis(100);

    loop {
        // Draw current state
        terminal.draw(|frame| draw_status(frame, data))?;

        // Check for keyboard input (non-blocking with short timeout)
        let deadline = tokio::time::Instant::now() + refresh_duration;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            let poll_time = remaining.min(poll_duration);

            if event::poll(poll_time)? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                            KeyCode::Char('c')
                                if key.modifiers.contains(event::KeyModifiers::CONTROL) =>
                            {
                                return Ok(())
                            }
                            KeyCode::Char('r') => {
                                // Force refresh
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        // Fetch new data (keep old data visible during fetch)
        if let Ok(new_data) = fetch_status_data(settings).await {
            *data = new_data;
        }
    }
}

/// Draw the status TUI.
fn draw_status(frame: &mut Frame, data: &StatusData) {
    let area = frame.area();

    // Filter active services
    let active_services: Vec<_> = data
        .services
        .iter()
        .filter(|s| s.status != foia::models::ServiceState::Stopped)
        .collect();
    let services_height = if active_services.is_empty() {
        0
    } else {
        (active_services.len() + 2).min(8) as u16
    };

    // Create main layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),               // Header
            Constraint::Length(3),               // Database info
            Constraint::Length(9),               // Documents section
            Constraint::Length(5),               // Queues section
            Constraint::Length(services_height), // Services section
            Constraint::Min(5),                  // Sources table
            Constraint::Length(1),               // Footer
        ])
        .split(area);

    // Header
    let header = Paragraph::new(format!(
        "foia status                                          Last updated: {}",
        data.last_updated
    ))
    .style(Style::default().bold())
    .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, chunks[0]);

    // Database info
    let db_info = Paragraph::new(format!(
        "Database: {}\nData Dir: {}",
        data.database_url, data.data_dir
    ))
    .block(Block::default());
    frame.render_widget(db_info, chunks[1]);

    // Documents section
    let ocr_pending = data
        .status_counts
        .get(DocumentStatus::Downloaded.as_str())
        .copied()
        .unwrap_or(0);

    let docs_text = format!(
        "  Total:        {:>12}\n  Pending:      {:>12}\n  Downloaded:   {:>12}\n  OCR Complete: {:>12}\n  Indexed:      {:>12}\n  Failed:       {:>12}",
        format_number(data.total_docs),
        format_number(data.status_counts.get(DocumentStatus::Pending.as_str()).copied().unwrap_or(0)),
        format_number(data.status_counts.get(DocumentStatus::Downloaded.as_str()).copied().unwrap_or(0)),
        format_number(data.status_counts.get(DocumentStatus::OcrComplete.as_str()).copied().unwrap_or(0)),
        format_number(data.status_counts.get(DocumentStatus::Indexed.as_str()).copied().unwrap_or(0)),
        format_number(data.status_counts.get(DocumentStatus::Failed.as_str()).copied().unwrap_or(0)),
    );
    let docs = Paragraph::new(docs_text).block(
        Block::default()
            .title(" DOCUMENTS ")
            .title_style(Style::default().fg(Color::Cyan).bold())
            .borders(Borders::TOP),
    );
    frame.render_widget(docs, chunks[2]);

    // Queues section
    let queues_text = format!(
        "  Download queue: {:>10} pending\n  OCR queue:      {:>10} awaiting",
        format_number(data.pending_downloads),
        format_number(ocr_pending),
    );
    let queues = Paragraph::new(queues_text).block(
        Block::default()
            .title(" QUEUES ")
            .title_style(Style::default().fg(Color::Cyan).bold())
            .borders(Borders::TOP),
    );
    frame.render_widget(queues, chunks[3]);

    // Services section
    if !active_services.is_empty() {
        let header_cells = ["Service", "Status", "Heartbeat", "Task"]
            .iter()
            .map(|h| Cell::from(*h).style(Style::default().bold()));
        let header = Row::new(header_cells).height(1);

        let rows = active_services.iter().map(|svc| {
            let status_style = match svc.status {
                foia::models::ServiceState::Running => Style::default().fg(Color::Green),
                foia::models::ServiceState::Starting => Style::default().fg(Color::Yellow),
                foia::models::ServiceState::Error => Style::default().fg(Color::Red),
                foia::models::ServiceState::Idle => Style::default().fg(Color::DarkGray),
                _ => Style::default(),
            };
            let age = chrono::Utc::now() - svc.last_heartbeat;
            let age_str = if age.num_seconds() < 60 {
                format!("{}s ago", age.num_seconds())
            } else {
                format!("{}m ago", age.num_minutes())
            };
            let task = svc.current_task.as_deref().unwrap_or("-");
            Row::new([
                Cell::from(truncate_string(&svc.id, 24)),
                Cell::from(svc.status.as_str()).style(status_style),
                Cell::from(age_str),
                Cell::from(truncate_string(task, 30)),
            ])
        });

        let table = Table::new(
            rows,
            [
                Constraint::Min(26),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Min(20),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .title(" SERVICES ")
                .title_style(Style::default().fg(Color::Cyan).bold())
                .borders(Borders::TOP),
        );
        frame.render_widget(table, chunks[4]);
    }

    // Sources table
    if !data.sources.is_empty() {
        let header_cells = ["Source", "Total", "Pending", "Downloaded", "OCR Done"]
            .iter()
            .map(|h| Cell::from(*h).style(Style::default().bold()));
        let header = Row::new(header_cells).height(1);

        let rows = data.sources.iter().map(|s| {
            Row::new([
                Cell::from(truncate_string(&s.id, 24)),
                Cell::from(format_number(s.total)).style(Style::default().fg(Color::White)),
                Cell::from(format_number(s.pending)).style(if s.pending > 0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                }),
                Cell::from(format_number(s.downloaded)).style(if s.downloaded > 0 {
                    Style::default().fg(Color::Blue)
                } else {
                    Style::default()
                }),
                Cell::from(format_number(s.ocr_done)).style(if s.ocr_done > 0 {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default()
                }),
            ])
        });

        let table = Table::new(
            rows,
            [
                Constraint::Min(26),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .title(" SOURCES ")
                .title_style(Style::default().fg(Color::Cyan).bold())
                .borders(Borders::TOP),
        );
        frame.render_widget(table, chunks[5]);
    }

    // Footer
    let footer = Paragraph::new("Press 'q' to quit, 'r' to refresh")
        .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, chunks[6]);
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
