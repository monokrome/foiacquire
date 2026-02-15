//! Helper utilities for CLI commands.

/// Truncate a string to a maximum length, adding "..." if truncated.
pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max - 3])
    }
}

/// Format bytes as human-readable size.
pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.2} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Convert MIME type to short form for display.
pub fn mime_short(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "pdf",
        m if m.starts_with("image/") => "image",
        m if m.contains("word") => "doc",
        m if m.contains("excel") || m.contains("spreadsheet") => "xls",
        "text/html" => "html",
        "text/plain" => "txt",
        _ => "other",
    }
}
