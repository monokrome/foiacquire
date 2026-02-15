//! Terminal UI with locked status region.
//!
//! Provides a simple TUI that locks status/progress bars at the top of the terminal
//! while allowing other output to scroll below. Only activates in interactive terminals.

#![allow(dead_code)]

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Mutex;

use console::Term;
use crossterm::{
    cursor::{self, MoveTo},
    execute,
    terminal::{self, ClearType, ScrollUp},
};

/// Global state for the TUI.
static TUI_ACTIVE: AtomicBool = AtomicBool::new(false);
static STATUS_LINES: AtomicU16 = AtomicU16::new(0);
static STATUS_CONTENT: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Check if TUI mode is currently active.
pub fn is_active() -> bool {
    TUI_ACTIVE.load(Ordering::SeqCst)
}

/// Set scroll region using ANSI escape sequence (DECSTBM).
/// Top and bottom are 1-indexed.
fn set_scroll_region(top: u16, bottom: u16) -> io::Result<()> {
    let mut stdout = io::stdout();
    // DECSTBM: ESC [ top ; bottom r
    write!(stdout, "\x1b[{};{}r", top, bottom)?;
    stdout.flush()
}

/// Initialize the TUI with a fixed number of status lines at the top.
///
/// # Arguments
/// * `num_status_lines` - Number of lines to reserve at the top for status
///
/// Returns Ok(true) if TUI was activated, Ok(false) if not in interactive terminal.
pub fn init(num_status_lines: u16) -> io::Result<bool> {
    // Check environment variable to force TUI off (useful for containers)
    if std::env::var("NO_TUI").is_ok() || std::env::var("FOIA_NO_TUI").is_ok() {
        return Ok(false);
    }

    // Only activate in interactive terminals
    // Use try/catch pattern since Term::stdout() can fail in Docker without TTY
    let is_terminal = std::panic::catch_unwind(|| {
        let term = Term::stdout();
        term.is_term()
    })
    .unwrap_or(false);

    if !is_terminal {
        return Ok(false);
    }

    // Double-check with crossterm's detection (more reliable in some environments)
    if !crossterm::tty::IsTty::is_tty(&io::stdout()) {
        return Ok(false);
    }

    // terminal::size() can also fail without a TTY
    let (_, height) = match terminal::size() {
        Ok(size) => size,
        Err(_) => return Ok(false),
    };

    // Need at least status lines + some scroll area
    if height < num_status_lines + 5 {
        return Ok(false);
    }

    STATUS_LINES.store(num_status_lines, Ordering::SeqCst);

    // Initialize status content
    {
        let mut content = STATUS_CONTENT.lock().unwrap();
        content.clear();
        for _ in 0..num_status_lines {
            content.push(String::new());
        }
    }

    let mut stdout = io::stdout();

    // Try to set up TUI - if any operation fails, fall back to non-TUI mode
    // This prevents cryptic errors in container environments that claim to have a TTY but don't
    let setup_result = (|| -> io::Result<()> {
        // Clear the entire screen first to remove any previous output (e.g., cargo warnings)
        execute!(stdout, terminal::Clear(ClearType::All), MoveTo(0, 0))?;

        // Clear the status area lines
        for _ in 0..num_status_lines {
            execute!(stdout, terminal::Clear(ClearType::CurrentLine))?;
            println!();
        }

        // Set scroll region to exclude top status lines (1-indexed)
        set_scroll_region(num_status_lines + 1, height)?;

        // Move cursor to start of scroll region
        execute!(stdout, MoveTo(0, num_status_lines))?;

        stdout.flush()?;
        Ok(())
    })();

    if let Err(e) = setup_result {
        // TUI setup failed - fall back to non-TUI mode silently
        tracing::debug!("TUI setup failed, falling back to plain output: {}", e);
        STATUS_LINES.store(0, Ordering::SeqCst);
        return Ok(false);
    }

    TUI_ACTIVE.store(true, Ordering::SeqCst);

    Ok(true)
}

/// Cleanup the TUI, restoring normal terminal operation.
pub fn cleanup() -> io::Result<()> {
    if !TUI_ACTIVE.load(Ordering::SeqCst) {
        return Ok(());
    }

    TUI_ACTIVE.store(false, Ordering::SeqCst);

    // Best-effort cleanup - don't fail if terminal ops fail
    let height = match terminal::size() {
        Ok((_, h)) => h,
        Err(_) => return Ok(()),
    };

    let mut stdout = io::stdout();

    // Reset scroll region to full terminal
    let _ = set_scroll_region(1, height);

    // Move to bottom
    let _ = execute!(stdout, MoveTo(0, height - 1));
    let _ = stdout.flush();

    Ok(())
}

/// Update a status line (0-indexed from top).
pub fn set_status(line: u16, content: &str) -> io::Result<()> {
    let num_lines = STATUS_LINES.load(Ordering::SeqCst);

    if line >= num_lines {
        return Ok(());
    }

    // Update cached content
    {
        let mut status = STATUS_CONTENT.lock().unwrap();
        if (line as usize) < status.len() {
            status[line as usize] = content.to_string();
        }
    }

    if !TUI_ACTIVE.load(Ordering::SeqCst) {
        // Not in TUI mode, just print
        println!("{}", content);
        return Ok(());
    }

    let mut stdout = io::stdout();

    // Save cursor position
    execute!(stdout, cursor::SavePosition)?;

    // Move to status line and update
    execute!(stdout, MoveTo(0, line))?;
    execute!(stdout, terminal::Clear(ClearType::CurrentLine))?;
    print!("{}", content);

    // Restore cursor position
    execute!(stdout, cursor::RestorePosition)?;

    stdout.flush()?;

    Ok(())
}

/// Clear a status line.
pub fn clear_status(line: u16) -> io::Result<()> {
    set_status(line, "")
}

/// Print a log message to the scrolling area.
///
/// In TUI mode, this ensures the message appears in the scroll region.
/// In non-TUI mode, it just prints normally.
pub fn log(message: &str) -> io::Result<()> {
    if !TUI_ACTIVE.load(Ordering::SeqCst) {
        println!("{}", message);
        return Ok(());
    }

    let mut stdout = io::stdout();
    let num_status = STATUS_LINES.load(Ordering::SeqCst);

    // Save cursor, move to scroll area, print, restore
    execute!(stdout, cursor::SavePosition)?;

    // Ensure we're in the scroll region
    let (_, height) = terminal::size()?;
    let (_, cur_y) = cursor::position()?;

    if cur_y < num_status {
        execute!(stdout, MoveTo(0, num_status))?;
    }

    // Print the message
    println!("{}", message);

    // If we're near the bottom, scroll
    let (_, new_y) = cursor::position()?;
    if new_y >= height - 1 {
        execute!(stdout, ScrollUp(1))?;
    }

    stdout.flush()?;

    Ok(())
}

/// Print a log message with formatting (like println!).
#[macro_export]
macro_rules! tui_log {
    ($($arg:tt)*) => {
        $crate::cli::tui::log(&format!($($arg)*)).ok()
    };
}

/// A guard that cleans up the TUI on drop.
pub struct TuiGuard {
    active: bool,
}

impl TuiGuard {
    /// Initialize TUI and return a guard that cleans up on drop.
    pub fn new(num_status_lines: u16) -> io::Result<Self> {
        let active = init(num_status_lines)?;
        Ok(Self { active })
    }

    /// Check if TUI is active.
    pub fn is_active(&self) -> bool {
        self.active
    }
}

impl Drop for TuiGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = cleanup();
        }
    }
}

/// Simple progress display that works with the TUI.
pub struct StatusBar {
    line: u16,
    prefix: String,
}

impl StatusBar {
    /// Create a new status bar on the given line.
    pub fn new(line: u16, prefix: &str) -> Self {
        Self {
            line,
            prefix: prefix.to_string(),
        }
    }

    /// Update the status bar message.
    pub fn set_message(&self, message: &str) {
        let content = format!("{}{}", self.prefix, message);
        let _ = set_status(self.line, &content);
    }

    /// Update with progress.
    pub fn set_progress(&self, current: u64, total: u64, message: &str) {
        let pct = if total > 0 {
            (current * 100) / total
        } else {
            0
        };
        let bar_width = 20;
        let filled = ((current * bar_width) / total.max(1)) as usize;
        let empty = bar_width as usize - filled;

        let bar = format!("[{}{}] {:>3}%", "█".repeat(filled), "░".repeat(empty), pct);

        let content = format!("{}{} {}", self.prefix, bar, message);
        let _ = set_status(self.line, &content);
    }

    /// Clear the status bar.
    pub fn clear(&self) {
        let _ = clear_status(self.line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_bar_progress_format() {
        let bar = StatusBar::new(0, "Download: ");
        // Just test that it doesn't panic
        bar.set_progress(50, 100, "file.pdf");
        bar.set_progress(0, 100, "starting");
        bar.set_progress(100, 100, "done");
    }

    #[test]
    fn test_status_bar_zero_total() {
        let bar = StatusBar::new(0, "Test: ");
        // Should handle zero total without panic
        bar.set_progress(0, 0, "empty");
    }

    #[test]
    fn test_is_active_callable() {
        // Just verify is_active() is callable - actual state depends on test ordering
        let _ = is_active();
    }
}
