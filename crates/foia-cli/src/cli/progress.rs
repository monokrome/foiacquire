//! Multi-progress display for concurrent downloads.
//!
//! Also provides global progress context for coordinating output from
//! any part of the application during progress display.

#![allow(dead_code)]

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::RwLock;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

/// Global reference to active progress display for coordinating output.
static ACTIVE_PROGRESS: OnceLock<RwLock<Option<MultiProgress>>> = OnceLock::new();

fn get_active_progress() -> &'static RwLock<Option<MultiProgress>> {
    ACTIVE_PROGRESS.get_or_init(|| RwLock::new(None))
}

/// Set the global active progress display.
pub fn set_active_progress(multi: Option<MultiProgress>) {
    if let Ok(mut guard) = get_active_progress().write() {
        *guard = multi;
    }
}

/// Print a message that coordinates with any active progress display.
/// Falls back to println! if no progress display is active.
pub fn progress_println(message: &str) {
    if let Ok(guard) = get_active_progress().read() {
        if let Some(ref multi) = *guard {
            let _ = multi.println(message);
            return;
        }
    }
    println!("{}", message);
}

/// Macro for printing that coordinates with progress display.
#[macro_export]
macro_rules! progress_println {
    ($($arg:tt)*) => {
        $crate::cli::progress::progress_println(&format!($($arg)*))
    };
}

/// Manages a pool of progress bars for concurrent downloads.
pub struct DownloadProgress {
    multi: MultiProgress,
    slots: Arc<Mutex<Vec<ProgressBarSlot>>>,
    summary_bar: ProgressBar,
}

struct ProgressBarSlot {
    bar: ProgressBar,
    in_use: bool,
}

impl DownloadProgress {
    /// Create a new download progress display with the given number of worker slots.
    pub fn new(num_workers: usize, total_pending: u64) -> Self {
        let multi = MultiProgress::new();

        // Summary bar at the top
        let summary_bar = multi.add(ProgressBar::new(total_pending));
        summary_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} [{bar:30.cyan/blue}] {pos}/{len}")
                .unwrap()
                .progress_chars("█▓░"),
        );
        summary_bar.set_message("Downloading");

        // Create slots for each worker
        let mut slots = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let bar = multi.add(ProgressBar::new(0));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "  {spinner:.dim} {wide_msg} [{bar:25.cyan/blue}] {bytes}/{total_bytes}",
                    )
                    .unwrap()
                    .progress_chars("━╸─"),
            );
            bar.set_message("idle");
            slots.push(ProgressBarSlot { bar, in_use: false });
        }

        // Register as active progress display for coordinated output
        set_active_progress(Some(multi.clone()));

        Self {
            multi,
            slots: Arc::new(Mutex::new(slots)),
            summary_bar,
        }
    }

    /// Acquire a progress bar slot for a download.
    /// Returns the slot index to use with other methods.
    pub async fn start_download(
        &self,
        worker_id: usize,
        filename: &str,
        total_bytes: Option<u64>,
    ) -> usize {
        let mut slots = self.slots.lock().await;

        // Use the worker's dedicated slot
        let slot = &mut slots[worker_id];
        slot.in_use = true;

        let display_name = truncate_filename(filename, 35);
        slot.bar.set_length(total_bytes.unwrap_or(0));
        slot.bar.set_position(0);
        slot.bar.set_message(display_name);

        if total_bytes.is_none() {
            // Unknown size - use spinner style
            slot.bar.set_style(
                ProgressStyle::default_spinner()
                    .template("  {spinner:.cyan} {wide_msg}")
                    .unwrap(),
            );
        } else {
            slot.bar.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "  {spinner:.cyan} {wide_msg} [{bar:25.cyan/blue}] {bytes}/{total_bytes}",
                    )
                    .unwrap()
                    .progress_chars("━╸─"),
            );
        }

        slot.bar
            .enable_steady_tick(std::time::Duration::from_millis(100));

        worker_id
    }

    /// Update progress for a download.
    pub async fn update_progress(&self, slot_id: usize, bytes_downloaded: u64) {
        let slots = self.slots.lock().await;
        if slot_id < slots.len() {
            slots[slot_id].bar.set_position(bytes_downloaded);
        }
    }

    /// Mark a download as complete and free the slot.
    pub async fn finish_download(&self, slot_id: usize, success: bool) {
        let mut slots = self.slots.lock().await;
        if slot_id < slots.len() {
            let slot = &mut slots[slot_id];
            slot.in_use = false;
            slot.bar.disable_steady_tick();

            // Reset to idle state
            slot.bar.set_style(
                ProgressStyle::default_bar()
                    .template("  {spinner:.dim} {wide_msg}")
                    .unwrap()
                    .progress_chars("━╸─"),
            );
            slot.bar.set_message("idle");
            slot.bar.set_length(0);
            slot.bar.set_position(0);
        }

        // Update summary
        if success {
            self.summary_bar.inc(1);
        }
    }

    /// Update the summary message.
    pub fn set_summary(&self, downloaded: usize, skipped: usize) {
        self.summary_bar
            .set_message(format!("Downloaded: {} | Skipped: {}", downloaded, skipped));
    }

    /// Finish all progress bars and clear the display.
    pub async fn finish(&self) {
        let slots = self.slots.lock().await;
        for slot in slots.iter() {
            slot.bar.finish_and_clear();
        }
        self.summary_bar.finish_and_clear();

        // Unregister active progress display
        set_active_progress(None);
    }

    /// Print a message that coordinates with the progress display.
    /// This prevents the message from corrupting the progress bars.
    pub fn println(&self, message: &str) {
        let _ = self.multi.println(message);
    }

    /// Suspend the progress display to run a closure that may output text.
    /// The progress bars will be hidden, the closure runs, then bars are restored.
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.multi.suspend(f)
    }
}

/// Truncate a filename for display, keeping the extension visible.
fn truncate_filename(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        return name.to_string();
    }

    // Try to keep the extension
    if let Some(dot_pos) = name.rfind('.') {
        let ext = &name[dot_pos..];
        if ext.len() < max_len - 4 {
            let prefix_len = max_len - ext.len() - 3;
            return format!("{}...{}", &name[..prefix_len], ext);
        }
    }

    // Just truncate
    format!("{}...", &name[..max_len - 3])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_filename() {
        assert_eq!(truncate_filename("short.pdf", 20), "short.pdf");
        assert_eq!(
            truncate_filename("a_very_long_filename_that_needs_truncation.pdf", 25),
            "a_very_long_filena....pdf"
        );
        assert_eq!(truncate_filename("no_extension", 8), "no_ex...");
    }
}
