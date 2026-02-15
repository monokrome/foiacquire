//! Command-line interface for foia.

mod commands;
pub mod helpers;
pub mod icons;
pub mod progress;
pub mod tui;

pub use commands::{is_verbose, run};
#[allow(unused_imports)]
pub use progress::progress_println;
