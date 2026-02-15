//! Document analysis commands (MIME detection, text extraction, OCR).

mod check;
mod compare;
mod process;

pub use check::cmd_analyze_check;
pub use compare::cmd_analyze_compare;
pub use process::cmd_analyze;
