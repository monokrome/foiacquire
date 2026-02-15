//! Scrape, download, status, and refresh commands.
//!
//! Split into submodules:
//! - `helpers.rs`: Helper functions for document processing
//! - `scrape_cmd.rs`: Main scrape command
//! - `download.rs`: Download pending documents
//! - `status.rs`: Show system status
//! - `refresh.rs`: Refresh document metadata

mod discovery;
mod download;
mod helpers;
mod refresh;
mod scrape_cmd;
mod single_source;
mod status;

pub use download::cmd_download;
pub use refresh::cmd_refresh;
pub use scrape_cmd::cmd_scrape;
pub use status::cmd_status;
