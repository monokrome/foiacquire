//! Import source implementations.

pub mod concordance;
pub mod warc;

pub use concordance::{ConcordanceImportSource, MultiPageMode};
pub use warc::WarcImportSource;
