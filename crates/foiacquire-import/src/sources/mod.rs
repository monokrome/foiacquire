//! Import source implementations.

pub mod concordance;
pub mod warc;

pub use concordance::{ConcordanceImportSource, MultiPageMode};
pub use warc::{guess_mime_type_from_url, WarcImportSource};
