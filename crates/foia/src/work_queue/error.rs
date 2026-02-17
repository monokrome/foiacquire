//! Work queue error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkQueueError {
    #[error("Database error: {0}")]
    Database(#[from] diesel::result::Error),
    #[error("Item already claimed by another worker")]
    AlreadyClaimed,
    #[error("Item not found: {0}")]
    NotFound(String),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("{0}")]
    Other(String),
}
