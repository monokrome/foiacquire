//! LLM integration for document summarization and tagging.
//!
//! Uses a local LLM (via Ollama) to generate synopses and tags for documents.

mod client;

pub use client::{LlmClient, LlmConfig};
