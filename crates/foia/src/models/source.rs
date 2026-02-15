//! Source models for FOIA document sources.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Type of document source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    FbiVault,
    CiaFoia,
    FoiaGov,
    MuckRock,
    DocumentCloud,
    Custom,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FbiVault => "fbi_vault",
            Self::CiaFoia => "cia_foia",
            Self::FoiaGov => "foia_gov",
            Self::MuckRock => "muckrock",
            Self::DocumentCloud => "documentcloud",
            Self::Custom => "custom",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "fbi_vault" => Some(Self::FbiVault),
            "cia_foia" => Some(Self::CiaFoia),
            "foia_gov" => Some(Self::FoiaGov),
            "muckrock" => Some(Self::MuckRock),
            "documentcloud" => Some(Self::DocumentCloud),
            "custom" => Some(Self::Custom),
            _ => None,
        }
    }
}

/// A FOIA document source.
///
/// Represents an agency or organization that publishes FOIA documents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    /// Unique identifier for this source.
    pub id: String,
    /// Type of source.
    pub source_type: SourceType,
    /// Human-readable name.
    pub name: String,
    /// Base URL for the source.
    pub base_url: String,
    /// Additional source-specific metadata.
    pub metadata: serde_json::Value,
    /// When the source was added.
    pub created_at: DateTime<Utc>,
    /// When the source was last scraped.
    pub last_scraped: Option<DateTime<Utc>>,
}

impl Source {
    /// Create a new source.
    pub fn new(id: String, source_type: SourceType, name: String, base_url: String) -> Self {
        Self {
            id,
            source_type,
            name,
            base_url,
            metadata: serde_json::json!({}),
            created_at: Utc::now(),
            last_scraped: None,
        }
    }
}
