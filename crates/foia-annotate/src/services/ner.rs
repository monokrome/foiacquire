//! Named Entity Recognition for government/FOIA documents.
//!
//! Provides a `NerBackend` trait for pluggable extraction backends and a
//! built-in `RegexNerBackend` tuned for declassified government documents.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// A single extracted entity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Entity {
    pub text: String,
    pub entity_type: EntityType,
}

/// Classification of extracted entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Organization,
    Person,
    FileNumber,
    Location,
}

/// Result of NER extraction on a document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NerResult {
    pub entities: Vec<Entity>,
    pub counts: HashMap<String, usize>,
}

/// Trait for pluggable NER backends.
///
/// The built-in `RegexNerBackend` uses pattern matching tuned for FOIA
/// documents. Future backends (e.g. rust-bert, LLM-based) can implement
/// this trait and be swapped in via `NerAnnotator`.
#[allow(dead_code)]
pub trait NerBackend: Send + Sync {
    /// Human-readable backend identifier (e.g. "regex", "bert").
    fn backend_id(&self) -> &str;

    /// Extract named entities from text.
    fn extract(&self, text: &str) -> NerResult;
}

// ============================================================================
// RegexNerBackend — built-in, zero-dependency backend
// ============================================================================

/// Regex-based NER backend tuned for government/FOIA documents.
///
/// Extracts organizations (government agencies), person names,
/// file/case numbers, classification markings, and locations using
/// pattern matching. High precision on its target domain, no external
/// models or runtime dependencies.
pub struct RegexNerBackend;

impl RegexNerBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RegexNerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl NerBackend for RegexNerBackend {
    fn backend_id(&self) -> &str {
        "regex"
    }

    fn extract(&self, text: &str) -> NerResult {
        let mut seen = HashSet::new();
        let mut entities = Vec::new();

        extract_organizations(text, &mut entities, &mut seen);
        extract_persons(text, &mut entities, &mut seen);
        extract_file_numbers(text, &mut entities, &mut seen);
        extract_locations(text, &mut entities, &mut seen);

        let mut counts = HashMap::new();
        for entity in &entities {
            let key = format!("{:?}", entity.entity_type).to_lowercase();
            *counts.entry(key).or_insert(0) += 1;
        }

        NerResult { entities, counts }
    }
}

/// Convenience function — extracts entities using the default `RegexNerBackend`.
#[allow(dead_code)]
pub fn extract_entities(text: &str) -> NerResult {
    RegexNerBackend.extract(text)
}

// ============================================================================
// Known government agencies and organizations
// ============================================================================

static KNOWN_AGENCIES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "CIA", "FBI", "NSA", "NSC", "DOD", "DOJ", "DOS", "DOE", "DIA", "NRO", "NGA", "DEA", "ATF",
        "ICE", "CBP", "TSA", "FEMA", "ODNI", "DNI", "DARPA", "IARPA", "USAID", "EPA", "IRS", "SEC",
        "FTC", "FCC", "FAA", "NASA", "NIST", "NIH", "CDC", "FDA", "USDA", "HHS", "DHS", "CISA",
        "ONI", "OSS", "KGB", "MI5", "MI6", "GCHQ", "BND", "DGSE", "MOSSAD", "ASIS", "CSIS", "ISI",
        "NATO", "INTERPOL", "GAO", "CBO", "OMB",
    ]
    .into_iter()
    .collect()
});

static FULL_NAME_AGENCIES: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    vec![
        "Central Intelligence Agency",
        "Federal Bureau of Investigation",
        "National Security Agency",
        "National Security Council",
        "Department of Defense",
        "Department of Justice",
        "Department of State",
        "Department of Energy",
        "Defense Intelligence Agency",
        "National Reconnaissance Office",
        "Drug Enforcement Administration",
        "Bureau of Alcohol, Tobacco, Firearms and Explosives",
        "Department of Homeland Security",
        "Office of the Director of National Intelligence",
        "Office of Strategic Services",
        "Government Accountability Office",
        "Congressional Budget Office",
        "Office of Management and Budget",
        "Joint Chiefs of Staff",
        "White House",
        "State Department",
        "Pentagon",
        "Secret Service",
        "U.S. Army",
        "U.S. Navy",
        "U.S. Air Force",
        "U.S. Marines",
        "U.S. Coast Guard",
        "National Guard",
    ]
});

// ============================================================================
// Person name patterns
// ============================================================================

static TITLE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?:(?:President|Vice President|Secretary|Director|General|Admiral|Colonel|Major|Captain|Lieutenant|Sergeant|Agent|Ambassador|Senator|Representative|Congressman|Congresswoman|Governor|Mayor|Judge|Justice|Attorney General|Dr\.|Prof\.|Mr\.|Mrs\.|Ms\.)\s+)([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)",
    )
    .expect("title pattern should compile")
});

static CAPITALIZED_NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b([A-Z][a-z]{2,}(?:\s+[A-Z]\.?\s+|\s+)[A-Z][a-z]{2,})\b")
        .expect("capitalized name pattern should compile")
});

// ============================================================================
// File/case number patterns
// ============================================================================

static FILE_NUMBER_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        // CIA-style: CIA-RDP96-00788R002100520004-9
        Regex::new(r"\b([A-Z]{2,5}-[A-Z]{2,5}\d{2,}-\d{4,}[A-Z]?\d*(?:-\d+)?)\b").unwrap(),
        // FOIA case: FOIA-2024-00123
        Regex::new(r"\b(FOIA-\d{4}-\d{3,})\b").unwrap(),
        // Document control number patterns
        Regex::new(r"\b((?:DOC|REF|MEMO|CABLE|REPORT)\s*(?:#|No\.?|Number)?\s*\d[\d\-/]{3,})\b")
            .unwrap(),
        // Classification marking: TOP SECRET//SCI, SECRET//NOFORN
        Regex::new(r"\b((?:TOP SECRET|SECRET|CONFIDENTIAL)(?://[A-Z]+(?:/[A-Z]+)*)?)\b").unwrap(),
    ]
});

// ============================================================================
// Location patterns
// ============================================================================

static US_STATES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "Alabama",
        "Alaska",
        "Arizona",
        "Arkansas",
        "California",
        "Colorado",
        "Connecticut",
        "Delaware",
        "Florida",
        "Georgia",
        "Hawaii",
        "Idaho",
        "Illinois",
        "Indiana",
        "Iowa",
        "Kansas",
        "Kentucky",
        "Louisiana",
        "Maine",
        "Maryland",
        "Massachusetts",
        "Michigan",
        "Minnesota",
        "Mississippi",
        "Missouri",
        "Montana",
        "Nebraska",
        "Nevada",
        "New Hampshire",
        "New Jersey",
        "New Mexico",
        "New York",
        "North Carolina",
        "North Dakota",
        "Ohio",
        "Oklahoma",
        "Oregon",
        "Pennsylvania",
        "Rhode Island",
        "South Carolina",
        "South Dakota",
        "Tennessee",
        "Texas",
        "Utah",
        "Vermont",
        "Virginia",
        "Washington",
        "West Virginia",
        "Wisconsin",
        "Wyoming",
    ]
    .into_iter()
    .collect()
});

static NOTABLE_LOCATIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "Washington D.C.",
        "Langley",
        "Fort Meade",
        "Quantico",
        "Camp David",
        "Area 51",
        "Guantanamo",
        "Berlin",
        "Moscow",
        "London",
        "Tokyo",
        "Beijing",
        "Saigon",
        "Havana",
        "Baghdad",
        "Kabul",
        "Tehran",
        "Pyongyang",
        "Islamabad",
    ]
    .into_iter()
    .collect()
});

// Words that look like names but aren't — reduces false positives.
static NAME_STOPWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "The United",
        "United States",
        "New York",
        "New Jersey",
        "New Mexico",
        "New Hampshire",
        "North Carolina",
        "North Dakota",
        "South Carolina",
        "South Dakota",
        "West Virginia",
        "Top Secret",
        "No Foreign",
        "National Security",
        "Foreign Affairs",
        "Central Intelligence",
        "Federal Bureau",
    ]
    .into_iter()
    .collect()
});

// ============================================================================
// Extraction helpers
// ============================================================================

fn extract_organizations(text: &str, entities: &mut Vec<Entity>, seen: &mut HashSet<String>) {
    for agency in KNOWN_AGENCIES.iter() {
        let pattern = format!(r"\b{}\b", regex::escape(agency));
        if let Ok(re) = Regex::new(&pattern) {
            if re.is_match(text) {
                let key = format!("org:{}", agency);
                if seen.insert(key) {
                    entities.push(Entity {
                        text: agency.to_string(),
                        entity_type: EntityType::Organization,
                    });
                }
            }
        }
    }

    for agency in FULL_NAME_AGENCIES.iter() {
        if text.contains(agency) {
            let key = format!("org:{}", agency);
            if seen.insert(key) {
                entities.push(Entity {
                    text: agency.to_string(),
                    entity_type: EntityType::Organization,
                });
            }
        }
    }
}

fn extract_persons(text: &str, entities: &mut Vec<Entity>, seen: &mut HashSet<String>) {
    for cap in TITLE_PATTERN.captures_iter(text) {
        if let Some(name_match) = cap.get(1) {
            let name = name_match.as_str().trim();
            if is_plausible_name(name) {
                let key = format!("person:{}", name);
                if seen.insert(key) {
                    entities.push(Entity {
                        text: name.to_string(),
                        entity_type: EntityType::Person,
                    });
                }
            }
        }
    }

    for cap in CAPITALIZED_NAME.captures_iter(text) {
        if let Some(name_match) = cap.get(1) {
            let name = name_match.as_str().trim();
            if is_plausible_name(name) && !is_stopword_name(name) {
                let key = format!("person:{}", name);
                if seen.insert(key) {
                    entities.push(Entity {
                        text: name.to_string(),
                        entity_type: EntityType::Person,
                    });
                }
            }
        }
    }
}

fn extract_file_numbers(text: &str, entities: &mut Vec<Entity>, seen: &mut HashSet<String>) {
    for pattern in FILE_NUMBER_PATTERNS.iter() {
        for cap in pattern.captures_iter(text) {
            if let Some(m) = cap.get(1) {
                let file_num = m.as_str().trim();
                let key = format!("file:{}", file_num);
                if seen.insert(key) {
                    entities.push(Entity {
                        text: file_num.to_string(),
                        entity_type: EntityType::FileNumber,
                    });
                }
            }
        }
    }
}

fn extract_locations(text: &str, entities: &mut Vec<Entity>, seen: &mut HashSet<String>) {
    for state in US_STATES.iter() {
        let pattern = format!(r"\b{}\b", regex::escape(state));
        if let Ok(re) = Regex::new(&pattern) {
            if re.is_match(text) {
                let key = format!("loc:{}", state);
                if seen.insert(key) {
                    entities.push(Entity {
                        text: state.to_string(),
                        entity_type: EntityType::Location,
                    });
                }
            }
        }
    }

    for location in NOTABLE_LOCATIONS.iter() {
        if text.contains(location) {
            let key = format!("loc:{}", location);
            if seen.insert(key) {
                entities.push(Entity {
                    text: location.to_string(),
                    entity_type: EntityType::Location,
                });
            }
        }
    }
}

fn is_plausible_name(name: &str) -> bool {
    let parts: Vec<&str> = name.split_whitespace().collect();
    if parts.len() < 2 || parts.len() > 4 {
        return false;
    }
    parts.iter().all(|p| {
        let first = p.chars().next().unwrap_or('a');
        first.is_uppercase() && p.len() >= 2
    })
}

fn is_stopword_name(name: &str) -> bool {
    NAME_STOPWORDS.contains(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_agencies() {
        let text = "The CIA and FBI conducted a joint operation with the NSA.";
        let result = extract_entities(text);

        let org_names: Vec<&str> = result
            .entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Organization)
            .map(|e| e.text.as_str())
            .collect();

        assert!(org_names.contains(&"CIA"));
        assert!(org_names.contains(&"FBI"));
        assert!(org_names.contains(&"NSA"));
    }

    #[test]
    fn test_extract_full_name_agencies() {
        let text = "The Central Intelligence Agency worked with the Department of Defense.";
        let result = extract_entities(text);

        let org_names: Vec<&str> = result
            .entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Organization)
            .map(|e| e.text.as_str())
            .collect();

        assert!(org_names.contains(&"Central Intelligence Agency"));
        assert!(org_names.contains(&"Department of Defense"));
    }

    #[test]
    fn test_extract_titled_persons() {
        let text = "Director Allen Dulles met with President John Kennedy.";
        let result = extract_entities(text);

        let persons: Vec<&str> = result
            .entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Person)
            .map(|e| e.text.as_str())
            .collect();

        assert!(persons.contains(&"Allen Dulles"));
        assert!(persons.contains(&"John Kennedy"));
    }

    #[test]
    fn test_extract_file_numbers() {
        let text = "See document CIA-RDP96-00788R002100520004-9 and FOIA-2024-00123.";
        let result = extract_entities(text);

        let files: Vec<&str> = result
            .entities
            .iter()
            .filter(|e| e.entity_type == EntityType::FileNumber)
            .map(|e| e.text.as_str())
            .collect();

        assert!(files.contains(&"CIA-RDP96-00788R002100520004-9"));
        assert!(files.contains(&"FOIA-2024-00123"));
    }

    #[test]
    fn test_extract_locations() {
        let text = "Operations in Virginia and at Langley headquarters.";
        let result = extract_entities(text);

        let locs: Vec<&str> = result
            .entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Location)
            .map(|e| e.text.as_str())
            .collect();

        assert!(locs.contains(&"Virginia"));
        assert!(locs.contains(&"Langley"));
    }

    #[test]
    fn test_extract_classification() {
        let text = "This document is classified TOP SECRET//SCI.";
        let result = extract_entities(text);

        let files: Vec<&str> = result
            .entities
            .iter()
            .filter(|e| e.entity_type == EntityType::FileNumber)
            .map(|e| e.text.as_str())
            .collect();

        assert!(files.contains(&"TOP SECRET//SCI"));
    }

    #[test]
    fn test_no_duplicate_entities() {
        let text = "The CIA met the CIA and the CIA again.";
        let result = extract_entities(text);

        let cia_count = result.entities.iter().filter(|e| e.text == "CIA").count();
        assert_eq!(cia_count, 1);
    }

    #[test]
    fn test_stopword_filtering() {
        let text = "United States government operations.";
        let result = extract_entities(text);

        let persons: Vec<&str> = result
            .entities
            .iter()
            .filter(|e| e.entity_type == EntityType::Person)
            .map(|e| e.text.as_str())
            .collect();

        assert!(!persons.contains(&"United States"));
    }

    #[test]
    fn test_empty_text() {
        let result = extract_entities("");
        assert!(result.entities.is_empty());
        assert!(result.counts.is_empty());
    }

    #[test]
    fn test_counts_by_type() {
        let text = "CIA and FBI in Virginia.";
        let result = extract_entities(text);

        assert_eq!(result.counts.get("organization"), Some(&2));
        assert_eq!(result.counts.get("location"), Some(&1));
    }

    #[test]
    fn test_regex_backend_id() {
        let backend = RegexNerBackend::new();
        assert_eq!(backend.backend_id(), "regex");
    }

    #[test]
    fn test_backend_trait_matches_convenience() {
        let text = "The CIA operates from Langley with Director John Smith.";
        let convenience = extract_entities(text);
        let via_trait = RegexNerBackend::new().extract(text);

        assert_eq!(convenience.entities.len(), via_trait.entities.len());
        for (a, b) in convenience.entities.iter().zip(via_trait.entities.iter()) {
            assert_eq!(a, b);
        }
    }
}
