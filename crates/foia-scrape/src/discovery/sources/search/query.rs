//! Search query builder utilities.
//!
//! Helps construct search queries with site:, filetype:, and other operators.

/// Builder for constructing search queries.
#[derive(Debug, Clone, Default)]
pub struct QueryBuilder {
    /// Target domain for site: restriction.
    site: Option<String>,
    /// File type filter.
    filetype: Option<String>,
    /// Search terms.
    terms: Vec<String>,
    /// Exact phrase matches.
    phrases: Vec<String>,
    /// Excluded terms.
    excluded: Vec<String>,
    /// URL path filter (inurl:).
    inurl: Option<String>,
}

impl QueryBuilder {
    /// Create a new query builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict search to a specific domain.
    pub fn site(mut self, domain: &str) -> Self {
        self.site = Some(domain.to_string());
        self
    }

    /// Filter by file type.
    pub fn filetype(mut self, ext: &str) -> Self {
        self.filetype = Some(ext.to_string());
        self
    }

    /// Add a search term.
    pub fn term(mut self, term: &str) -> Self {
        if !term.is_empty() {
            self.terms.push(term.to_string());
        }
        self
    }

    /// Add multiple search terms.
    pub fn terms(mut self, terms: &[String]) -> Self {
        for term in terms {
            if !term.is_empty() {
                self.terms.push(term.clone());
            }
        }
        self
    }

    /// Add an exact phrase match.
    pub fn phrase(mut self, phrase: &str) -> Self {
        if !phrase.is_empty() {
            self.phrases.push(phrase.to_string());
        }
        self
    }

    /// Exclude a term from results.
    pub fn exclude(mut self, term: &str) -> Self {
        if !term.is_empty() {
            self.excluded.push(term.to_string());
        }
        self
    }

    /// Filter by URL path.
    pub fn inurl(mut self, path: &str) -> Self {
        self.inurl = Some(path.to_string());
        self
    }

    /// Build the final query string.
    pub fn build(&self) -> String {
        let mut parts = Vec::new();

        // Add site restriction first
        if let Some(ref site) = self.site {
            parts.push(format!("site:{}", site));
        }

        // Add filetype
        if let Some(ref ft) = self.filetype {
            parts.push(format!("filetype:{}", ft));
        }

        // Add inurl
        if let Some(ref inurl) = self.inurl {
            parts.push(format!("inurl:{}", inurl));
        }

        // Add exact phrases
        for phrase in &self.phrases {
            parts.push(format!("\"{}\"", phrase));
        }

        // Add regular terms
        for term in &self.terms {
            parts.push(term.clone());
        }

        // Add excluded terms
        for term in &self.excluded {
            parts.push(format!("-{}", term));
        }

        parts.join(" ")
    }
}

/// Pre-built queries for common discovery patterns.
pub struct CommonQueries;

impl CommonQueries {
    /// Queries focused on finding listing/index pages.
    pub fn listing_page_terms() -> &'static [&'static str] {
        &[
            "reading room",
            "document library",
            "publications",
            "reports",
            "FOIA",
            "declassified",
            "index of",
            "browse documents",
            "all reports",
            "archive",
        ]
    }

    /// Queries for document types.
    pub fn document_terms() -> &'static [&'static str] {
        &[
            "report",
            "audit",
            "investigation",
            "memorandum",
            "memo",
            "letter",
            "order",
            "decision",
            "opinion",
            "guidance",
        ]
    }

    /// Build a set of queries for a domain prioritizing listing pages.
    pub fn listing_queries(domain: &str) -> Vec<String> {
        Self::listing_page_terms()
            .iter()
            .map(|term| QueryBuilder::new().site(domain).phrase(term).build())
            .collect()
    }

    /// Build queries combining domain with custom terms.
    pub fn custom_queries(domain: &str, terms: &[String]) -> Vec<String> {
        terms
            .iter()
            .map(|term| QueryBuilder::new().site(domain).term(term).build())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_site_query() {
        let query = QueryBuilder::new().site("example.gov").build();
        assert_eq!(query, "site:example.gov");
    }

    #[test]
    fn site_with_term() {
        let query = QueryBuilder::new().site("example.gov").term("FOIA").build();
        assert_eq!(query, "site:example.gov FOIA");
    }

    #[test]
    fn site_with_phrase() {
        let query = QueryBuilder::new()
            .site("example.gov")
            .phrase("reading room")
            .build();
        assert_eq!(query, "site:example.gov \"reading room\"");
    }

    #[test]
    fn complex_query() {
        let query = QueryBuilder::new()
            .site("cia.gov")
            .filetype("pdf")
            .phrase("reading room")
            .term("declassified")
            .exclude("classified")
            .build();

        assert!(query.contains("site:cia.gov"));
        assert!(query.contains("filetype:pdf"));
        assert!(query.contains("\"reading room\""));
        assert!(query.contains("declassified"));
        assert!(query.contains("-classified"));
    }

    #[test]
    fn listing_queries_generation() {
        let queries = CommonQueries::listing_queries("fbi.gov");
        assert!(!queries.is_empty());
        assert!(queries[0].contains("site:fbi.gov"));
    }

    #[test]
    fn custom_queries_generation() {
        let terms = vec!["mkultra".to_string(), "cointelpro".to_string()];
        let queries = CommonQueries::custom_queries("cia.gov", &terms);

        assert_eq!(queries.len(), 2);
        assert!(queries[0].contains("site:cia.gov"));
        assert!(queries[0].contains("mkultra"));
    }
}
