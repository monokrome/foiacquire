//! Shared utilities for discovery sources: URL normalization, XML parsing, deduplication.

/// Extract the domain from a target that may be a full URL or bare domain.
///
/// If `target` starts with "http", parses it as a URL and returns the host.
/// Otherwise returns the target as-is.
pub fn extract_domain(target: &str) -> String {
    if target.starts_with("http") {
        url::Url::parse(target)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
            .unwrap_or_else(|| target.to_string())
    } else {
        target.to_string()
    }
}

/// Normalize a target into a base URL with scheme, stripping trailing slashes.
///
/// If `target` already starts with "http", just trims trailing `/`.
/// Otherwise prepends `https://`.
pub fn normalize_base_url(target: &str) -> String {
    if target.starts_with("http") {
        target.trim_end_matches('/').to_string()
    } else {
        format!("https://{}", target.trim_end_matches('/'))
    }
}

/// Extract all `<loc>` values from XML, unescaping XML entities.
pub fn extract_xml_locs(xml: &str) -> Vec<String> {
    let mut locs = Vec::new();
    for line in xml.lines() {
        let line = line.trim();
        if let Some(start) = line.find("<loc>") {
            if let Some(end) = line.find("</loc>") {
                let content_start = start + 5;
                if end > content_start {
                    let url = &line[content_start..end];
                    let url = url
                        .replace("&amp;", "&")
                        .replace("&lt;", "<")
                        .replace("&gt;", ">")
                        .replace("&quot;", "\"")
                        .replace("&apos;", "'");
                    if !url.is_empty() {
                        locs.push(url);
                    }
                }
            }
        }
    }
    locs
}

/// Sort, deduplicate, and optionally truncate a list of strings in place.
///
/// Pass `max_results: 0` to skip truncation.
pub fn dedup_and_limit(items: &mut Vec<String>, max_results: usize) {
    items.sort();
    items.dedup();
    if max_results > 0 && items.len() > max_results {
        items.truncate(max_results);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_domain_from_url() {
        assert_eq!(extract_domain("https://fbi.gov/foia"), "fbi.gov");
        assert_eq!(
            extract_domain("https://vault.fbi.gov/page"),
            "vault.fbi.gov"
        );
    }

    #[test]
    fn extract_domain_bare() {
        assert_eq!(extract_domain("fbi.gov"), "fbi.gov");
    }

    #[test]
    fn normalize_base_url_with_scheme() {
        assert_eq!(
            normalize_base_url("https://example.gov/"),
            "https://example.gov"
        );
    }

    #[test]
    fn normalize_base_url_without_scheme() {
        assert_eq!(normalize_base_url("example.gov"), "https://example.gov");
    }

    #[test]
    fn extract_xml_locs_basic() {
        let xml = r#"<urlset>
  <url><loc>https://a.gov/1</loc></url>
  <url><loc>https://a.gov/2</loc></url>
</urlset>"#;
        let locs = extract_xml_locs(xml);
        assert_eq!(locs, vec!["https://a.gov/1", "https://a.gov/2"]);
    }

    #[test]
    fn extract_xml_locs_unescapes_entities() {
        let xml = "<url><loc>https://a.gov/?q=1&amp;p=2</loc></url>";
        let locs = extract_xml_locs(xml);
        assert_eq!(locs, vec!["https://a.gov/?q=1&p=2"]);
    }

    #[test]
    fn dedup_and_limit_deduplicates() {
        let mut items = vec!["b".into(), "a".into(), "b".into(), "c".into()];
        dedup_and_limit(&mut items, 0);
        assert_eq!(items, vec!["a", "b", "c"]);
    }

    #[test]
    fn dedup_and_limit_truncates() {
        let mut items = vec!["c".into(), "a".into(), "b".into()];
        dedup_and_limit(&mut items, 2);
        assert_eq!(items, vec!["a", "b"]);
    }

    #[test]
    fn dedup_and_limit_zero_skips_truncation() {
        let mut items = vec!["c".into(), "a".into(), "b".into()];
        dedup_and_limit(&mut items, 0);
        assert_eq!(items.len(), 3);
    }
}
