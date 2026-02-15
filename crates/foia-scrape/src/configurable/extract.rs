//! JSON path and URL extraction utilities.

use crate::config::UrlExtractionConfig;

/// Resolve a path to a full URL, handling both absolute and relative paths.
pub fn resolve_url(base_url: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }

    // Use proper URL joining to handle edge cases
    if let Ok(base) = url::Url::parse(base_url) {
        if let Ok(resolved) = base.join(path) {
            return resolved.to_string();
        }
    }

    // Fallback: manual joining with proper slash handling
    let base = base_url.trim_end_matches('/');
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    };
    format!("{}{}", base, path)
}

/// Extract a value from nested JSON using dot-notation path.
pub fn extract_path<'a>(data: &'a serde_json::Value, path: &str) -> &'a serde_json::Value {
    if path.is_empty() {
        return data;
    }

    let mut current = data;
    for key in path.split('.') {
        current = match current {
            serde_json::Value::Object(map) => map.get(key).unwrap_or(&serde_json::Value::Null),
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    arr.get(idx).unwrap_or(&serde_json::Value::Null)
                } else {
                    &serde_json::Value::Null
                }
            }
            _ => &serde_json::Value::Null,
        };
    }

    current
}

/// Extract URLs from an item using configured extraction rules.
/// Returns multiple URLs when nested_arrays is configured.
pub fn extract_urls(item: &serde_json::Value, extraction: &UrlExtractionConfig) -> Vec<String> {
    let mut urls = Vec::new();

    // If nested_arrays is configured, traverse the nested structure
    if !extraction.nested_arrays.is_empty() {
        extract_urls_nested(item, &extraction.nested_arrays, extraction, &mut urls);
        return urls;
    }

    // Simple extraction - single URL
    if let Some(url) = extract_single_url(item, extraction) {
        urls.push(url);
    }

    urls
}

/// Recursively extract URLs from nested arrays.
fn extract_urls_nested(
    item: &serde_json::Value,
    remaining_paths: &[String],
    extraction: &UrlExtractionConfig,
    urls: &mut Vec<String>,
) {
    if remaining_paths.is_empty() {
        // At the leaf - extract the URL
        if let Some(url) = extract_single_url(item, extraction) {
            urls.push(url);
        }
        return;
    }

    let current_path = &remaining_paths[0];
    let rest = &remaining_paths[1..];

    // Get the array at current path
    if let Some(arr) = item.get(current_path).and_then(|v| v.as_array()) {
        for nested_item in arr {
            extract_urls_nested(nested_item, rest, extraction, urls);
        }
    }
}

/// Extract a single URL from an item.
pub fn extract_single_url(
    item: &serde_json::Value,
    extraction: &UrlExtractionConfig,
) -> Option<String> {
    if let Some(s) = item.as_str() {
        return Some(s.to_string());
    }

    if let Some(obj) = item.as_object() {
        // Direct field extraction
        if let Some(url) = obj.get(&extraction.url_field).and_then(|v| v.as_str()) {
            return Some(url.to_string());
        }

        // Template-based URL construction
        if let Some(ref template) = extraction.url_template {
            let mut url: String = template.clone();
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    url = url.replace(&format!("{{{}}}", key), s);
                } else if let Some(n) = value.as_i64() {
                    url = url.replace(&format!("{{{}}}", key), &n.to_string());
                }
            }
            if !url.contains('{') {
                return Some(url);
            }
        }

        // Fallback field
        if let Some(ref fallback) = extraction.fallback_field {
            if let Some(url) = obj.get(fallback).and_then(|v| v.as_str()) {
                return Some(url.to_string());
            }
        }
    }

    None
}

/// Legacy single URL extraction for backward compatibility.
pub fn extract_url(item: &serde_json::Value, extraction: &UrlExtractionConfig) -> Option<String> {
    extract_urls(item, extraction).into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolve_url_absolute() {
        assert_eq!(
            resolve_url("https://example.com", "https://other.com/doc.pdf"),
            "https://other.com/doc.pdf"
        );
        assert_eq!(
            resolve_url("https://example.com", "http://other.com/doc.pdf"),
            "http://other.com/doc.pdf"
        );
    }

    #[test]
    fn test_resolve_url_relative() {
        // Absolute path from domain root
        assert_eq!(
            resolve_url("https://example.com", "/docs/file.pdf"),
            "https://example.com/docs/file.pdf"
        );
        // Absolute path resolves from domain root, not base path
        assert_eq!(
            resolve_url("https://example.com/api", "/docs/file.pdf"),
            "https://example.com/docs/file.pdf"
        );
        // Relative path (no leading slash) resolves from base
        assert_eq!(
            resolve_url("https://example.com/api/", "docs/file.pdf"),
            "https://example.com/api/docs/file.pdf"
        );
    }

    #[test]
    fn test_extract_path_simple() {
        let data = json!({"name": "test", "url": "https://example.com"});
        assert_eq!(extract_path(&data, "name"), &json!("test"));
        assert_eq!(extract_path(&data, "url"), &json!("https://example.com"));
    }

    #[test]
    fn test_extract_path_nested() {
        let data = json!({
            "data": {
                "items": [
                    {"url": "https://example.com/1"},
                    {"url": "https://example.com/2"}
                ]
            }
        });
        assert_eq!(
            extract_path(&data, "data.items.0.url"),
            &json!("https://example.com/1")
        );
        assert_eq!(
            extract_path(&data, "data.items.1.url"),
            &json!("https://example.com/2")
        );
    }

    #[test]
    fn test_extract_path_empty() {
        let data = json!({"name": "test"});
        assert_eq!(extract_path(&data, ""), &data);
    }

    #[test]
    fn test_extract_path_missing() {
        let data = json!({"name": "test"});
        assert_eq!(extract_path(&data, "missing"), &json!(null));
        assert_eq!(extract_path(&data, "a.b.c"), &json!(null));
    }

    #[test]
    fn test_extract_single_url_string() {
        let item = json!("https://example.com/doc.pdf");
        let config = UrlExtractionConfig::default();
        assert_eq!(
            extract_single_url(&item, &config),
            Some("https://example.com/doc.pdf".to_string())
        );
    }

    #[test]
    fn test_extract_single_url_object_field() {
        let item = json!({"url": "https://example.com/doc.pdf", "title": "Test"});
        let config = UrlExtractionConfig {
            url_field: "url".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_single_url(&item, &config),
            Some("https://example.com/doc.pdf".to_string())
        );
    }

    #[test]
    fn test_extract_single_url_template() {
        let item = json!({"id": "123", "type": "pdf"});
        let config = UrlExtractionConfig {
            url_field: "url".to_string(),
            url_template: Some("https://example.com/docs/{id}.{type}".to_string()),
            ..Default::default()
        };
        assert_eq!(
            extract_single_url(&item, &config),
            Some("https://example.com/docs/123.pdf".to_string())
        );
    }

    #[test]
    fn test_extract_single_url_fallback() {
        let item = json!({"link": "https://example.com/doc.pdf"});
        let config = UrlExtractionConfig {
            url_field: "url".to_string(),
            fallback_field: Some("link".to_string()),
            ..Default::default()
        };
        assert_eq!(
            extract_single_url(&item, &config),
            Some("https://example.com/doc.pdf".to_string())
        );
    }

    #[test]
    fn test_extract_urls_simple() {
        let item = json!({"url": "https://example.com/doc.pdf"});
        let config = UrlExtractionConfig {
            url_field: "url".to_string(),
            ..Default::default()
        };
        let urls = extract_urls(&item, &config);
        assert_eq!(urls, vec!["https://example.com/doc.pdf"]);
    }

    #[test]
    fn test_extract_urls_nested_arrays() {
        let item = json!({
            "files": [
                {"url": "https://example.com/1.pdf"},
                {"url": "https://example.com/2.pdf"}
            ]
        });
        let config = UrlExtractionConfig {
            url_field: "url".to_string(),
            nested_arrays: vec!["files".to_string()],
            ..Default::default()
        };
        let urls = extract_urls(&item, &config);
        assert_eq!(urls.len(), 2);
        assert!(urls.contains(&"https://example.com/1.pdf".to_string()));
        assert!(urls.contains(&"https://example.com/2.pdf".to_string()));
    }

    #[test]
    fn test_extract_url_legacy() {
        let item = json!({"url": "https://example.com/doc.pdf"});
        let config = UrlExtractionConfig {
            url_field: "url".to_string(),
            ..Default::default()
        };
        assert_eq!(
            extract_url(&item, &config),
            Some("https://example.com/doc.pdf".to_string())
        );
    }
}
