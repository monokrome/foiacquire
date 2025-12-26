//! JSON path and URL extraction utilities.

use super::super::config::UrlExtractionConfig;

/// Resolve a path to a full URL, handling both absolute and relative paths.
pub fn resolve_url(base_url: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        path.to_string()
    } else {
        format!("{}{}", base_url, path)
    }
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
