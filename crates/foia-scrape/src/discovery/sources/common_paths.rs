//! Common path enumeration discovery source.
//!
//! Enumerates well-known paths that often contain documents on government sites.

use async_trait::async_trait;
use tracing::debug;

use super::create_discovery_client;
use crate::discovery::url_utils::normalize_base_url;
use crate::discovery::{DiscoveredUrl, DiscoveryError, DiscoverySource, DiscoverySourceConfig};
use foia::models::DiscoveryMethod;

/// Common paths found on government document sites.
const COMMON_PATHS: &[&str] = &[
    // FOIA reading rooms
    "/foia/",
    "/foia/reading-room/",
    "/foia/electronic-reading-room/",
    "/foia/library/",
    "/foia/documents/",
    "/foia/records/",
    "/reading-room/",
    "/readingroom/",
    "/electronic-reading-room/",
    // Document sections
    "/documents/",
    "/docs/",
    "/files/",
    "/publications/",
    "/reports/",
    "/records/",
    "/library/",
    "/resources/",
    // Archives
    "/archive/",
    "/archives/",
    "/declassified/",
    "/historical/",
    // News and releases
    "/news/",
    "/press/",
    "/press-releases/",
    "/newsroom/",
    "/media/",
    // Data and statistics
    "/data/",
    "/statistics/",
    "/datasets/",
    // Specific document types
    "/audits/",
    "/investigations/",
    "/opinions/",
    "/decisions/",
    "/orders/",
    "/regulations/",
    "/policies/",
    "/guidance/",
    "/manuals/",
    "/forms/",
    // Inspector General
    "/oig/",
    "/inspector-general/",
    "/oversight/",
    // Budget and financial
    "/budget/",
    "/financial/",
    "/contracts/",
    "/grants/",
];

/// Discovery source that checks common document paths.
#[derive(Default)]
pub struct CommonPathsSource {
    /// Additional custom paths to check.
    custom_paths: Vec<String>,
}

impl CommonPathsSource {
    /// Create a new common paths source.
    pub fn new() -> Self {
        Self {
            custom_paths: Vec::new(),
        }
    }

    /// Add custom paths to check.
    pub fn with_custom_paths(mut self, paths: Vec<String>) -> Self {
        self.custom_paths = paths;
        self
    }

    /// Get all paths to check.
    fn all_paths(&self) -> Vec<&str> {
        let mut paths: Vec<&str> = COMMON_PATHS.to_vec();
        paths.extend(self.custom_paths.iter().map(|s| s.as_str()));
        paths
    }

    /// Check if a path exists and returns a valid response.
    async fn check_path(
        &self,
        base_url: &str,
        path: &str,
        config: &DiscoverySourceConfig,
    ) -> Option<(String, u16)> {
        let url = format!("{}{}", base_url.trim_end_matches('/'), path);

        let client = match create_discovery_client("common_paths", config, None, None) {
            Ok(c) => c,
            Err(_) => return None,
        };

        match client.head(&url, None, None).await {
            Ok(response) => {
                let status = response.status.as_u16();
                // Note: HttpClient already follows redirects, so we get the final URL
                if status == 200 || status == 301 || status == 302 {
                    Some((url.clone(), status))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl DiscoverySource for CommonPathsSource {
    fn name(&self) -> &str {
        "common_paths"
    }

    fn method(&self) -> DiscoveryMethod {
        DiscoveryMethod::CommonPath
    }

    async fn discover(
        &self,
        target_domain: &str,
        _search_terms: &[String],
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<DiscoveredUrl>, DiscoveryError> {
        let base_url = normalize_base_url(target_domain);

        // Get additional paths from config
        let extra_paths: Vec<String> = config
            .custom_params
            .get("paths")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut all_paths = self.all_paths();
        all_paths.extend(extra_paths.iter().map(|s| s.as_str()));

        debug!(
            "Checking {} common paths for {}",
            all_paths.len(),
            target_domain
        );

        let mut discovered = Vec::new();

        // Check paths concurrently in batches
        let batch_size = 10;
        for chunk in all_paths.chunks(batch_size) {
            let futures: Vec<_> = chunk
                .iter()
                .map(|path| self.check_path(&base_url, path, config))
                .collect();

            let results = futures::future::join_all(futures).await;

            for (_path, result) in chunk.iter().zip(results) {
                if let Some((url, _status)) = result {
                    debug!("Found valid path: {}", url);

                    let mut disc_url = DiscoveredUrl::new(
                        url,
                        DiscoveryMethod::CommonPath,
                        "common_paths".to_string(),
                    )
                    .listing_page(); // All common paths are listing pages

                    disc_url.detect_listing_page();
                    discovered.push(disc_url);

                    // Apply limit
                    if config.max_results > 0 && discovered.len() >= config.max_results {
                        return Ok(discovered);
                    }
                }
            }

            // Rate limit between batches
            if config.rate_limit_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(config.rate_limit_ms)).await;
            }
        }

        debug!(
            "Common paths discovery found {} URLs for {}",
            discovered.len(),
            target_domain
        );

        Ok(discovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_paths_list() {
        let source = CommonPathsSource::new();
        let paths = source.all_paths();

        assert!(paths.contains(&"/foia/"));
        assert!(paths.contains(&"/documents/"));
        assert!(paths.contains(&"/reports/"));
    }

    #[test]
    fn custom_paths() {
        let source = CommonPathsSource::new().with_custom_paths(vec!["/custom/path/".to_string()]);
        let paths = source.all_paths();

        assert!(paths.contains(&"/custom/path/"));
        assert!(paths.contains(&"/foia/"));
    }
}
