//! Analysis backend manager for registration and selection.
//!
//! The manager handles:
//! - Registering built-in backends (OCR, Whisper)
//! - Registering custom backends from configuration
//! - Selecting appropriate backends for a given mimetype and method list

use std::collections::HashMap;
use std::sync::Arc;

use super::backend::{AnalysisBackend, AnalysisGranularity};
use super::custom::{CustomAnalysisConfig, CustomBackend};
use super::ocr_adapter::OcrAnalysisAdapter;
use super::whisper::{WhisperBackend, WhisperConfig};
use crate::ocr::TesseractBackend;

/// Manager for multiple analysis backends.
pub struct AnalysisManager {
    /// Registered backends by their identifier.
    /// Key format: "ocr" for built-in OCR, "whisper" for Whisper, "custom:name" for custom.
    backends: HashMap<String, Arc<dyn AnalysisBackend>>,
}

impl AnalysisManager {
    /// Create a new empty manager.
    pub fn new() -> Self {
        Self {
            backends: HashMap::new(),
        }
    }

    /// Create a manager with default backends auto-registered.
    pub fn with_defaults() -> Self {
        let mut manager = Self::new();
        manager.register_ocr_backends();
        manager.register_whisper(None);
        manager
    }

    /// Register a backend.
    pub fn register(&mut self, key: &str, backend: Arc<dyn AnalysisBackend>) {
        self.backends.insert(key.to_string(), backend);
    }

    /// Register built-in OCR backends (currently just Tesseract as default).
    pub fn register_ocr_backends(&mut self) {
        let tesseract = OcrAnalysisAdapter::new(TesseractBackend::new());
        self.backends.insert("ocr".to_string(), Arc::new(tesseract));

        // Also register with specific backend name for explicit selection
        let tesseract2 = OcrAnalysisAdapter::new(TesseractBackend::new());
        self.backends
            .insert("ocr:tesseract".to_string(), Arc::new(tesseract2));

        // Register other OCR backends if available
        #[cfg(feature = "ocr-ocrs")]
        {
            use crate::ocr::OcrsBackend;
            let ocrs = OcrAnalysisAdapter::new(OcrsBackend::new());
            self.backends.insert("ocr:ocrs".to_string(), Arc::new(ocrs));
        }

        #[cfg(feature = "ocr-paddle")]
        {
            use crate::ocr::PaddleBackend;
            let paddle = OcrAnalysisAdapter::new(PaddleBackend::new());
            self.backends
                .insert("ocr:paddleocr".to_string(), Arc::new(paddle));
        }

        // DeepSeek OCR
        use crate::ocr::DeepSeekBackend;
        let deepseek = OcrAnalysisAdapter::new(DeepSeekBackend::new());
        self.backends
            .insert("ocr:deepseek".to_string(), Arc::new(deepseek));
    }

    /// Register Whisper backend.
    pub fn register_whisper(&mut self, config: Option<WhisperConfig>) {
        let backend = config.map(WhisperBackend::with_config).unwrap_or_default();
        self.backends
            .insert("whisper".to_string(), Arc::new(backend));
    }

    /// Register a custom backend.
    /// Backends are registered under "custom:{name}" prefix and looked up
    /// via get_backends_for() which checks both "custom:{name}" and plain "{name}".
    pub fn register_custom(&mut self, name: &str, config: CustomAnalysisConfig) {
        let backend = CustomBackend::new(name.to_string(), config);
        self.backends
            .insert(format!("custom:{}", name), Arc::new(backend));
    }

    /// Register custom backends from analysis config.
    pub fn register_customs_from_config(
        &mut self,
        methods: &HashMap<String, foia::config::AnalysisMethodConfig>,
    ) {
        for (name, method_config) in methods {
            // Skip built-in methods
            if name == "ocr" || name == "whisper" {
                continue;
            }

            // Create custom backend config
            if let Some(ref command) = method_config.command {
                let custom_config = CustomAnalysisConfig {
                    command: command.clone(),
                    args: method_config.args.clone(),
                    mimetypes: method_config.mimetypes.clone(),
                    granularity: method_config.granularity.clone(),
                    stdout: method_config.stdout,
                    output_file: method_config.output_file.clone(),
                    ..Default::default()
                };
                let backend = CustomBackend::new(name.clone(), custom_config);
                self.backends.insert(name.clone(), Arc::new(backend));
            }
        }
    }

    /// Get a backend by key.
    pub fn get(&self, key: &str) -> Option<Arc<dyn AnalysisBackend>> {
        self.backends.get(key).cloned()
    }

    /// Get backends for the specified methods and mimetype.
    ///
    /// Returns backends that:
    /// 1. Match one of the requested methods
    /// 2. Support the given mimetype
    /// 3. Are available (dependencies installed)
    pub fn get_backends_for(
        &self,
        methods: &[String],
        mimetype: &str,
    ) -> Vec<Arc<dyn AnalysisBackend>> {
        let mut result = Vec::new();

        for method in methods {
            let method_lower = method.to_lowercase();

            // Handle "ocr" as matching any ocr:* backend (use default)
            if method_lower == "ocr" {
                if let Some(backend) = self.backends.get("ocr") {
                    if backend.supports_mimetype(mimetype) && backend.is_available() {
                        result.push(Arc::clone(backend));
                    }
                }
                continue;
            }

            // Handle "whisper"
            if method_lower == "whisper" {
                if let Some(backend) = self.backends.get("whisper") {
                    if backend.supports_mimetype(mimetype) && backend.is_available() {
                        result.push(Arc::clone(backend));
                    }
                }
                continue;
            }

            // Try exact match first
            if let Some(backend) = self.backends.get(&method_lower) {
                if backend.supports_mimetype(mimetype) && backend.is_available() {
                    result.push(Arc::clone(backend));
                    continue;
                }
            }

            // Try with custom: prefix
            if let Some(backend) = self.backends.get(&format!("custom:{}", method_lower)) {
                if backend.supports_mimetype(mimetype) && backend.is_available() {
                    result.push(Arc::clone(backend));
                    continue;
                }
            }

            // Try with ocr: prefix
            if let Some(backend) = self.backends.get(&format!("ocr:{}", method_lower)) {
                if backend.supports_mimetype(mimetype) && backend.is_available() {
                    result.push(Arc::clone(backend));
                }
            }
        }

        result
    }

    /// Get all page-level backends from a list.
    pub fn filter_page_level(
        backends: &[Arc<dyn AnalysisBackend>],
    ) -> Vec<Arc<dyn AnalysisBackend>> {
        backends
            .iter()
            .filter(|b| b.granularity() == AnalysisGranularity::Page)
            .cloned()
            .collect()
    }

    /// Get all document-level backends from a list.
    pub fn filter_document_level(
        backends: &[Arc<dyn AnalysisBackend>],
    ) -> Vec<Arc<dyn AnalysisBackend>> {
        backends
            .iter()
            .filter(|b| b.granularity() == AnalysisGranularity::Document)
            .cloned()
            .collect()
    }

    /// List all registered backend keys.
    pub fn list_backends(&self) -> Vec<&str> {
        self.backends.keys().map(|s| s.as_str()).collect()
    }

    /// List available (installed) backends.
    pub fn list_available(&self) -> Vec<(&str, &dyn AnalysisBackend)> {
        self.backends
            .iter()
            .filter(|(_, b)| b.is_available())
            .map(|(k, b)| (k.as_str(), b.as_ref()))
            .collect()
    }

    /// Check if a method name is valid (registered).
    pub fn is_valid_method(&self, method: &str) -> bool {
        let method_lower = method.to_lowercase();
        self.backends.contains_key(&method_lower)
            || self
                .backends
                .contains_key(&format!("custom:{}", method_lower))
            || self.backends.contains_key(&format!("ocr:{}", method_lower))
    }
}

impl Default for AnalysisManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_creation() {
        let manager = AnalysisManager::new();
        assert!(manager.backends.is_empty());
    }

    #[test]
    fn test_manager_with_defaults() {
        let manager = AnalysisManager::with_defaults();
        // Should have at least ocr and whisper registered
        assert!(manager.backends.contains_key("ocr"));
        assert!(manager.backends.contains_key("whisper"));
    }

    #[test]
    fn test_get_backends_for_pdf() {
        let manager = AnalysisManager::with_defaults();
        let backends = manager.get_backends_for(&["ocr".to_string()], "application/pdf");
        // OCR supports PDF, but may not be available in test environment
        // Just check it doesn't panic
        assert!(backends.len() <= 1);
    }

    #[test]
    fn test_get_backends_for_audio() {
        let manager = AnalysisManager::with_defaults();
        let _backends = manager.get_backends_for(&["whisper".to_string()], "audio/mp3");
        // Whisper may not be available, but should be in the list if it supports audio
        // The backend supports the mimetype, availability is separate
    }

    #[test]
    fn test_filter_by_granularity() {
        let manager = AnalysisManager::with_defaults();

        // Get all backends for a generic test
        let all_backends: Vec<Arc<dyn AnalysisBackend>> =
            manager.backends.values().cloned().collect();

        let page_level = AnalysisManager::filter_page_level(&all_backends);
        let doc_level = AnalysisManager::filter_document_level(&all_backends);

        // OCR should be page-level, Whisper should be document-level
        for b in &page_level {
            assert_eq!(b.granularity(), AnalysisGranularity::Page);
        }
        for b in &doc_level {
            assert_eq!(b.granularity(), AnalysisGranularity::Document);
        }
    }
}
