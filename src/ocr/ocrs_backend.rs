//! OCRS OCR backend implementation.
//!
//! Uses the ocrs crate for pure-Rust OCR without external dependencies.
//! This is a lightweight, CPU-based OCR engine.
//!
//! Models are automatically downloaded on first use from:
//! https://ocrs-models.s3-accelerate.amazonaws.com/

use std::path::Path;
use std::sync::OnceLock;
use std::time::Instant;
use tempfile::TempDir;

use super::backend::{OcrBackend, OcrBackendType, OcrConfig, OcrError, OcrResult};
use super::model_utils::{
    build_ocr_result, ensure_models_present, find_model_dir, model_availability_hint,
    ModelDirConfig, ModelSpec,
};
use super::pdf_utils;

/// Global cached OcrEngine instance (initialized once, reused for all OCR calls).
/// OcrEngine is Send+Sync and its methods take &self, so no Mutex needed.
static OCR_ENGINE: OnceLock<ocrs::OcrEngine> = OnceLock::new();

/// Model directory configuration for OCRS.
const MODEL_CONFIG: ModelDirConfig = ModelDirConfig {
    subdir: "ocrs",
    required_files: &["text-detection.rten", "text-recognition.rten"],
};

/// Model specifications for downloading.
const DETECTION_MODEL: ModelSpec = ModelSpec {
    url: "https://ocrs-models.s3-accelerate.amazonaws.com/text-detection.rten",
    filename: "text-detection.rten",
    size_hint: "2.5 MB",
};

const RECOGNITION_MODEL: ModelSpec = ModelSpec {
    url: "https://ocrs-models.s3-accelerate.amazonaws.com/text-recognition.rten",
    filename: "text-recognition.rten",
    size_hint: "10 MB",
};

/// OCRS OCR backend (pure Rust).
pub struct OcrsBackend {
    config: OcrConfig,
}

impl OcrsBackend {
    /// Create a new OCRS backend with default configuration.
    pub fn new() -> Self {
        Self {
            config: OcrConfig::default(),
        }
    }

    /// Create a new OCRS backend with custom configuration.
    #[allow(dead_code)]
    pub fn with_config(config: OcrConfig) -> Self {
        Self { config }
    }

    /// Ensure models are downloaded, downloading them if necessary.
    fn ensure_models(&self) -> Result<std::path::PathBuf, OcrError> {
        ensure_models_present(
            self.config.model_path.as_ref(),
            &MODEL_CONFIG,
            &[&DETECTION_MODEL, &RECOGNITION_MODEL],
        )
    }

    /// Get or initialize the cached OCR engine.
    fn get_or_init_engine(&self) -> Result<&'static ocrs::OcrEngine, OcrError> {
        // Try to get existing engine first
        if let Some(engine) = OCR_ENGINE.get() {
            return Ok(engine);
        }

        // Initialize the engine (only happens once)
        let model_dir = self.ensure_models()?;

        let detection_path = model_dir.join("text-detection.rten");
        let recognition_path = model_dir.join("text-recognition.rten");

        // Load models
        let detection_model = rten::Model::load_file(&detection_path)
            .map_err(|e| OcrError::OcrFailed(format!("Failed to load detection model: {}", e)))?;
        let recognition_model = rten::Model::load_file(&recognition_path)
            .map_err(|e| OcrError::OcrFailed(format!("Failed to load recognition model: {}", e)))?;

        // Create engine
        let engine = ocrs::OcrEngine::new(ocrs::OcrEngineParams {
            detection_model: Some(detection_model),
            recognition_model: Some(recognition_model),
            ..Default::default()
        })
        .map_err(|e| OcrError::OcrFailed(format!("Failed to create OCR engine: {}", e)))?;

        // Store in global cache - if another thread beat us, that's fine
        let _ = OCR_ENGINE.set(engine);

        // Return the engine (either ours or the one that won the race)
        OCR_ENGINE
            .get()
            .ok_or_else(|| OcrError::OcrFailed("Failed to cache OCR engine".to_string()))
    }

    /// Run OCR on an image.
    fn run_ocrs(&self, image_path: &Path) -> Result<String, OcrError> {
        let engine = self.get_or_init_engine()?;

        // Load image
        let img = image::open(image_path)
            .map_err(|e| OcrError::ImageError(format!("Failed to load image: {}", e)))?;
        let rgb_img = img.to_rgb8();

        // Convert to ocrs format
        let (width, height) = rgb_img.dimensions();

        // Create image source - from_bytes takes (bytes, (width, height))
        let img_source = ocrs::ImageSource::from_bytes(rgb_img.as_raw(), (width, height))
            .map_err(|e| OcrError::ImageError(format!("Failed to convert image: {}", e)))?;

        // Use the convenience API to get all text
        let input = engine
            .prepare_input(img_source)
            .map_err(|e| OcrError::OcrFailed(format!("Failed to prepare input: {}", e)))?;

        let text = engine
            .get_text(&input)
            .map_err(|e| OcrError::OcrFailed(format!("Failed to extract text: {}", e)))?;

        Ok(text)
    }
}

impl Default for OcrsBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OcrBackend for OcrsBackend {
    fn backend_type(&self) -> OcrBackendType {
        OcrBackendType::Ocrs
    }

    fn is_available(&self) -> bool {
        // Always available - models will be auto-downloaded on first use
        true
    }

    fn availability_hint(&self) -> String {
        model_availability_hint(
            self.config.model_path.as_ref(),
            &MODEL_CONFIG,
            "OCRS",
            "12 MB",
        )
    }

    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let text = self.run_ocrs(image_path)?;
        Ok(build_ocr_result(text, OcrBackendType::Ocrs, None, start))
    }

    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let temp_dir = TempDir::new()?;
        let image_path = pdf_utils::pdf_page_to_image(pdf_path, page, temp_dir.path())?;
        let text = self.run_ocrs(&image_path)?;
        Ok(build_ocr_result(text, OcrBackendType::Ocrs, None, start))
    }
}
