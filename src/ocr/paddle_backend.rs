//! PaddleOCR backend implementation.
//!
//! Uses paddle-ocr-rs for OCR via ONNX Runtime.
//! Supports CPU and GPU acceleration through ONNX Runtime.
//!
//! Models are automatically downloaded on first use from:
//! https://github.com/RapidAI/RapidOCR

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use tempfile::TempDir;

use paddle_ocr_rs::ocr_lite::OcrLite;

use super::backend::{OcrBackend, OcrBackendType, OcrConfig, OcrError, OcrResult};
use super::model_utils::{ensure_model_file, ModelDirConfig, ModelSpec};

/// Global cached OcrLite instance (initialized once, reused for all OCR calls).
/// OcrLite is Send+Sync, wrapped in Mutex since detect_from_path needs &mut self.
static OCR_ENGINE: OnceLock<Mutex<OcrLite>> = OnceLock::new();

/// Model directory configuration for PaddleOCR.
const MODEL_CONFIG: ModelDirConfig = ModelDirConfig {
    subdir: "paddle-ocr",
    required_files: &[DET_MODEL_NAME, REC_MODEL_NAME],
};

/// Expected model filenames (standardized)
const DET_MODEL_NAME: &str = "ch_PP-OCRv4_det_infer.onnx";
const REC_MODEL_NAME: &str = "ch_PP-OCRv4_rec_infer.onnx";
const CLS_MODEL_NAME: &str = "ch_ppocr_mobile_v2.0_cls_infer.onnx";

/// Model specifications for downloading.
const DET_MODEL: ModelSpec = ModelSpec {
    url: "https://huggingface.co/SWHL/RapidOCR/resolve/main/PP-OCRv4/ch_PP-OCRv4_det_infer.onnx",
    filename: DET_MODEL_NAME,
    size_hint: "4 MB",
};

const REC_MODEL: ModelSpec = ModelSpec {
    url: "https://huggingface.co/SWHL/RapidOCR/resolve/main/PP-OCRv4/ch_PP-OCRv4_rec_infer.onnx",
    filename: REC_MODEL_NAME,
    size_hint: "10 MB",
};

const CLS_MODEL: ModelSpec = ModelSpec {
    url: "https://www.modelscope.cn/models/RapidAI/RapidOCR/resolve/v3.4.0/onnx/PP-OCRv4/cls/ch_ppocr_mobile_v2.0_cls_infer.onnx",
    filename: CLS_MODEL_NAME,
    size_hint: "1 MB",
};

/// PaddleOCR backend via ONNX Runtime.
pub struct PaddleBackend {
    config: OcrConfig,
}

impl PaddleBackend {
    /// Create a new PaddleOCR backend with default configuration.
    pub fn new() -> Self {
        Self {
            config: OcrConfig::default(),
        }
    }

    /// Create a new PaddleOCR backend with custom configuration.
    #[allow(dead_code)]
    pub fn with_config(config: OcrConfig) -> Self {
        Self { config }
    }

    /// Find the model directory, checking config path and standard locations.
    fn find_model_dir(&self) -> Option<PathBuf> {
        // Check config path first
        if let Some(ref path) = self.config.model_path {
            if MODEL_CONFIG.has_required_files(path) {
                return Some(path.clone());
            }
        }

        // Check standard locations
        for candidate in MODEL_CONFIG.candidate_dirs() {
            // Check for our standardized v4 model names first
            if MODEL_CONFIG.has_required_files(&candidate) {
                return Some(candidate);
            }
            // Also check legacy naming patterns
            for version in ["v5", "v4", "v3"] {
                let det_model = format!("ch_PP-OCR{}_mobile_det.onnx", version);
                if candidate.join(&det_model).exists() {
                    return Some(candidate);
                }
            }
        }

        None
    }

    /// Ensure models are downloaded, downloading them if necessary.
    fn ensure_models(&self) -> Result<PathBuf, OcrError> {
        if let Some(dir) = self.find_model_dir() {
            return Ok(dir);
        }

        let model_dir = MODEL_CONFIG.default_dir();
        std::fs::create_dir_all(&model_dir).map_err(OcrError::Io)?;

        ensure_model_file(&DET_MODEL, &model_dir)?;
        ensure_model_file(&REC_MODEL, &model_dir)?;
        ensure_model_file(&CLS_MODEL, &model_dir)?;

        Ok(model_dir)
    }

    /// Find model files in the model directory.
    fn find_models(&self) -> Result<(String, String, String), OcrError> {
        let model_dir = self.ensure_models()?;

        // Check for our standardized v4 model names first
        let det_model = model_dir.join(DET_MODEL_NAME);
        let rec_model = model_dir.join(REC_MODEL_NAME);
        let cls_model = model_dir.join(CLS_MODEL_NAME);

        if det_model.exists() && rec_model.exists() {
            return Ok((
                det_model.to_string_lossy().to_string(),
                cls_model.to_string_lossy().to_string(),
                rec_model.to_string_lossy().to_string(),
            ));
        }

        // Try legacy naming patterns
        for version in ["v5", "v4", "v3"] {
            let det_model = model_dir.join(format!("ch_PP-OCR{}_mobile_det.onnx", version));
            let rec_model = model_dir.join(format!("ch_PP-OCR{}_rec_mobile_infer.onnx", version));
            let cls_model = model_dir.join("ch_ppocr_mobile_v2.0_cls_infer.onnx");

            if det_model.exists() && rec_model.exists() {
                return Ok((
                    det_model.to_string_lossy().to_string(),
                    cls_model.to_string_lossy().to_string(),
                    rec_model.to_string_lossy().to_string(),
                ));
            }
        }

        Err(OcrError::ModelNotFound(
            "Could not find matching PaddleOCR model files".to_string(),
        ))
    }

    /// Get or initialize the cached OCR engine.
    fn get_or_init_engine(&self) -> Result<&'static Mutex<OcrLite>, OcrError> {
        // Try to get existing engine first
        if let Some(engine) = OCR_ENGINE.get() {
            return Ok(engine);
        }

        // Initialize the engine (only happens once)
        let (det_model, cls_model, rec_model) = self.find_models()?;

        let mut ocr = OcrLite::new();
        let num_threads = 4; // Use more threads for better performance
        ocr.init_models(&det_model, &cls_model, &rec_model, num_threads)
            .map_err(|e| OcrError::OcrFailed(format!("Failed to init PaddleOCR: {}", e)))?;

        // Store in global cache - if another thread beat us, that's fine
        let _ = OCR_ENGINE.set(Mutex::new(ocr));

        // Return the engine (either ours or the one that won the race)
        OCR_ENGINE
            .get()
            .ok_or_else(|| OcrError::OcrFailed("Failed to cache OCR engine".to_string()))
    }

    /// Run OCR on an image path.
    fn run_paddle(&self, image_path: &Path) -> Result<String, OcrError> {
        let engine_mutex = self.get_or_init_engine()?;
        let mut ocr = engine_mutex
            .lock()
            .map_err(|e| OcrError::OcrFailed(format!("Failed to lock OCR engine: {}", e)))?;

        // Run detection from path
        let result = ocr
            .detect_from_path(
                image_path.to_str().unwrap_or(""),
                50,    // padding
                1024,  // max side length
                0.5,   // box score threshold
                0.3,   // unclip ratio
                1.6,   // box threshold
                false, // do angle
                false, // most angle
            )
            .map_err(|e| OcrError::OcrFailed(format!("PaddleOCR detection failed: {}", e)))?;

        // Extract text from results
        let texts: Vec<String> = result
            .text_blocks
            .iter()
            .map(|block| block.text.clone())
            .collect();

        Ok(texts.join("\n"))
    }

    /// Convert a PDF page to an image.
    fn pdf_page_to_image(
        &self,
        pdf_path: &Path,
        page: u32,
        output_dir: &Path,
    ) -> Result<std::path::PathBuf, OcrError> {
        let page_str = page.to_string();
        let output_prefix = output_dir.join("page");

        let status = Command::new("pdftoppm")
            .args(["-png", "-r", "300", "-f", &page_str, "-l", &page_str])
            .arg(pdf_path)
            .arg(&output_prefix)
            .status();

        match status {
            Ok(s) if s.success() => self.find_page_image(output_dir, page).ok_or_else(|| {
                OcrError::OcrFailed(format!("No image generated for page {}", page))
            }),
            Ok(_) => Err(OcrError::OcrFailed(
                "pdftoppm failed to convert PDF page".to_string(),
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(OcrError::BackendNotAvailable(
                    "pdftoppm not found (install poppler-utils)".to_string(),
                ))
            }
            Err(e) => Err(OcrError::Io(e)),
        }
    }

    /// Find the image file for a specific page number.
    fn find_page_image(&self, temp_path: &Path, page_num: u32) -> Option<std::path::PathBuf> {
        for digits in [2, 3, 4] {
            let filename = format!("page-{:0width$}.png", page_num, width = digits);
            let path = temp_path.join(&filename);
            if path.exists() {
                return Some(path);
            }
        }
        None
    }
}

impl Default for PaddleBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OcrBackend for PaddleBackend {
    fn backend_type(&self) -> OcrBackendType {
        OcrBackendType::PaddleOcr
    }

    fn is_available(&self) -> bool {
        // Always available - models will be auto-downloaded on first use
        true
    }

    fn availability_hint(&self) -> String {
        match self.find_model_dir() {
            Some(path) => format!("PaddleOCR models found at {:?}", path),
            None => {
                format!(
                    "PaddleOCR models will be auto-downloaded on first use (~15 MB total) to {:?}",
                    MODEL_CONFIG.default_dir()
                )
            }
        }
    }

    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let text = self.run_paddle(image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::PaddleOcr,
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }

    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        let start = Instant::now();

        let temp_dir = TempDir::new()?;
        let image_path = self.pdf_page_to_image(pdf_path, page, temp_dir.path())?;

        let text = self.run_paddle(&image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::PaddleOcr,
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }
}
