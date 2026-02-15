//! Shared PDF-to-image conversion utilities for OCR backends.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

use super::backend::OcrError;
use super::model_utils::PDFTOPPM_NOT_FOUND;

/// Convert a PDF page to an image using pdftoppm.
///
/// Uses 300 DPI PNG output for optimal OCR quality.
pub fn pdf_page_to_image(
    pdf_path: &Path,
    page: u32,
    output_dir: &Path,
) -> Result<PathBuf, OcrError> {
    let page_str = page.to_string();
    let output_prefix = output_dir.join("page");

    let status = Command::new("pdftoppm")
        .args(["-png", "-r", "300", "-f", &page_str, "-l", &page_str])
        .arg(pdf_path)
        .arg(&output_prefix)
        .status();

    match status {
        Ok(s) if s.success() => find_page_image(output_dir, page)
            .ok_or_else(|| OcrError::OcrFailed(format!("No image generated for page {}", page))),
        Ok(_) => Err(OcrError::OcrFailed(
            "pdftoppm failed to convert PDF page".to_string(),
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(OcrError::BackendNotAvailable(
            PDFTOPPM_NOT_FOUND.to_string(),
        )),
        Err(e) => Err(OcrError::Io(e)),
    }
}

/// Find the image file for a specific page number.
///
/// pdftoppm names files like page-01.png, page-02.png, etc.
/// The padding width varies based on total page count.
pub fn find_page_image(temp_path: &Path, page_num: u32) -> Option<PathBuf> {
    for digits in [2, 3, 4] {
        let filename = format!("page-{:0width$}.png", page_num, width = digits);
        let path = temp_path.join(&filename);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

/// Compute SHA-256 hash of a file.
///
/// Returns hex-encoded hash string.
pub fn compute_file_hash(path: &Path) -> Result<String, OcrError> {
    let data = fs::read(path).map_err(OcrError::Io)?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_find_page_image_not_found() {
        let temp = TempDir::new().unwrap();
        assert!(find_page_image(temp.path(), 1).is_none());
    }

    #[test]
    fn test_find_page_image_with_2_digit_padding() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("page-01.png");
        std::fs::write(&path, b"fake png").unwrap();

        let found = find_page_image(temp.path(), 1);
        assert!(found.is_some());
        assert_eq!(found.unwrap(), path);
    }

    #[test]
    fn test_find_page_image_with_3_digit_padding() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("page-001.png");
        std::fs::write(&path, b"fake png").unwrap();

        let found = find_page_image(temp.path(), 1);
        assert!(found.is_some());
        assert_eq!(found.unwrap(), path);
    }
}
