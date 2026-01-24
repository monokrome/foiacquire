//! OCR processing helper functions.

use crate::config::OcrConfig;
use crate::models::{Document, DocumentPage, PageOcrStatus};
use crate::ocr::{FallbackOcrBackend, OcrBackend, OcrConfig as OcrBackendConfig, TextExtractor};
use crate::repository::DieselDocumentRepository;

use super::types::PageOcrResult;

/// Extract text from a document per-page using pdftotext.
/// This function runs in a blocking context and uses the runtime handle to call async methods.
pub fn extract_document_text_per_page(
    doc: &Document,
    doc_repo: &DieselDocumentRepository,
    handle: &tokio::runtime::Handle,
) -> anyhow::Result<usize> {
    let extractor = TextExtractor::new();

    let version = doc
        .current_version()
        .ok_or_else(|| anyhow::anyhow!("Document has no versions"))?;

    // Only process PDFs with per-page extraction
    if version.mime_type != "application/pdf" {
        // For non-PDFs, use the old extraction method
        let result = extractor.extract(&version.file_path, &version.mime_type)?;

        // Create a single "page" for non-PDF documents
        let mut page = DocumentPage::new(doc.id.clone(), version.id, 1);
        page.pdf_text = Some(result.text.clone());
        page.final_text = Some(result.text);
        page.ocr_status = PageOcrStatus::OcrComplete;
        handle.block_on(doc_repo.save_page(&page))?;

        // Cache page count (1 for non-PDFs)
        let _ = handle.block_on(doc_repo.set_version_page_count(version.id, 1));

        // Non-PDFs are complete immediately - finalize the document
        let _ = handle.block_on(doc_repo.finalize_document(&doc.id));

        return Ok(1);
    }

    // Get page count (use cached value if available)
    let page_count = version.page_count.unwrap_or_else(|| {
        tracing::debug!(
            "Getting page count for document {}: {}",
            doc.id,
            version.file_path.display()
        );
        let count = extractor
            .get_pdf_page_count(&version.file_path)
            .unwrap_or(1);
        tracing::debug!("Document {} has {} pages", doc.id, count);
        count
    });

    // Cache page count if not already cached
    if version.page_count.is_none() {
        let _ = handle.block_on(doc_repo.set_version_page_count(version.id, page_count));
    }

    // Delete any existing pages for this document version (in case of re-processing)
    handle.block_on(doc_repo.delete_pages(&doc.id, version.id as i32))?;

    let mut pages_created = 0;

    for page_num in 1..=page_count {
        tracing::debug!(
            "Processing page {}/{} of document {}",
            page_num,
            page_count,
            doc.id
        );
        // Extract text using pdftotext
        let pdf_text = extractor
            .extract_pdf_page_text(&version.file_path, page_num)
            .unwrap_or_default();

        let mut page = DocumentPage::new(doc.id.clone(), version.id, page_num);
        page.pdf_text = Some(pdf_text.clone());
        page.ocr_status = PageOcrStatus::TextExtracted;

        tracing::debug!(
            "Saving page {}/{} to database for document {}",
            page_num,
            page_count,
            doc.id
        );
        let page_id = handle.block_on(doc_repo.save_page(&page))?;

        // Store pdftotext result in page_ocr_results for comparison
        if !pdf_text.is_empty() {
            let _ = handle.block_on(doc_repo.store_page_ocr_result(
                page_id,
                "pdftotext",
                None, // no model for pdftotext
                Some(&pdf_text),
                None, // no confidence score for pdftotext
                None, // no processing time tracked
                None, // no image hash for pdftotext (text extraction)
            ));
        }

        pages_created += 1;
    }

    Ok(pages_created)
}

/// Run OCR on a page and compare with existing text.
/// If all pages for this document are now complete, the document is finalized
/// (status set to OcrComplete, combined text saved).
/// This function runs in a blocking context and uses the runtime handle to call async methods.
///
/// Uses the default tesseract backend. For configurable fallback chains, use
/// `ocr_document_page_with_config`.
#[allow(dead_code)]
pub fn ocr_document_page(
    page: &DocumentPage,
    doc_repo: &DieselDocumentRepository,
    handle: &tokio::runtime::Handle,
) -> anyhow::Result<PageOcrResult> {
    ocr_document_page_with_config(page, doc_repo, handle, &OcrConfig::default())
}

/// Run OCR on a page using configured backend entries.
///
/// Each backend entry produces a separate result:
/// - Single backend: runs and stores result
/// - Fallback chain: tries backends in order until one succeeds, stores result
///
/// Example config: `["tesseract", ["groq", "gemini"]]`
/// - Runs tesseract, stores as "tesseract"
/// - Runs groq (falls back to gemini if rate limited), stores as "groq" or "gemini"
pub fn ocr_document_page_with_config(
    page: &DocumentPage,
    doc_repo: &DieselDocumentRepository,
    handle: &tokio::runtime::Handle,
    ocr_config: &OcrConfig,
) -> anyhow::Result<PageOcrResult> {
    let extractor = TextExtractor::new();

    // Get the document to find the file path
    let doc = handle
        .block_on(doc_repo.get(&page.document_id))?
        .ok_or_else(|| anyhow::anyhow!("Document not found"))?;

    let version = doc
        .versions
        .iter()
        .find(|v| v.id == page.version_id)
        .ok_or_else(|| anyhow::anyhow!("Version not found"))?;

    // Compute image hash once for deduplication across all backends
    let image_hash = extractor
        .get_pdf_page_hash(&version.file_path, page.page_number)
        .ok();

    let mut updated_page = page.clone();
    let mut improved = false;
    let mut any_succeeded = false;
    let mut best_text: Option<String> = None;
    let mut best_char_count = 0usize;

    let pdf_chars = page
        .pdf_text
        .as_ref()
        .map(|t| t.chars().filter(|c| !c.is_whitespace()).count())
        .unwrap_or(0);

    // Process each backend entry
    for entry in &ocr_config.backends {
        let backend_names: Vec<&str> = entry.backends();

        // Check for existing result from any backend in this entry
        let existing = if let Some(ref hash) = image_hash {
            backend_names.iter().find_map(|name| {
                handle
                    .block_on(doc_repo.find_ocr_result_by_image_hash(hash, name))
                    .ok()
                    .flatten()
                    .map(|r| (r, name.to_string()))
            })
        } else {
            None
        };

        if let Some((existing_result, backend_name)) = existing {
            // Reuse existing result
            let ocr_text = existing_result.text.clone().unwrap_or_default();
            let ocr_chars = ocr_text.chars().filter(|c| !c.is_whitespace()).count();

            // Store reference for this page
            let _ = handle.block_on(doc_repo.store_page_ocr_result(
                page.id,
                &backend_name,
                existing_result.model.as_deref(),
                Some(&ocr_text),
                existing_result.confidence,
                existing_result.processing_time_ms,
                image_hash.as_deref(),
            ));

            tracing::debug!(
                "Reused existing {} result for page {} (hash match)",
                backend_name,
                page.page_number
            );

            any_succeeded = true;
            if ocr_chars > best_char_count {
                best_char_count = ocr_chars;
                best_text = Some(ocr_text);
            }
        } else {
            // Run OCR with this entry (single backend or fallback chain)
            let fallback =
                FallbackOcrBackend::from_names(&backend_names, OcrBackendConfig::default());

            match fallback.ocr_pdf_page(&version.file_path, page.page_number) {
                Ok(result) => {
                    let ocr_text = result.text;
                    let backend_name = result.backend.as_str();
                    let ocr_chars = ocr_text.chars().filter(|c| !c.is_whitespace()).count();

                    // Store result
                    let _ = handle.block_on(doc_repo.store_page_ocr_result(
                        page.id,
                        backend_name,
                        result.model.as_deref(),
                        Some(&ocr_text),
                        result.confidence,
                        Some(result.processing_time_ms as i32),
                        image_hash.as_deref(),
                    ));

                    tracing::debug!(
                        "OCR completed for page {} using {} backend ({} chars)",
                        page.page_number,
                        backend_name,
                        ocr_chars
                    );

                    any_succeeded = true;
                    if ocr_chars > best_char_count {
                        best_char_count = ocr_chars;
                        best_text = Some(ocr_text);
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "OCR entry {:?} failed for page {}: {}",
                        entry,
                        page.page_number,
                        e
                    );
                }
            }
        }
    }

    // Update page with best result
    if let Some(text) = best_text {
        improved = best_char_count > pdf_chars + (pdf_chars / 5);
        updated_page.ocr_text = Some(text.clone());
        updated_page.ocr_status = PageOcrStatus::OcrComplete;
        updated_page.final_text = if best_char_count > 0 {
            Some(text)
        } else {
            page.pdf_text.clone()
        };
    } else if any_succeeded {
        // All results were empty
        updated_page.ocr_status = PageOcrStatus::OcrComplete;
        updated_page.final_text = page.pdf_text.clone();
    } else {
        // All backends failed
        updated_page.ocr_status = PageOcrStatus::Failed;
        updated_page.final_text = page.pdf_text.clone();
    }

    handle.block_on(doc_repo.save_page(&updated_page))?;

    // Check if all pages for this document are now complete
    let mut document_finalized = false;
    if handle
        .block_on(doc_repo.are_all_pages_complete(&page.document_id, page.version_id as i32))?
    {
        let _ = handle.block_on(doc_repo.finalize_document(&page.document_id));
        document_finalized = true;
        tracing::debug!(
            "Document {} finalized after page {} completed",
            page.document_id,
            page.page_number
        );
    }

    Ok(PageOcrResult {
        improved,
        document_finalized,
    })
}
