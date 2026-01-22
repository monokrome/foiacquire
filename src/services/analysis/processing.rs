//! OCR processing helper functions.

use crate::models::{Document, DocumentPage, PageOcrStatus};
use crate::ocr::TextExtractor;
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
                Some(&pdf_text),
                None, // no confidence score for pdftotext
                None, // no processing time tracked
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
pub fn ocr_document_page(
    page: &DocumentPage,
    doc_repo: &DieselDocumentRepository,
    handle: &tokio::runtime::Handle,
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

    // Run OCR on this page
    let mut updated_page = page.clone();
    let mut improved = false;

    match extractor.ocr_pdf_page(&version.file_path, page.page_number) {
        Ok(ocr_text) => {
            let ocr_chars = ocr_text.chars().filter(|c| !c.is_whitespace()).count();
            let pdf_chars = page
                .pdf_text
                .as_ref()
                .map(|t| t.chars().filter(|c| !c.is_whitespace()).count())
                .unwrap_or(0);

            // Track if OCR provided more content (for reporting)
            improved = ocr_chars > pdf_chars + (pdf_chars / 5);

            updated_page.ocr_text = Some(ocr_text.clone());
            updated_page.ocr_status = PageOcrStatus::OcrComplete;

            // Prefer OCR over extracted text (unless OCR is empty)
            updated_page.final_text = if ocr_chars > 0 {
                Some(ocr_text.clone())
            } else {
                page.pdf_text.clone()
            };

            // Store tesseract result in page_ocr_results for comparison
            let _ = handle.block_on(doc_repo.store_page_ocr_result(
                page.id,
                "tesseract",
                Some(&ocr_text),
                None, // TODO: could extract confidence from tesseract
                None, // TODO: could track processing time
            ));
        }
        Err(e) => {
            tracing::debug!(
                "OCR failed for page {}, using PDF text: {}",
                page.page_number,
                e
            );
            // Mark as failed but still set final_text to PDF text so document can be finalized
            updated_page.ocr_status = PageOcrStatus::Failed;
            updated_page.final_text = page.pdf_text.clone();
        }
    };

    handle.block_on(doc_repo.save_page(&updated_page))?;

    // Check if all pages for this document are now complete, and if so, finalize it
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
