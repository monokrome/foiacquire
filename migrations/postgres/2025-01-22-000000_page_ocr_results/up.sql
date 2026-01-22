-- Multi-backend OCR results storage
-- Stores OCR results from different backends (tesseract, gemini, groq, etc.)
-- for comparison and quality selection

CREATE TABLE page_ocr_results (
    id SERIAL PRIMARY KEY,
    page_id INTEGER NOT NULL REFERENCES document_pages(id) ON DELETE CASCADE,
    backend TEXT NOT NULL,                -- tesseract, gemini, groq, pdftotext, etc.
    text TEXT,                            -- Extracted text (NULL if failed)
    confidence REAL,                      -- Backend-reported confidence (0.0-1.0)
    quality_score REAL,                   -- Computed quality score for comparison
    char_count INTEGER,                   -- Character count for quick comparison
    word_count INTEGER,                   -- Word count for quick comparison
    processing_time_ms INTEGER,           -- How long OCR took
    error_message TEXT,                   -- Error message if failed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(page_id, backend)
);

-- Indexes for efficient queries
CREATE INDEX idx_page_ocr_results_page ON page_ocr_results(page_id);
CREATE INDEX idx_page_ocr_results_backend ON page_ocr_results(backend);
CREATE INDEX idx_page_ocr_results_quality ON page_ocr_results(quality_score DESC NULLS LAST);
CREATE INDEX idx_page_ocr_results_page_quality ON page_ocr_results(page_id, quality_score DESC NULLS LAST);

-- Migrate existing OCR data from document_pages.ocr_text
-- Historical OCR was done with tesseract, so we label it as such
INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'tesseract',
    ocr_text,
    LENGTH(ocr_text),
    array_length(regexp_split_to_array(ocr_text, '\s+'), 1),
    COALESCE(updated_at::timestamptz, created_at::timestamptz, NOW())
FROM document_pages
WHERE ocr_text IS NOT NULL AND ocr_text != '';

-- Also migrate pdftotext results (pdf_text column)
INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'pdftotext',
    pdf_text,
    LENGTH(pdf_text),
    array_length(regexp_split_to_array(pdf_text, '\s+'), 1),
    COALESCE(updated_at::timestamptz, created_at::timestamptz, NOW())
FROM document_pages
WHERE pdf_text IS NOT NULL AND pdf_text != '';
