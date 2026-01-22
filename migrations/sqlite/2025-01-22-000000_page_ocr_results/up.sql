-- Multi-backend OCR results storage
-- Stores OCR results from different backends (tesseract, gemini, groq, etc.)
-- for comparison and quality selection

CREATE TABLE page_ocr_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id INTEGER NOT NULL,
    backend TEXT NOT NULL,                -- tesseract, gemini, groq, pdftotext, etc.
    text TEXT,                            -- Extracted text (NULL if failed)
    confidence REAL,                      -- Backend-reported confidence (0.0-1.0)
    quality_score REAL,                   -- Computed quality score for comparison
    char_count INTEGER,                   -- Character count for quick comparison
    word_count INTEGER,                   -- Word count for quick comparison
    processing_time_ms INTEGER,           -- How long OCR took
    error_message TEXT,                   -- Error message if failed
    created_at TEXT NOT NULL,
    FOREIGN KEY (page_id) REFERENCES document_pages(id) ON DELETE CASCADE,
    UNIQUE(page_id, backend)
);

-- Indexes for efficient queries
CREATE INDEX idx_page_ocr_results_page ON page_ocr_results(page_id);
CREATE INDEX idx_page_ocr_results_backend ON page_ocr_results(backend);
CREATE INDEX idx_page_ocr_results_quality ON page_ocr_results(quality_score DESC);
CREATE INDEX idx_page_ocr_results_page_quality ON page_ocr_results(page_id, quality_score DESC);

-- Migrate existing OCR data from document_pages.ocr_text
-- Historical OCR was done with tesseract, so we label it as such
INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'tesseract',
    ocr_text,
    LENGTH(ocr_text),
    LENGTH(ocr_text) - LENGTH(REPLACE(ocr_text, ' ', '')) + 1,
    COALESCE(updated_at, created_at)
FROM document_pages
WHERE ocr_text IS NOT NULL AND ocr_text != '';

-- Also migrate pdftotext results (pdf_text column)
INSERT INTO page_ocr_results (page_id, backend, text, char_count, word_count, created_at)
SELECT
    id,
    'pdftotext',
    pdf_text,
    LENGTH(pdf_text),
    LENGTH(pdf_text) - LENGTH(REPLACE(pdf_text, ' ', '')) + 1,
    COALESCE(updated_at, created_at)
FROM document_pages
WHERE pdf_text IS NOT NULL AND pdf_text != '';
