-- Initial schema for foiacquire (combines all tables from v13)

-- Document storage
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    extracted_text TEXT,
    synopsis TEXT,
    tags TEXT,
    status TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    estimated_date TEXT,
    date_confidence TEXT,
    date_source TEXT,
    manual_date TEXT,
    discovery_method TEXT NOT NULL DEFAULT 'import',
    category_id TEXT REFERENCES file_categories(id)
);

CREATE TABLE IF NOT EXISTS document_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    mime_type TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    source_url TEXT,
    original_filename TEXT,
    server_date TEXT,
    page_count INTEGER,
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS storage_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS virtual_files (
    id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    version_id INTEGER NOT NULL,
    archive_path TEXT NOT NULL,
    filename TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    extracted_text TEXT,
    synopsis TEXT,
    tags TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(id),
    FOREIGN KEY (version_id) REFERENCES document_versions(id)
);

CREATE TABLE IF NOT EXISTS document_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    version_id INTEGER NOT NULL,
    page_number INTEGER NOT NULL,
    pdf_text TEXT,
    ocr_text TEXT,
    final_text TEXT,
    ocr_status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(id),
    FOREIGN KEY (version_id) REFERENCES document_versions(id),
    UNIQUE(document_id, version_id, page_number)
);

CREATE TABLE IF NOT EXISTS document_annotations (
    document_id TEXT NOT NULL,
    annotation_type TEXT NOT NULL,
    completed_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    result TEXT,
    error TEXT,
    PRIMARY KEY (document_id, annotation_type),
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS page_ocr_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id INTEGER NOT NULL,
    backend TEXT NOT NULL,
    ocr_text TEXT,
    confidence REAL,
    processing_time_ms INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY (page_id) REFERENCES document_pages(id),
    UNIQUE(page_id, backend)
);

CREATE TABLE IF NOT EXISTS document_counts (
    source_id TEXT PRIMARY KEY,
    count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS file_categories (
    id TEXT PRIMARY KEY,
    description TEXT,
    doc_count INTEGER NOT NULL DEFAULT 0
);

-- Default file categories
INSERT OR IGNORE INTO file_categories (id, description, doc_count) VALUES
    ('documents', 'PDF, Word, text, and email documents', 0),
    ('images', 'Image files (PNG, JPG, GIF, etc.)', 0),
    ('data', 'Spreadsheets, CSV, JSON, and XML files', 0),
    ('archives', 'ZIP, TAR, and other archive files', 0),
    ('markup', 'HTML and XML markup files', 0),
    ('other', 'Other file types', 0);

-- Sources
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_scraped TEXT
);

-- Crawl state
CREATE TABLE IF NOT EXISTS crawl_urls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    source_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'discovered',
    discovery_method TEXT NOT NULL DEFAULT 'seed',
    parent_url TEXT,
    discovery_context TEXT NOT NULL DEFAULT '{}',
    depth INTEGER NOT NULL DEFAULT 0,
    discovered_at TEXT NOT NULL,
    fetched_at TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TEXT,
    etag TEXT,
    last_modified TEXT,
    content_hash TEXT,
    document_id TEXT,
    UNIQUE(source_id, url)
);

CREATE TABLE IF NOT EXISTS crawl_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    url TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT 'GET',
    request_headers TEXT NOT NULL DEFAULT '{}',
    request_at TEXT NOT NULL,
    response_status INTEGER,
    response_headers TEXT NOT NULL DEFAULT '{}',
    response_at TEXT,
    response_size INTEGER,
    duration_ms INTEGER,
    error TEXT,
    was_conditional INTEGER NOT NULL DEFAULT 0,
    was_not_modified INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS crawl_config (
    source_id TEXT PRIMARY KEY,
    config_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Configuration history
CREATE TABLE IF NOT EXISTS configuration_history (
    uuid TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    format TEXT NOT NULL,
    hash TEXT NOT NULL
);

-- Rate limiting
CREATE TABLE IF NOT EXISTS rate_limit_state (
    domain TEXT PRIMARY KEY,
    current_delay_ms INTEGER NOT NULL,
    in_backoff INTEGER NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    rate_limit_hits INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rate_limit_domains (
    domain TEXT PRIMARY KEY,
    current_delay_ms INTEGER NOT NULL,
    last_request_at INTEGER,
    consecutive_successes INTEGER NOT NULL DEFAULT 0,
    in_backoff INTEGER NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    rate_limit_hits INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rate_limit_403s (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    url TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL
);

-- Document indexes
CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(source_url);
CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_source_updated ON documents(source_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_source_status ON documents(source_id, status);
CREATE INDEX IF NOT EXISTS idx_documents_synopsis_null ON documents(source_id) WHERE synopsis IS NULL;
CREATE INDEX IF NOT EXISTS idx_documents_estimated_date ON documents(estimated_date) WHERE estimated_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_with_tags ON documents(id) WHERE tags IS NOT NULL AND tags != '[]';
CREATE INDEX IF NOT EXISTS idx_documents_category ON documents(category_id) WHERE category_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_category_updated ON documents(category_id, updated_at DESC) WHERE category_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_category_source_updated ON documents(category_id, source_id, updated_at DESC) WHERE category_id IS NOT NULL;

-- Version indexes
CREATE INDEX IF NOT EXISTS idx_versions_document ON document_versions(document_id);
CREATE INDEX IF NOT EXISTS idx_versions_hash ON document_versions(content_hash);
CREATE INDEX IF NOT EXISTS idx_versions_mime_type ON document_versions(mime_type);
CREATE INDEX IF NOT EXISTS idx_versions_doc_mime ON document_versions(document_id, mime_type);

-- Virtual file indexes
CREATE INDEX IF NOT EXISTS idx_virtual_files_document ON virtual_files(document_id);
CREATE INDEX IF NOT EXISTS idx_virtual_files_version ON virtual_files(version_id);
CREATE INDEX IF NOT EXISTS idx_virtual_files_status ON virtual_files(status);

-- Page indexes
CREATE INDEX IF NOT EXISTS idx_document_pages_document ON document_pages(document_id);
CREATE INDEX IF NOT EXISTS idx_document_pages_version ON document_pages(version_id);
CREATE INDEX IF NOT EXISTS idx_document_pages_ocr_status ON document_pages(ocr_status);
CREATE INDEX IF NOT EXISTS idx_pages_doc_version ON document_pages(document_id, version_id);
CREATE INDEX IF NOT EXISTS idx_pages_with_text ON document_pages(document_id) WHERE final_text IS NOT NULL;

-- Annotation indexes
CREATE INDEX IF NOT EXISTS idx_annotations_type ON document_annotations(annotation_type);
CREATE INDEX IF NOT EXISTS idx_annotations_completed ON document_annotations(completed_at);

-- OCR result indexes
CREATE INDEX IF NOT EXISTS idx_page_ocr_results_page ON page_ocr_results(page_id);
CREATE INDEX IF NOT EXISTS idx_page_ocr_results_backend ON page_ocr_results(backend);

-- Crawl indexes
CREATE INDEX IF NOT EXISTS idx_crawl_urls_source_status ON crawl_urls(source_id, status);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_parent ON crawl_urls(parent_url);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_discovered ON crawl_urls(discovered_at);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_retry ON crawl_urls(next_retry_at) WHERE status = 'failed';
CREATE INDEX IF NOT EXISTS idx_crawl_requests_source ON crawl_requests(source_id, request_at);
CREATE INDEX IF NOT EXISTS idx_crawl_requests_url ON crawl_requests(url);

-- Config history indexes
CREATE INDEX IF NOT EXISTS idx_config_history_created_at ON configuration_history(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_config_history_hash ON configuration_history(hash);

-- Rate limit indexes
CREATE INDEX IF NOT EXISTS idx_403s_domain_time ON rate_limit_403s(domain, timestamp_ms);

-- Triggers for document counts
CREATE TRIGGER IF NOT EXISTS tr_documents_insert
AFTER INSERT ON documents
BEGIN
    INSERT INTO document_counts (source_id, count)
    VALUES (NEW.source_id, 1)
    ON CONFLICT(source_id) DO UPDATE SET count = count + 1;
END;

CREATE TRIGGER IF NOT EXISTS tr_documents_delete
AFTER DELETE ON documents
BEGIN
    UPDATE document_counts SET count = count - 1
    WHERE source_id = OLD.source_id;
END;

-- Triggers for category counts
CREATE TRIGGER IF NOT EXISTS tr_category_count_insert
AFTER INSERT ON documents
WHEN NEW.category_id IS NOT NULL
BEGIN
    UPDATE file_categories SET doc_count = doc_count + 1
    WHERE id = NEW.category_id;
END;

CREATE TRIGGER IF NOT EXISTS tr_category_count_delete
AFTER DELETE ON documents
WHEN OLD.category_id IS NOT NULL
BEGIN
    UPDATE file_categories SET doc_count = doc_count - 1
    WHERE id = OLD.category_id;
END;

CREATE TRIGGER IF NOT EXISTS tr_category_count_update
AFTER UPDATE OF category_id ON documents
WHEN OLD.category_id IS NOT NEW.category_id
BEGIN
    UPDATE file_categories SET doc_count = doc_count - 1
    WHERE id = OLD.category_id AND OLD.category_id IS NOT NULL;
    UPDATE file_categories SET doc_count = doc_count + 1
    WHERE id = NEW.category_id AND NEW.category_id IS NOT NULL;
END;

-- Store schema version
INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('format_version', '13');
