-- FOIAcquire SQLite Schema

-- Sources table
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    last_scraped TEXT
);

-- Documents table
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    extracted_text TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    synopsis TEXT,
    tags TEXT,
    estimated_date TEXT,
    date_confidence TEXT,
    date_source TEXT,
    manual_date TEXT,
    discovery_method TEXT NOT NULL DEFAULT 'seed',
    category_id TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

-- Document versions table
CREATE TABLE IF NOT EXISTS document_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_hash_blake3 TEXT,
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

-- Document pages table
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
    UNIQUE(document_id, version_id, page_number),
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

-- Virtual files table
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
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

-- Crawl URLs table
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

-- Crawl requests table
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

-- Crawl config table
CREATE TABLE IF NOT EXISTS crawl_config (
    source_id TEXT PRIMARY KEY,
    config_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Configuration history table
CREATE TABLE IF NOT EXISTS configuration_history (
    uuid TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    format TEXT NOT NULL DEFAULT 'json',
    hash TEXT NOT NULL
);

-- Rate limit state table
CREATE TABLE IF NOT EXISTS rate_limit_state (
    domain TEXT PRIMARY KEY,
    current_delay_ms INTEGER NOT NULL,
    in_backoff INTEGER NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    rate_limit_hits INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source_id);
CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(source_url);
CREATE INDEX IF NOT EXISTS idx_document_versions_doc ON document_versions(document_id);
CREATE INDEX IF NOT EXISTS idx_document_versions_hashes ON document_versions(content_hash, content_hash_blake3, file_size);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_source_status ON crawl_urls(source_id, status);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_parent ON crawl_urls(parent_url);
CREATE INDEX IF NOT EXISTS idx_crawl_requests_source ON crawl_requests(source_id, request_at);
CREATE INDEX IF NOT EXISTS idx_config_history_hash ON configuration_history(hash);
