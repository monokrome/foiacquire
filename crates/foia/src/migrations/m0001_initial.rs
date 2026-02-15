use cetane::prelude::*;

pub fn migration() -> Migration {
    Migration::new("0001_initial_schema")
        // file_categories - must come first for FK reference
        .operation(
            CreateTable::new("file_categories")
                .add_field(Field::new("id", FieldType::Text).primary_key())
                .add_field(Field::new("description", FieldType::Text))
                .add_field(Field::new("doc_count", FieldType::Integer).not_null().default("0")),
        )
        // Seed file_categories
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"INSERT OR IGNORE INTO file_categories (id, description, doc_count) VALUES
    ('documents', 'PDF, Word, text, and email documents', 0),
    ('images', 'Image files (PNG, JPG, GIF, etc.)', 0),
    ('data', 'Spreadsheets, CSV, JSON, and XML files', 0),
    ('archives', 'ZIP, TAR, and other archive files', 0),
    ('markup', 'HTML and XML markup files', 0),
    ('other', 'Other file types', 0)"#,
                )
                .for_backend(
                    "postgres",
                    r#"INSERT INTO file_categories (id, description, doc_count) VALUES
    ('documents', 'PDF, Word, text, and email documents', 0),
    ('images', 'Image files (PNG, JPG, GIF, etc.)', 0),
    ('data', 'Spreadsheets, CSV, JSON, and XML files', 0),
    ('archives', 'ZIP, TAR, and other archive files', 0),
    ('markup', 'HTML and XML markup files', 0),
    ('other', 'Other file types', 0)
ON CONFLICT (id) DO NOTHING"#,
                ),
        )
        // documents
        .operation(
            CreateTable::new("documents")
                .add_field(Field::new("id", FieldType::Text).primary_key())
                .add_field(Field::new("source_id", FieldType::Text).not_null())
                .add_field(Field::new("title", FieldType::Text).not_null())
                .add_field(Field::new("source_url", FieldType::Text).not_null())
                .add_field(Field::new("extracted_text", FieldType::Text))
                .add_field(Field::new("synopsis", FieldType::Text))
                .add_field(Field::new("tags", FieldType::Text))
                .add_field(Field::new("status", FieldType::Text).not_null())
                .add_field(Field::new("metadata", FieldType::Text).not_null())
                .add_field(Field::new("created_at", FieldType::Text).not_null())
                .add_field(Field::new("updated_at", FieldType::Text).not_null())
                .add_field(Field::new("estimated_date", FieldType::Text))
                .add_field(Field::new("date_confidence", FieldType::Text))
                .add_field(Field::new("date_source", FieldType::Text))
                .add_field(Field::new("manual_date", FieldType::Text))
                .add_field(Field::new("discovery_method", FieldType::Text).not_null().default("'import'"))
                .add_field(Field::new("category_id", FieldType::Text).references("file_categories", "id")),
        )
        // document_versions - backend-specific due to SERIAL vs AUTOINCREMENT
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS document_versions (
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
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS document_versions (
    id SERIAL PRIMARY KEY,
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
)"#,
                ),
        )
        // storage_meta
        .operation(
            CreateTable::new("storage_meta")
                .add_field(Field::new("key", FieldType::Text).primary_key())
                .add_field(Field::new("value", FieldType::Text).not_null()),
        )
        // virtual_files
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS virtual_files (
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
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS virtual_files (
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
)"#,
                ),
        )
        // document_pages
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS document_pages (
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
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS document_pages (
    id SERIAL PRIMARY KEY,
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
)"#,
                ),
        )
        // document_annotations - has composite PK
        .operation(
            RunSql::new(
                r#"CREATE TABLE IF NOT EXISTS document_annotations (
    document_id TEXT NOT NULL,
    annotation_type TEXT NOT NULL,
    completed_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    result TEXT,
    error TEXT,
    PRIMARY KEY (document_id, annotation_type),
    FOREIGN KEY (document_id) REFERENCES documents(id)
)"#,
            ),
        )
        // page_ocr_results
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS page_ocr_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id INTEGER NOT NULL,
    backend TEXT NOT NULL,
    ocr_text TEXT,
    confidence REAL,
    processing_time_ms INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY (page_id) REFERENCES document_pages(id),
    UNIQUE(page_id, backend)
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS page_ocr_results (
    id SERIAL PRIMARY KEY,
    page_id INTEGER NOT NULL,
    backend TEXT NOT NULL,
    ocr_text TEXT,
    confidence REAL,
    processing_time_ms INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY (page_id) REFERENCES document_pages(id),
    UNIQUE(page_id, backend)
)"#,
                ),
        )
        // document_counts
        .operation(
            CreateTable::new("document_counts")
                .add_field(Field::new("source_id", FieldType::Text).primary_key())
                .add_field(Field::new("count", FieldType::Integer).not_null().default("0")),
        )
        // sources
        .operation(
            CreateTable::new("sources")
                .add_field(Field::new("id", FieldType::Text).primary_key())
                .add_field(Field::new("source_type", FieldType::Text).not_null())
                .add_field(Field::new("name", FieldType::Text).not_null())
                .add_field(Field::new("base_url", FieldType::Text).not_null())
                .add_field(Field::new("metadata", FieldType::Text).not_null())
                .add_field(Field::new("created_at", FieldType::Text).not_null())
                .add_field(Field::new("last_scraped", FieldType::Text)),
        )
        // crawl_urls
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS crawl_urls (
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
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS crawl_urls (
    id SERIAL PRIMARY KEY,
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
)"#,
                ),
        )
        // crawl_requests
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS crawl_requests (
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
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS crawl_requests (
    id SERIAL PRIMARY KEY,
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
)"#,
                ),
        )
        // crawl_config
        .operation(
            CreateTable::new("crawl_config")
                .add_field(Field::new("source_id", FieldType::Text).primary_key())
                .add_field(Field::new("config_hash", FieldType::Text).not_null())
                .add_field(Field::new("updated_at", FieldType::Text).not_null()),
        )
        // configuration_history
        .operation(
            CreateTable::new("configuration_history")
                .add_field(Field::new("uuid", FieldType::Text).primary_key())
                .add_field(Field::new("created_at", FieldType::Text).not_null())
                .add_field(Field::new("data", FieldType::Text).not_null())
                .add_field(Field::new("format", FieldType::Text).not_null())
                .add_field(Field::new("hash", FieldType::Text).not_null()),
        )
        // rate_limit_state
        .operation(
            CreateTable::new("rate_limit_state")
                .add_field(Field::new("domain", FieldType::Text).primary_key())
                .add_field(Field::new("current_delay_ms", FieldType::Integer).not_null())
                .add_field(Field::new("in_backoff", FieldType::Integer).not_null().default("0"))
                .add_field(Field::new("total_requests", FieldType::Integer).not_null().default("0"))
                .add_field(Field::new("rate_limit_hits", FieldType::Integer).not_null().default("0"))
                .add_field(Field::new("updated_at", FieldType::Text).not_null().default("CURRENT_TIMESTAMP")),
        )
        // rate_limit_domains
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS rate_limit_domains (
    domain TEXT PRIMARY KEY,
    current_delay_ms INTEGER NOT NULL,
    last_request_at INTEGER,
    consecutive_successes INTEGER NOT NULL DEFAULT 0,
    in_backoff INTEGER NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    rate_limit_hits INTEGER NOT NULL DEFAULT 0
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS rate_limit_domains (
    domain TEXT PRIMARY KEY,
    current_delay_ms INTEGER NOT NULL,
    last_request_at BIGINT,
    consecutive_successes INTEGER NOT NULL DEFAULT 0,
    in_backoff INTEGER NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    rate_limit_hits INTEGER NOT NULL DEFAULT 0
)"#,
                ),
        )
        // rate_limit_403s
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TABLE IF NOT EXISTS rate_limit_403s (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    url TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL
)"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE TABLE IF NOT EXISTS rate_limit_403s (
    id SERIAL PRIMARY KEY,
    domain TEXT NOT NULL,
    url TEXT NOT NULL,
    timestamp_ms BIGINT NOT NULL
)"#,
                ),
        )
        // Document indexes
        .operation(AddIndex::new("documents", Index::new("idx_documents_source").column("source_id")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_status").column("status")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_url").column("source_url")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_updated_at").column_desc("updated_at")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_source_updated").column("source_id").column_desc("updated_at")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_source_status").column("source_id").column("status")))
        // Partial indexes
        .operation(AddIndex::new("documents", Index::new("idx_documents_synopsis_null").column("source_id").filter("synopsis IS NULL")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_estimated_date").column("estimated_date").filter("estimated_date IS NOT NULL")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_with_tags").column("id").filter("tags IS NOT NULL AND tags != '[]'")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_category").column("category_id").filter("category_id IS NOT NULL")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_category_updated").column("category_id").column_desc("updated_at").filter("category_id IS NOT NULL")))
        .operation(AddIndex::new("documents", Index::new("idx_documents_category_source_updated").column("category_id").column("source_id").column_desc("updated_at").filter("category_id IS NOT NULL")))
        // Version indexes
        .operation(AddIndex::new("document_versions", Index::new("idx_versions_document").column("document_id")))
        .operation(AddIndex::new("document_versions", Index::new("idx_versions_hash").column("content_hash")))
        .operation(AddIndex::new("document_versions", Index::new("idx_versions_mime_type").column("mime_type")))
        .operation(AddIndex::new("document_versions", Index::new("idx_versions_doc_mime").column("document_id").column("mime_type")))
        // Virtual file indexes
        .operation(AddIndex::new("virtual_files", Index::new("idx_virtual_files_document").column("document_id")))
        .operation(AddIndex::new("virtual_files", Index::new("idx_virtual_files_version").column("version_id")))
        .operation(AddIndex::new("virtual_files", Index::new("idx_virtual_files_status").column("status")))
        // Page indexes
        .operation(AddIndex::new("document_pages", Index::new("idx_document_pages_document").column("document_id")))
        .operation(AddIndex::new("document_pages", Index::new("idx_document_pages_version").column("version_id")))
        .operation(AddIndex::new("document_pages", Index::new("idx_document_pages_ocr_status").column("ocr_status")))
        .operation(AddIndex::new("document_pages", Index::new("idx_pages_doc_version").column("document_id").column("version_id")))
        .operation(AddIndex::new("document_pages", Index::new("idx_pages_with_text").column("document_id").filter("final_text IS NOT NULL")))
        // Annotation indexes
        .operation(AddIndex::new("document_annotations", Index::new("idx_annotations_type").column("annotation_type")))
        .operation(AddIndex::new("document_annotations", Index::new("idx_annotations_completed").column("completed_at")))
        // OCR result indexes
        .operation(AddIndex::new("page_ocr_results", Index::new("idx_page_ocr_results_page").column("page_id")))
        .operation(AddIndex::new("page_ocr_results", Index::new("idx_page_ocr_results_backend").column("backend")))
        // Crawl indexes
        .operation(AddIndex::new("crawl_urls", Index::new("idx_crawl_urls_source_status").column("source_id").column("status")))
        .operation(AddIndex::new("crawl_urls", Index::new("idx_crawl_urls_parent").column("parent_url")))
        .operation(AddIndex::new("crawl_urls", Index::new("idx_crawl_urls_discovered").column("discovered_at")))
        .operation(AddIndex::new("crawl_urls", Index::new("idx_crawl_urls_retry").column("next_retry_at").filter("status = 'failed'")))
        .operation(AddIndex::new("crawl_requests", Index::new("idx_crawl_requests_source").column("source_id").column("request_at")))
        .operation(AddIndex::new("crawl_requests", Index::new("idx_crawl_requests_url").column("url")))
        // Config history indexes
        .operation(AddIndex::new("configuration_history", Index::new("idx_config_history_created_at").column_desc("created_at")))
        .operation(AddIndex::new("configuration_history", Index::new("idx_config_history_hash").column("hash")))
        // Rate limit indexes
        .operation(AddIndex::new("rate_limit_403s", Index::new("idx_403s_domain_time").column("domain").column("timestamp_ms")))
        // Triggers
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TRIGGER IF NOT EXISTS tr_documents_insert
AFTER INSERT ON documents
BEGIN
    INSERT INTO document_counts (source_id, count)
    VALUES (NEW.source_id, 1)
    ON CONFLICT(source_id) DO UPDATE SET count = count + 1;
END"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE OR REPLACE FUNCTION update_document_count_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO document_counts (source_id, count)
    VALUES (NEW.source_id, 1)
    ON CONFLICT(source_id) DO UPDATE SET count = document_counts.count + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql"#,
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TRIGGER IF NOT EXISTS tr_documents_delete
AFTER DELETE ON documents
BEGIN
    UPDATE document_counts SET count = count - 1
    WHERE source_id = OLD.source_id;
END"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE OR REPLACE FUNCTION update_document_count_delete()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE document_counts SET count = count - 1
    WHERE source_id = OLD.source_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql"#,
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TRIGGER IF NOT EXISTS tr_category_count_insert
AFTER INSERT ON documents
WHEN NEW.category_id IS NOT NULL
BEGIN
    UPDATE file_categories SET doc_count = doc_count + 1
    WHERE id = NEW.category_id;
END"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE OR REPLACE FUNCTION update_category_count_insert()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.category_id IS NOT NULL THEN
        UPDATE file_categories SET doc_count = doc_count + 1
        WHERE id = NEW.category_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql"#,
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TRIGGER IF NOT EXISTS tr_category_count_delete
AFTER DELETE ON documents
WHEN OLD.category_id IS NOT NULL
BEGIN
    UPDATE file_categories SET doc_count = doc_count - 1
    WHERE id = OLD.category_id;
END"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE OR REPLACE FUNCTION update_category_count_delete()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.category_id IS NOT NULL THEN
        UPDATE file_categories SET doc_count = doc_count - 1
        WHERE id = OLD.category_id;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql"#,
                ),
        )
        .operation(
            RunSql::portable()
                .for_backend(
                    "sqlite",
                    r#"CREATE TRIGGER IF NOT EXISTS tr_category_count_update
AFTER UPDATE OF category_id ON documents
WHEN OLD.category_id IS NOT NEW.category_id
BEGIN
    UPDATE file_categories SET doc_count = doc_count - 1
    WHERE id = OLD.category_id AND OLD.category_id IS NOT NULL;
    UPDATE file_categories SET doc_count = doc_count + 1
    WHERE id = NEW.category_id AND NEW.category_id IS NOT NULL;
END"#,
                )
                .for_backend(
                    "postgres",
                    r#"CREATE OR REPLACE FUNCTION update_category_count_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.category_id IS DISTINCT FROM NEW.category_id THEN
        IF OLD.category_id IS NOT NULL THEN
            UPDATE file_categories SET doc_count = doc_count - 1
            WHERE id = OLD.category_id;
        END IF;
        IF NEW.category_id IS NOT NULL THEN
            UPDATE file_categories SET doc_count = doc_count + 1
            WHERE id = NEW.category_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql"#,
                ),
        )
        // PostgreSQL trigger creation
        .operation(
            RunSql::new("DROP TRIGGER IF EXISTS tr_documents_insert ON documents")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("DROP TRIGGER IF EXISTS tr_documents_delete ON documents")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("DROP TRIGGER IF EXISTS tr_category_count_insert ON documents")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("DROP TRIGGER IF EXISTS tr_category_count_delete ON documents")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("DROP TRIGGER IF EXISTS tr_category_count_update ON documents")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("CREATE TRIGGER tr_documents_insert AFTER INSERT ON documents FOR EACH ROW EXECUTE FUNCTION update_document_count_insert()")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("CREATE TRIGGER tr_documents_delete AFTER DELETE ON documents FOR EACH ROW EXECUTE FUNCTION update_document_count_delete()")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("CREATE TRIGGER tr_category_count_insert AFTER INSERT ON documents FOR EACH ROW EXECUTE FUNCTION update_category_count_insert()")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("CREATE TRIGGER tr_category_count_delete AFTER DELETE ON documents FOR EACH ROW EXECUTE FUNCTION update_category_count_delete()")
                .only_for(&["postgres"]),
        )
        .operation(
            RunSql::new("CREATE TRIGGER tr_category_count_update AFTER UPDATE OF category_id ON documents FOR EACH ROW EXECUTE FUNCTION update_category_count_update()")
                .only_for(&["postgres"]),
        )
        // Set schema version
        .operation(
            RunSql::portable()
                .for_backend("sqlite", "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('format_version', '13')")
                .for_backend("postgres", "INSERT INTO storage_meta (key, value) VALUES ('format_version', '13') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
        )
}
