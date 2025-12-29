# Commands Reference

Complete reference for all foiacquire commands.

## Global Options

These options apply to all commands:

```
-t, --target <PATH>    Target directory or database file
-c, --config <PATH>    Configuration file path
    --cwd              Resolve relative paths from current directory
-v, --verbose          Enable verbose logging
-h, --help             Print help
-V, --version          Print version
```

## Initialization

### init

Initialize database and create directory structure.

```bash
foiacquire init [OPTIONS]
```

Creates the database schema and `documents/` directory in the target location.

**Examples:**
```bash
foiacquire init --target ./foia-data
foiacquire init  # Uses config file or default location
```

## Source Management

### source list

List all configured scraper sources.

```bash
foiacquire source list
```

Shows source IDs, base URLs, and document counts.

### source rename

Rename a source and update all associated documents.

```bash
foiacquire source rename <OLD_NAME> <NEW_NAME>
```

**Example:**
```bash
foiacquire source rename fbi fbi_vault
```

## Discovery & Crawling

### crawl

Discover document URLs without downloading.

```bash
foiacquire crawl <SOURCE_ID> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum URLs to discover |

**Example:**
```bash
foiacquire crawl fbi_vault --limit 1000
```

### discover

Analyze URL patterns to generate new candidates.

```bash
foiacquire discover <SOURCE_ID> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--min-examples <N>` | Minimum examples needed for pattern (default: 3) |
| `--dry-run` | Show candidates without adding to queue |

Analyzes existing URLs to find patterns (e.g., sequential IDs, date-based paths) and generates new URLs to try.

**Example:**
```bash
foiacquire discover fbi_vault --dry-run
```

### state status

Show crawl state for a source.

```bash
foiacquire state status [SOURCE_ID]
```

### state clear

Clear crawl state to restart from beginning.

```bash
foiacquire state clear <SOURCE_ID>
```

## Downloading

### download

Download documents from the crawl queue.

```bash
foiacquire download [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--workers <N>` | Parallel download workers (default: 4) |
| `--limit <N>` | Maximum documents to download |
| `--progress` | Show progress bar |

**Example:**
```bash
foiacquire download fbi_vault --workers 8 --limit 500
```

### scrape

Combined crawl and download in one command.

```bash
foiacquire scrape [SOURCE_IDS...] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--all` | Scrape all configured sources |
| `--workers <N>` | Parallel workers |
| `--limit <N>` | Maximum documents per source |
| `--daemon` | Run continuously |
| `--interval <SECS>` | Interval between daemon runs |
| `--reload <MODE>` | Config reload: `next-run`, `stop-process`, `inplace` |

**Examples:**
```bash
# Single source
foiacquire scrape fbi_vault --limit 100

# Multiple sources
foiacquire scrape fbi_vault cia_foia --workers 4

# All sources in daemon mode
foiacquire scrape --all --daemon --interval 3600
```

### refresh

Re-fetch metadata for existing documents.

```bash
foiacquire refresh [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--workers <N>` | Parallel workers |
| `--limit <N>` | Maximum documents |
| `--force` | Refresh even if not stale |

### import

Import documents from WARC archive files.

```bash
foiacquire import <FILES...> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--source <ID>` | Source ID to assign |
| `--filter <REGEX>` | URL pattern filter |
| `--limit <N>` | Maximum records to import |
| `--scan-limit <N>` | Maximum records to scan |
| `--dry-run` | Show what would be imported |
| `--no-resume` | Don't resume from checkpoint |
| `--checkpoint-interval <N>` | Records between checkpoints |

**Example:**
```bash
foiacquire import archive.warc.gz --source archive_org --filter "\.pdf$"
```

## Document Processing

### analyze

Extract text and run OCR on documents.

```bash
foiacquire analyze [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--workers <N>` | Parallel workers |
| `--limit <N>` | Maximum documents |
| `--extract-urls` | Extract URLs from text |
| `--daemon` | Run continuously |
| `--interval <SECS>` | Daemon interval |

**Examples:**
```bash
foiacquire analyze --workers 4
foiacquire analyze fbi_vault --limit 100
```

### analyze-check

Verify OCR tools are installed and working.

```bash
foiacquire analyze-check
```

Checks for: tesseract, pdftotext, and optional backends (ocrs, paddle).

### analyze-compare

Compare OCR backends on a test file.

```bash
foiacquire analyze-compare <FILE> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--pages <RANGE>` | Page range (e.g., "1-5") |
| `--backends <LIST>` | Backends to test (comma-separated) |
| `--deepseek-path <PATH>` | Path to DeepSeek model |

**Example:**
```bash
foiacquire analyze-compare scan.pdf --backends tesseract,ocrs
```

### archive

Extract contents from ZIP archives and email attachments.

```bash
foiacquire archive [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum archives to process |
| `--ocr` | Run OCR on extracted files |

### annotate

Generate summaries and tags using LLM.

```bash
foiacquire annotate [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum documents |
| `--endpoint <URL>` | Ollama endpoint |
| `--model <NAME>` | Model name |
| `--daemon` | Run continuously |
| `--interval <SECS>` | Daemon interval |

**Example:**
```bash
foiacquire annotate --limit 50 --model llama3.2
```

### detect-dates

Detect and estimate publication dates.

```bash
foiacquire detect-dates [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum documents |
| `--dry-run` | Show dates without saving |

### llm-models

List available LLM models from Ollama.

```bash
foiacquire llm-models
```

## Browsing & Search

### ls

List documents with filtering.

```bash
foiacquire ls [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--source <ID>` | Filter by source |
| `--tag <TAG>` | Filter by tag |
| `--type-filter <TYPE>` | Filter by MIME type |
| `--limit <N>` | Maximum results |
| `--format <FMT>` | Output: `table`, `json`, `ids` |

**Examples:**
```bash
foiacquire ls --source fbi_vault --limit 20
foiacquire ls --tag classified --format json
```

### info

Show document metadata.

```bash
foiacquire info <DOC_ID>
```

Displays: title, URL, source, dates, hashes, status, tags, and extracted text preview.

### read

Output document content.

```bash
foiacquire read <DOC_ID> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--text` | Output extracted text instead of binary |

**Example:**
```bash
foiacquire read abc123 --text | less
foiacquire read abc123 > document.pdf
```

### search

Full-text search across documents.

```bash
foiacquire search <QUERY> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--source <ID>` | Filter by source |
| `--limit <N>` | Maximum results |

**Example:**
```bash
foiacquire search "project blue book" --limit 50
```

### serve

Start the web interface.

```bash
foiacquire serve [BIND_ADDRESS]
```

Default bind address: `127.0.0.1:3030`

**Examples:**
```bash
foiacquire serve                    # localhost only
foiacquire serve 0.0.0.0:3030      # all interfaces
foiacquire serve 192.168.1.10:8080 # specific IP
```

## Configuration Management

### config recover

Recover a skeleton config from an existing database.

```bash
foiacquire config recover
```

Generates a basic config based on sources found in the database.

### config restore

Restore the most recent config from database history.

```bash
foiacquire config restore
```

### config history

List configuration history entries.

```bash
foiacquire config history
```

## Database Management

### db copy

Copy data between databases (SQLite ↔ PostgreSQL).

```bash
foiacquire db copy <FROM> <TO> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--clear` | Clear destination before copy |
| `--batch-size <N>` | Rows per batch |
| `--copy` | Use COPY protocol (Postgres) |
| `--progress` | Show progress |
| `--analyze` | Run ANALYZE after copy |
| `--skip-duplicates` | Skip existing records |
| `--tables <LIST>` | Specific tables to copy |

**Examples:**
```bash
# SQLite to PostgreSQL
foiacquire db copy ./foiacquire.db postgres://user:pass@host/db --copy --progress

# PostgreSQL to SQLite backup
foiacquire db copy postgres://... ./backup.db
```

### db remap-categories

Update document categories based on MIME types.

```bash
foiacquire db remap-categories [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--dry-run` | Show changes without applying |
| `--batch-size <N>` | Batch size |

## Browser Testing

### browser-test

Test browser-based fetching.

```bash
foiacquire browser-test <URL> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--headed` | Show browser window |
| `--engine <TYPE>` | Engine: `standard`, `stealth`, `cookies` |
| `--proxy <URL>` | Proxy URL |
| `--browser-url <URL>` | Remote DevTools URL |
| `--cookies <PATH>` | Load cookies from file |
| `--save-cookies <PATH>` | Save cookies to file |
| `--output <PATH>` | Save page content |
| `--binary` | Fetch as binary |
| `--context-url <URL>` | Visit URL first (for auth) |

**Example:**
```bash
foiacquire browser-test https://example.gov/protected --engine stealth --headed
```

## Status

### status

Show system status.

```bash
foiacquire status
```

Displays database stats, queue status, and configuration info.
