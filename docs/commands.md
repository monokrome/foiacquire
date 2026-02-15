# Commands Reference

Complete reference for all foia commands.

## Global Options

These options apply to all commands:

```
-t, --target <PATH>    Target directory or database file
-c, --config <PATH>    Configuration file path
    --cwd              Resolve relative paths from current directory
-v, --verbose          Enable verbose logging
-D, --direct           Disable Tor (direct connection)
    --no-obfuscation   Use Tor without pluggable transports
-h, --help             Print help
-V, --version          Print version
```

### Privacy Modes

By default, foia routes traffic through Tor with obfuscation when the embedded-tor feature is enabled. Override with:

| Flag | Description |
|------|-------------|
| `--direct` / `-D` | Disable Tor entirely (direct connection) |
| `--no-obfuscation` | Use Tor without pluggable transports |

Or via environment variables:

| Variable | Description |
|----------|-------------|
| `FOIA_DIRECT=1` | Same as `--direct` |
| `FOIA_NO_OBFUSCATION=1` | Same as `--no-obfuscation` |
| `SOCKS_PROXY` | Use external SOCKS5 proxy instead of embedded Tor |

## Initialization

### init

Initialize database and create directory structure.

```bash
foia init [OPTIONS]
```

Creates the database schema and `documents/` directory in the target location.

**Examples:**
```bash
foia init --target ./foia-data
foia init  # Uses config file or default location
```

## Source Management

### source list

List all configured scraper sources.

```bash
foia source list
```

Shows source IDs, base URLs, and document counts.

### source rename

Rename a source and update all associated documents.

```bash
foia source rename <OLD_NAME> <NEW_NAME>
```

**Example:**
```bash
foia source rename fbi fbi_vault
```

## Discovery & Crawling

### crawl

Discover document URLs without downloading.

```bash
foia crawl <SOURCE_ID> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum URLs to discover |

**Example:**
```bash
foia crawl fbi_vault --limit 1000
```

### discover

Analyze URL patterns to generate new candidates.

```bash
foia discover <SOURCE_ID> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--min-examples <N>` | Minimum examples needed for pattern (default: 3) |
| `--dry-run` | Show candidates without adding to queue |

Analyzes existing URLs to find patterns (e.g., sequential IDs, date-based paths) and generates new URLs to try.

**Example:**
```bash
foia discover fbi_vault --dry-run
```

### state status

Show crawl state for a source.

```bash
foia state status [SOURCE_ID]
```

### state clear

Clear crawl state to restart from beginning.

```bash
foia state clear <SOURCE_ID>
```

## Downloading

### download

Download documents from the crawl queue.

```bash
foia download [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--workers <N>` | Parallel download workers (default: 4) |
| `--limit <N>` | Maximum documents to download |
| `--progress` | Show progress bar |

**Example:**
```bash
foia download fbi_vault --workers 8 --limit 500
```

### scrape

Combined crawl and download in one command.

```bash
foia scrape [SOURCE_IDS...] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--all` | Scrape all configured sources |
| `--workers <N>` | Parallel workers |
| `--limit <N>` | Maximum documents per source |
| `--daemon` | Run continuously |
| `--interval <SECS>` | Interval between daemon runs |
| `-r, --reload[=MODE]` | Config reload mode (default: `next-run`, or `inplace` if flag used without value) |

**Reload Modes:**
- `next-run` - Reload config before next daemon iteration (default)
- `inplace` - Hot-reload config immediately (default when using `-r` or `--reload` alone)
- `stop-process` - Exit process to allow external restart

**Examples:**
```bash
# Single source
foia scrape fbi_vault --limit 100

# Multiple sources
foia scrape fbi_vault cia_foia --workers 4

# All sources in daemon mode
foia scrape --all --daemon --interval 3600

# Daemon with hot-reload on config change
foia scrape --all --daemon --reload

# Daemon with explicit reload mode
foia scrape --all --daemon --reload=next-run
```

### refresh

Re-fetch metadata for existing documents.

```bash
foia refresh [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--workers <N>` | Parallel workers |
| `--limit <N>` | Maximum documents |
| `--force` | Refresh even if not stale |

### import

Import documents from various sources.

#### import warc

Import documents from WARC archive files.

```bash
foia import warc <FILES...> [OPTIONS]
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
foia import warc archive.warc.gz --source archive_org --filter "\.pdf$"
```

#### import url

Import a single document from a URL.

```bash
foia import url <URL> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--source <ID>` | Source ID to assign (default: "manual") |
| `--title <TITLE>` | Document title |

**Example:**
```bash
foia import url https://example.gov/document.pdf --source manual --title "FOIA Response"
```

#### import stdin

Import document content from stdin.

```bash
foia import stdin [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--source <ID>` | Source ID to assign (default: "manual") |
| `--title <TITLE>` | Document title (required) |
| `--url <URL>` | Original URL (optional) |
| `--mimetype <TYPE>` | MIME type (default: auto-detect) |

**Examples:**
```bash
# Import a PDF from stdin
cat document.pdf | foia import stdin --title "My Document" --mimetype application/pdf

# Pipe from curl
curl -s https://example.gov/doc.pdf | foia import stdin --title "Downloaded Doc" --url https://example.gov/doc.pdf
```

## Document Processing

### analyze

Extract text and run OCR on documents.

```bash
foia analyze [SOURCE_ID] [OPTIONS]
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
foia analyze --workers 4
foia analyze fbi_vault --limit 100
```

### analyze-check

Verify OCR tools are installed and working.

```bash
foia analyze-check
```

Checks for: tesseract, pdftotext, and optional backends (ocrs, paddle).

### analyze-compare

Compare OCR backends on a test file.

```bash
foia analyze-compare <FILE> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--pages <RANGE>` | Page range (e.g., "1-5") |
| `--backends <LIST>` | Backends to test (comma-separated) |
| `--deepseek-path <PATH>` | Path to DeepSeek model |

**Example:**
```bash
foia analyze-compare scan.pdf --backends tesseract,ocrs
```

### archive

Extract contents from ZIP archives and email attachments.

```bash
foia archive [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum archives to process |
| `--ocr` | Run OCR on extracted files |

### annotate

Generate summaries and tags using LLM.

```bash
foia annotate [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum documents |
| `--endpoint <URL>` | LLM endpoint URL |
| `--model <NAME>` | Model name |
| `--daemon` | Run continuously |
| `--interval <SECS>` | Daemon interval |

**Examples:**
```bash
# Using Ollama (default)
foia annotate --limit 50 --model llama3.2

# Using Groq (via environment)
GROQ_API_KEY=gsk_... LLM_MODEL=llama-3.1-70b-versatile foia annotate
```

### annotate reset

Clear annotations to allow re-annotation.

```bash
foia annotate reset [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum documents to reset |

**Example:**
```bash
# Reset all annotations for a source
foia annotate reset fbi_vault
```

### detect-dates

Detect and estimate publication dates.

```bash
foia detect-dates [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum documents |
| `--dry-run` | Show dates without saving |

### extract-entities

Extract named entities (people, organizations, locations, file numbers) from document text.

```bash
foia extract-entities [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-l, --limit <N>` | Maximum documents to process |

Extracts entities using regex-based NER and stores them in the `document_entities` table. With the `gis` feature enabled, location entities are automatically geocoded using an embedded city database.

**Examples:**
```bash
foia extract-entities
foia extract-entities fbi_vault -l 100
```

### backfill-entities

Backfill the `document_entities` table from existing NER annotation metadata.

```bash
foia backfill-entities [SOURCE_ID] [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-l, --limit <N>` | Maximum documents to process |

For documents that already have NER annotations in metadata JSON but no rows in `document_entities`. One-time migration aid.

### search-entities

Search documents by extracted entities.

```bash
foia search-entities <QUERY> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--type <TYPE>` | Filter by entity type: `person`, `organization`, `location`, `file_number` |
| `--near <COORDS>` | Spatial search: `lat,lon,radius_km` (requires PostgreSQL + PostGIS) |
| `--source <ID>` | Filter by source |
| `-l, --limit <N>` | Maximum results |

**Examples:**
```bash
foia search-entities CIA
foia search-entities "Fort Meade" --type location
foia search-entities Moscow --near "55.75,37.61,100"
```

### llm-models

List available LLM models from Ollama.

```bash
foia llm-models
```

## Browsing & Search

### ls

List documents with filtering.

```bash
foia ls [OPTIONS]
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
foia ls --source fbi_vault --limit 20
foia ls --tag classified --format json
```

### info

Show document metadata.

```bash
foia info <DOC_ID>
```

Displays: title, URL, source, dates, hashes, status, tags, and extracted text preview.

### read

Output document content.

```bash
foia read <DOC_ID> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--text` | Output extracted text instead of binary |

**Example:**
```bash
foia read abc123 --text | less
foia read abc123 > document.pdf
```

### search

Full-text search across documents.

```bash
foia search <QUERY> [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--source <ID>` | Filter by source |
| `--limit <N>` | Maximum results |

**Example:**
```bash
foia search "project blue book" --limit 50
```

### serve

Start the web interface.

```bash
foia serve [BIND_ADDRESS]
```

Default bind address: `127.0.0.1:3030`

**Examples:**
```bash
foia serve                    # localhost only
foia serve 0.0.0.0:3030      # all interfaces
foia serve 192.168.1.10:8080 # specific IP
```

## Configuration Management

### config recover

Recover a skeleton config from an existing database.

```bash
foia config recover
```

Generates a basic config based on sources found in the database.

### config restore

Restore the most recent config from database history.

```bash
foia config restore
```

### config history

List configuration history entries.

```bash
foia config history
```

## Database Management

### db copy

Copy data between databases (SQLite ↔ PostgreSQL).

```bash
foia db copy <FROM> <TO> [OPTIONS]
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
foia db copy ./foia.db postgres://user:pass@host/db --copy --progress

# PostgreSQL to SQLite backup
foia db copy postgres://... ./backup.db
```

### db load-regions

Load region boundary data for spatial queries. Requires PostgreSQL with PostGIS and the `gis` feature.

```bash
foia db load-regions [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--file <PATH>` | Custom GeoJSON file (instead of embedded data) |

Loads Natural Earth country and state/province boundaries into the `regions` table. Safe to run repeatedly (upserts on name + type).

**Examples:**
```bash
foia db load-regions
foia db load-regions --file custom_boundaries.geojson
```

### db remap-categories

Update document categories based on MIME types.

```bash
foia db remap-categories [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--dry-run` | Show changes without applying |
| `--batch-size <N>` | Batch size |

## Browser Testing

### browser-test

Test browser-based fetching.

```bash
foia browser-test <URL> [OPTIONS]
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
foia browser-test https://example.gov/protected --engine stealth --headed
```

## Status

### status

Show system status.

```bash
foia status
```

Displays database stats, queue status, and configuration info.
