# foia

FOIA document acquisition and research system. Discovers, downloads, extracts text via OCR, and annotates public records from government agencies and document repositories.

## Features

- **Discovery** — find documents via HTML crawling, API search, sitemaps, Wayback Machine, search engines, and pattern-based URL generation
- **Scraping** — download with adaptive rate limiting, browser automation for JS-heavy sites, and optional Tor routing
- **Analysis** — text extraction with poppler-utils, OCR via Groq, OCRS, PaddleOCR, or Tesseract
- **Annotation** — LLM-powered summaries, tags, named entity recognition, and date detection (Ollama, Groq, OpenAI, Together.ai)
- **Import** — ingest from WARC archives, Concordance DAT/OPT load files, URL lists, or stdin
- **Search** — full-text search, entity-based queries, and a web interface
- **Storage** — content-addressable file storage with SHA-256 and BLAKE3 deduplication
- **Privacy** — Tor routing by default with pluggable transports; supports external SOCKS proxies
- **Database** — SQLite (default) or PostgreSQL for larger deployments

## Quick Start

### Docker

```sh
docker run -d --name foia \
  -v ./data:/opt/foia \
  -e DATABASE_URL=sqlite:///opt/foia/foia.db \
  ghcr.io/foiacquire/foia:clearnet
```

With Tor:

```sh
docker run -d --name foia \
  -v ./data:/opt/foia \
  ghcr.io/foiacquire/foia:latest
```

With browser automation:

```sh
docker run -d --name chromium -p 9222:9222 --shm-size=2g ghcr.io/foiacquire/chromium:latest
docker run -d --name foia \
  -v ./data:/opt/foia \
  -e BROWSER_URL=ws://chromium:9222 \
  --link chromium \
  ghcr.io/foiacquire/foia:latest scrape cia_foia
```

### From Source

Requires Rust toolchain and poppler-utils for text extraction.

```sh
cargo build --release
./target/release/foia init --config etc/example.json
./target/release/foia scrape --all
```

## Container Images

| Image | Tag | Description |
|-------|-----|-------------|
| `ghcr.io/foiacquire/foia` | `latest` | Tor-enabled (default) |
| `ghcr.io/foiacquire/foia` | `clearnet` | No Tor |
| `ghcr.io/foiacquire/foia` | `tesseract` | Tor + Tesseract OCR |
| `ghcr.io/foiacquire/foia` | `tesseract-clearnet` | Tesseract OCR, no Tor |
| `ghcr.io/foiacquire/chromium` | `latest` | Stealth browser + Tor ([repo](https://github.com/foiacquire/chromium)) |
| `ghcr.io/foiacquire/chromium` | `clearnet` | Standard browser |

## Usage

```sh
# Initialize database and import scraper configs
foia init --config config.json

# Discover and download documents
foia scrape --all
foia scrape fbi_vault --limit 100

# Run as daemon
foia scrape --all --daemon --interval 3600

# OCR and text extraction
foia analyze --daemon --interval 300

# LLM annotation (summaries, tags, entities)
foia annotate --daemon --interval 300

# Search and browse
foia search "surveillance program"
foia ls --source cia_foia
foia serve
```

## Commands

### Document Acquisition

| Command | Description |
|---------|-------------|
| `scrape <source>` | Crawl and download documents (supports `--all`, `--daemon`) |
| `crawl <source>` | Discover document URLs without downloading |
| `download [source]` | Download pending documents from queue |
| `refresh [source]` | Re-fetch metadata for existing documents |

### Discovery

| Command | Description |
|---------|-------------|
| `discover sitemap <source>` | Discover from sitemaps and robots.txt |
| `discover search <source>` | Search engine discovery (DuckDuckGo, Google, Bing, Brave) |
| `discover wayback <source>` | Wayback Machine historical snapshots |
| `discover pattern <source>` | Infer URL patterns from existing documents |
| `discover paths <source>` | Check common document paths |
| `discover all <source>` | Run all discovery methods |

### Import

| Command | Description |
|---------|-------------|
| `import warc <files>` | Import from WARC archives (.warc, .warc.gz) |
| `import concordance <path>` | Import from Concordance DAT/OPT load files |
| `import urls --file <file>` | Import URLs from a text file |
| `import stdin --url <url>` | Import content from stdin |

### Document Processing

| Command | Description |
|---------|-------------|
| `analyze [source]` | Extract text and run OCR (supports `--daemon`) |
| `analyze-check` | Verify OCR tools are installed |
| `analyze-compare <file>` | Compare OCR backends on a file |
| `annotate [source]` | Generate summaries/tags with LLM (supports `--daemon`) |
| `detect-dates [source]` | Detect publication dates in documents |
| `extract-entities [source]` | Extract named entities (people, orgs, locations) |
| `archive [source]` | Extract contents from ZIP/email attachments |

### Browsing & Search

| Command | Description |
|---------|-------------|
| `ls` | List documents with filtering by source, tag, type |
| `info <doc_id>` | Show document metadata |
| `read <doc_id>` | Output document content |
| `search <query>` | Full-text search |
| `search-entities <query>` | Search by extracted entities (supports spatial `--near`) |
| `serve [bind]` | Start web interface (default: 127.0.0.1:3030) |
| `status` | System status (TUI with `--live`, or `--json`) |

### Management

| Command | Description |
|---------|-------------|
| `init` | Initialize database and directories |
| `source list` | List configured sources |
| `source rename <old> <new>` | Rename a source |
| `config transfer` | Import config file into database |
| `config get <key>` | Get a config value |
| `config set <key> <value>` | Set a config value |
| `db migrate` | Run database migrations |
| `db copy <from> <to>` | Copy data between SQLite and PostgreSQL |
| `db deduplicate` | Deduplicate documents by content hash |
| `state status` | Show crawl state |
| `state clear <source>` | Reset crawl state |

## Configuration

Scraper configs define how to discover and fetch documents per source. See [`etc/example.json`](etc/example.json) for a complete example.

```json
{
  "scrapers": {
    "fbi_vault": {
      "discovery": {
        "type": "html_crawl",
        "base_url": "https://vault.fbi.gov",
        "start_paths": ["/alphabetical-index"],
        "document_links": ["a[href*='/vault/']"],
        "document_patterns": ["\\.pdf$"],
        "pagination": { "next_selectors": ["a[rel='next']"] }
      },
      "fetch": { "use_browser": false }
    }
  }
}
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Database connection (SQLite path or PostgreSQL URL) |
| `BROWSER_URL` | Chrome DevTools URL(s), comma-separated for pool |
| `BROWSER_SELECTION` | Pool strategy: `round-robin`, `random`, `per-domain` |
| `SOCKS_PROXY` | SOCKS proxy URL |
| `FOIA_DIRECT` | Set to `1` to disable Tor |
| `LLM_PROVIDER` | `groq`, `ollama`, `openai`, or `together` |
| `LLM_MODEL` | Model name for annotation |
| `GROQ_API_KEY` | Groq API key (also enables Groq OCR backend) |
| `ANALYSIS_OCR_BACKENDS` | OCR backends (comma-separated: `groq`, `ocrs`, `paddle`, `tesseract`) |
| `MIGRATE` | Set to `true` to run migrations on container startup |
| `USER_ID` / `GROUP_ID` | Container user/group mapping |
| `RUST_LOG` | Log level (`info`, `debug`, `trace`) |

## Architecture

Rust workspace with 7 crates:

| Crate | Purpose |
|-------|---------|
| `foia` | Core library — models, config, storage, HTTP client, database, LLM, browser |
| `foia-cli` | CLI binary (`foia`) |
| `foia-scrape` | Web scraping and download |
| `foia-analysis` | OCR and text extraction |
| `foia-annotate` | LLM annotation, NER, date detection |
| `foia-import` | WARC, Concordance, URL, and stdin import |
| `foia-server` | Web interface and API (Axum) |

### Feature Flags

| Feature | Description |
|---------|-------------|
| `browser` | Chromium automation via chromiumoxide (default) |
| `postgres` | PostgreSQL backend |
| `redis-backend` | Redis-backed distributed rate limiting |
| `amqp-broker` | RabbitMQ job queue |
| `ocr-ocrs` | OCRS pure-Rust OCR |
| `ocr-paddle` | PaddleOCR ONNX backend |
| `gis` | Geographic/spatial features |

## License

MIT
