# foiacquire

A command-line tool for acquiring, organizing, and searching FOIA documents from government archives and other sources.

## Features

- **Multi-source scraping** - Configurable scrapers for FBI Vault, CIA Reading Room, MuckRock, DocumentCloud, and custom sources
- **Smart rate limiting** - Adaptive delays with exponential backoff to avoid blocks, with optional Redis backend for distributed deployments
- **Content-addressable storage** - Documents stored by SHA-256 + BLAKE3 hash for deduplication
- **Multiple OCR backends** - Tesseract (default), OCRS (pure Rust), PaddleOCR (GPU), or DeepSeek (VLM)
- **Browser automation** - Chromium-based scraping for JavaScript-heavy sites with stealth mode for bot detection bypass
- **WARC import** - Import documents from Web Archive files with filtering and checkpointing
- **Full-text search** - Search across document content and metadata
- **Web UI** - Browse, search, and view documents through a local web interface
- **LLM annotation** - Generate summaries and tags using Ollama
- **Database flexibility** - SQLite (default) or PostgreSQL for larger deployments
- **Docker support** - Pre-built images for easy deployment

## Installation

Download a pre-built binary from [Releases](https://github.com/monokrome/foiacquire/releases), or build from source:

```bash
# Default build (SQLite + browser automation)
cargo install --git https://github.com/monokrome/foiacquire

# With PostgreSQL support
cargo install --git https://github.com/monokrome/foiacquire --features postgres

# With all OCR backends
cargo install --git https://github.com/monokrome/foiacquire --features ocr-all
```

### Feature Flags

| Feature | Description |
|---------|-------------|
| `browser` | Browser automation via Chromium (default) |
| `postgres` | PostgreSQL database support |
| `redis-backend` | Redis for distributed rate limiting |
| `ocr-ocrs` | OCRS pure-Rust OCR backend |
| `ocr-paddle` | PaddleOCR CNN-based backend |
| `ocr-all` | All OCR backends |

## Quick Start

```bash
# Initialize with a target directory
foiacquire init --target ./foia-data

# Or use an existing config
cp etc/example.json foiacquire.json
foiacquire init

# List configured sources
foiacquire source list

# Scrape documents (crawl + download)
foiacquire scrape fbi_vault --limit 100

# Run OCR on downloaded documents
foiacquire analyze --workers 4

# Generate summaries with LLM (requires Ollama)
foiacquire annotate --limit 50

# Start web UI
foiacquire serve
# Open http://localhost:3030
```

## Commands

### Document Acquisition

| Command | Description |
|---------|-------------|
| `scrape <source>` | Crawl and download documents from a source |
| `crawl <source>` | Discover document URLs without downloading |
| `download [source]` | Download documents from the crawl queue |
| `import <files...>` | Import from WARC archive files |
| `refresh [source]` | Re-fetch metadata for existing documents |

### Document Processing

| Command | Description |
|---------|-------------|
| `analyze [source]` | Extract text and run OCR on documents |
| `analyze-check` | Verify OCR tools are installed |
| `analyze-compare <file>` | Compare OCR backends on a file |
| `annotate [source]` | Generate summaries/tags with LLM |
| `detect-dates [source]` | Detect publication dates in documents |
| `archive [source]` | Extract contents from ZIP/email attachments |

### Browsing & Search

| Command | Description |
|---------|-------------|
| `ls` | List documents with filtering |
| `info <doc_id>` | Show document metadata |
| `read <doc_id>` | Output document content |
| `search <query>` | Full-text search |
| `serve [bind]` | Start web interface (default: 127.0.0.1:3030) |

### Management

| Command | Description |
|---------|-------------|
| `init` | Initialize database and directories |
| `source list` | List configured sources |
| `source rename` | Rename a source |
| `config recover` | Recover config from database |
| `config history` | Show configuration history |
| `db copy <from> <to>` | Migrate between SQLite/PostgreSQL |
| `state status` | Show crawl state |
| `state clear <source>` | Reset crawl state |

## Configuration

Create a `foiacquire.json` in your data directory or use `--config`:

```json
{
  "target": "./foia_documents/",
  "scrapers": {
    "my_source": {
      "discovery": {
        "type": "html_crawl",
        "base_url": "https://example.gov/foia",
        "start_paths": ["/documents"],
        "document_links": ["a[href*='/doc/']"],
        "document_patterns": ["\\.pdf$"],
        "pagination": {
          "next_selectors": ["a.next-page"]
        }
      },
      "fetch": {
        "use_browser": false
      }
    }
  }
}
```

See [docs/configuration.md](docs/configuration.md) for full options.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Database connection (e.g., `postgres://user:pass@host/db`) |
| `BROWSER_URL` | Remote Chrome DevTools URL (e.g., `ws://localhost:9222`) |
| `LLM_ENDPOINT` | Ollama API endpoint |
| `LLM_MODEL` | Model for annotation |
| `RUST_LOG` | Log level (e.g., `debug`, `info`) |

## Docker

```bash
# Run with local data directory
docker run -v ./foia-data:/opt/foiacquire \
  -e USER_ID=$(id -u) -e GROUP_ID=$(id -g) \
  ghcr.io/monokrome/foiacquire:latest scrape fbi_vault

# With PostgreSQL
docker run -v ./foia-data:/opt/foiacquire \
  -e DATABASE_URL=postgres://user:pass@host/foiacquire \
  ghcr.io/monokrome/foiacquire:latest scrape fbi_vault

# Start web UI
docker run -v ./foia-data:/opt/foiacquire \
  -p 3030:3030 \
  ghcr.io/monokrome/foiacquire:latest serve 0.0.0.0:3030
```

See [docs/docker.md](docs/docker.md) for Docker Compose examples and Synology setup.

## Documentation

- [Getting Started](docs/getting-started.md) - First-time setup guide
- [Configuration](docs/configuration.md) - All configuration options
- [Commands](docs/commands.md) - Detailed command reference
- [Scrapers](docs/scrapers.md) - Writing custom scraper configs
- [Docker Deployment](docs/docker.md) - Container deployment guide

## License

MIT
