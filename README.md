# foiacquire

A command-line tool for acquiring, organizing, and searching documents from various sources.

## Features

- **Multi-source scraping**: Configurable scrapers for document repositories and archives
- **Smart rate limiting**: Adaptive delays with exponential backoff to avoid getting blocked
- **Content-addressable storage**: Documents stored by SHA-256 hash for deduplication
- **OCR support**: Extract text from scanned PDFs using Tesseract, OCRS, PaddleOCR, or DeepSeek
- **WARC import**: Import documents from Web Archive files (e.g., Archive Team dumps)
- **Full-text search**: Search across all document content and metadata
- **Web UI**: Browse and search documents through a local web interface
- **LLM annotation**: Generate summaries and tags using local LLMs (Ollama)

## Installation

Download a binary from [Releases](https://github.com/monokrome/foiacquire/releases), or build from source:

```bash
cargo install --git https://github.com/monokrome/foiacquire
```

## Quick Start

```bash
# Create a configuration (use etc/example.json as documentation)
cp etc/example.json foiacquire.json

# Initialize the database (after configuring your target directory)
foiacquire init

# List available sources
foiacquire source list

# Scrape documents from a source
foiacquire scrape <source> --limit 100

# Run OCR on downloaded documents
foiacquire ocr --workers 4

# Start web UI
foiacquire serve
```

## Commands

| Command | Description |
|---------|-------------|
| `init` | Initialize database and directories |
| `source list` | List configured sources |
| `source rename` | Rename a source (updates all documents) |
| `scrape` | Download documents from sources |
| `crawl` | Discover document URLs without downloading |
| `download` | Download documents from crawl queue |
| `import` | Import from WARC archive files |
| `ocr` | Extract text from documents |
| `ocr-compare` | Compare OCR backends on a file |
| `search` | Search documents by content |
| `ls` | List documents |
| `info` | Show document details |
| `serve` | Start web interface |
| `annotate` | Generate summaries with LLM |

## License

MIT
