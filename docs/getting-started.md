# Getting Started

This guide walks you through setting up foiacquire and downloading your first documents.

## Prerequisites

- A terminal/command line
- ~500MB disk space for the binary and initial data
- For OCR: `tesseract` and `poppler-utils` (pdftotext) installed on your system

### Installing OCR Dependencies

**Arch Linux:**
```bash
sudo pacman -S tesseract tesseract-data-eng poppler
```

**Ubuntu/Debian:**
```bash
sudo apt install tesseract-ocr tesseract-ocr-eng poppler-utils
```

**macOS:**
```bash
brew install tesseract poppler
```

**Windows:**
Download Tesseract from [UB-Mannheim](https://github.com/UB-Mannheim/tesseract/wiki) and add to PATH.

## Installation

### Pre-built Binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/monokrome/foiacquire/releases):

- `foiacquire-linux-x86_64.tar.gz` - Linux (Intel/AMD)
- `foiacquire-linux-aarch64.tar.gz` - Linux (ARM64)
- `foiacquire-macos-x86_64.tar.gz` - macOS (Intel)
- `foiacquire-macos-aarch64.tar.gz` - macOS (Apple Silicon)
- `foiacquire-windows-x86_64.zip` - Windows

Extract and move to your PATH:

```bash
tar -xzf foiacquire-linux-x86_64.tar.gz
sudo mv foiacquire /usr/local/bin/
```

### Building from Source

Requires Rust 1.70+:

```bash
# Clone and build
git clone https://github.com/monokrome/foiacquire
cd foiacquire
cargo build --release

# The binary is at target/release/foiacquire
```

## Initial Setup

### 1. Create a Data Directory

Choose where you want to store documents:

```bash
mkdir ~/foia-documents
cd ~/foia-documents
```

### 2. Initialize the Database

```bash
foiacquire init --target .
```

This creates:
- `foiacquire.db` - SQLite database for metadata
- `documents/` - Directory for downloaded files

### 3. Create a Configuration File

Create `foiacquire.json` in your data directory:

```json
{
  "target": ".",
  "scrapers": {
    "fbi_vault": {
      "discovery": {
        "type": "html_crawl",
        "base_url": "https://vault.fbi.gov",
        "start_paths": ["/alphabetical-index"],
        "document_links": ["a[href*='/vault/']"],
        "document_patterns": ["\\.pdf$"],
        "pagination": {
          "next_selectors": ["a[rel='next']", "a.pager-next"]
        }
      },
      "fetch": {
        "use_browser": false
      }
    }
  }
}
```

Or copy the example configuration:

```bash
cp /path/to/foiacquire/etc/example.json foiacquire.json
```

### 4. Verify Setup

```bash
# List configured sources
foiacquire source list

# Check OCR tools
foiacquire analyze-check
```

## Your First Scrape

### Discover Documents

First, crawl to find document URLs without downloading:

```bash
foiacquire crawl fbi_vault --limit 50
```

This discovers documents and adds them to the download queue.

### Download Documents

```bash
foiacquire download fbi_vault --workers 4 --limit 50
```

Or combine both steps:

```bash
foiacquire scrape fbi_vault --workers 4 --limit 50
```

### Monitor Progress

The CLI shows real-time progress. You can also check status:

```bash
foiacquire state status fbi_vault
```

## Processing Documents

### Extract Text with OCR

```bash
# Process all documents needing OCR
foiacquire analyze --workers 4

# Process specific source
foiacquire analyze fbi_vault --workers 4 --limit 100
```

### Generate Summaries (Optional)

Requires [Ollama](https://ollama.ai/) running locally:

```bash
# Start Ollama and pull a model
ollama pull llama3.2

# Generate summaries
foiacquire annotate --limit 50
```

## Browsing Documents

### Web Interface

Start the built-in web server:

```bash
foiacquire serve
```

Open http://localhost:3030 in your browser to:
- Browse all documents
- Search by content or metadata
- View document details and extracted text
- Filter by source, type, or tags

### Command Line

```bash
# List recent documents
foiacquire ls --limit 20

# Search documents
foiacquire search "classified"

# View document details
foiacquire info <document_id>

# Read document content
foiacquire read <document_id> --text
```

## Next Steps

- [Configuration Reference](configuration.md) - All configuration options
- [Commands Reference](commands.md) - Detailed command documentation
- [Writing Scrapers](scrapers.md) - Create custom scraper configurations
- [Docker Deployment](docker.md) - Run in containers
