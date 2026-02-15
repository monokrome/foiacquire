# Getting Started

This guide walks you through setting up foia and downloading your first documents.

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

Download the latest release for your platform from [GitHub Releases](https://github.com/foiacquire/foia/releases):

- `foia-linux-x86_64.tar.gz` - Linux (Intel/AMD)
- `foia-linux-aarch64.tar.gz` - Linux (ARM64)
- `foia-macos-x86_64.tar.gz` - macOS (Intel)
- `foia-macos-aarch64.tar.gz` - macOS (Apple Silicon)
- `foia-windows-x86_64.zip` - Windows

Extract and move to your PATH:

```bash
tar -xzf foia-linux-x86_64.tar.gz
sudo mv foia /usr/local/bin/
```

### Building from Source

Requires Rust 1.70+:

```bash
# Clone and build
git clone https://github.com/foiacquire/foia
cd foia
cargo build --release

# The binary is at target/release/foia
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
foia init --target .
```

This creates:
- `foia.db` - SQLite database for metadata
- `documents/` - Directory for downloaded files

### 3. Create a Configuration File

Create `foia.json` in your data directory:

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
cp /path/to/foia/etc/example.json foia.json
```

### 4. Verify Setup

```bash
# List configured sources
foia source list

# Check OCR tools
foia analyze-check
```

## Your First Scrape

### Discover Documents

First, crawl to find document URLs without downloading:

```bash
foia crawl fbi_vault --limit 50
```

This discovers documents and adds them to the download queue.

### Download Documents

```bash
foia download fbi_vault --workers 4 --limit 50
```

Or combine both steps:

```bash
foia scrape fbi_vault --workers 4 --limit 50
```

### Monitor Progress

The CLI shows real-time progress. You can also check status:

```bash
foia state status fbi_vault
```

## Processing Documents

### Extract Text with OCR

```bash
# Process all documents needing OCR
foia analyze --workers 4

# Process specific source
foia analyze fbi_vault --workers 4 --limit 100
```

### Generate Summaries (Optional)

Requires [Ollama](https://ollama.ai/) running locally:

```bash
# Start Ollama and pull a model
ollama pull llama3.2

# Generate summaries
foia annotate --limit 50
```

## Browsing Documents

### Web Interface

Start the built-in web server:

```bash
foia serve
```

Open http://localhost:3030 in your browser to:
- Browse all documents
- Search by content or metadata
- View document details and extracted text
- Filter by source, type, or tags

### Command Line

```bash
# List recent documents
foia ls --limit 20

# Search documents
foia search "classified"

# View document details
foia info <document_id>

# Read document content
foia read <document_id> --text
```

## Next Steps

- [Configuration Reference](configuration.md) - All configuration options
- [Commands Reference](commands.md) - Detailed command documentation
- [Writing Scrapers](scrapers.md) - Create custom scraper configurations
- [Docker Deployment](docker.md) - Run in containers
