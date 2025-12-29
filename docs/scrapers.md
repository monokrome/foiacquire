# Writing Scraper Configurations

This guide explains how to create custom scraper configurations for new document sources.

## Overview

Each scraper configuration defines:
1. **Discovery** - How to find document URLs
2. **Fetch** - How to download documents
3. **Browser** (optional) - Browser automation settings

## Basic Structure

```json
{
  "scrapers": {
    "source_id": {
      "discovery": { ... },
      "fetch": { ... },
      "browser": { ... },
      "refresh_ttl_days": 14
    }
  }
}
```

The `source_id` is a unique identifier used in commands like `foiacquire scrape source_id`.

## Discovery Strategies

### HTML Crawling

Best for traditional websites with paginated listings.

```json
{
  "discovery": {
    "type": "html_crawl",
    "base_url": "https://example.gov",
    "start_paths": ["/foia/documents"],
    "document_links": ["a.document-link", "a[href*='/doc/']"],
    "document_patterns": ["\\.pdf$", "\\.docx?$"],
    "pagination": {
      "next_selectors": ["a.next-page", "a[rel='next']"],
      "max_pages": 100
    },
    "max_depth": 3,
    "use_browser": false
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | Must be `"html_crawl"` |
| `base_url` | Yes | Base URL for the site |
| `start_paths` | Yes | Array of paths to begin crawling |
| `document_links` | Yes | CSS selectors for document links |
| `document_patterns` | No | Regex patterns to filter URLs |
| `pagination.next_selectors` | No | CSS selectors for "next" links |
| `pagination.max_pages` | No | Maximum pages to crawl |
| `max_depth` | No | Maximum crawl depth from start pages |
| `use_browser` | No | Use browser for discovery pages |

#### CSS Selector Tips

```json
{
  "document_links": [
    "a[href$='.pdf']",           // Links ending in .pdf
    "a[href*='/document/']",     // Links containing /document/
    "a.doc-download",            // Links with class doc-download
    ".results-list a",           // Links inside .results-list
    "table.documents td a"       // Links in table cells
  ]
}
```

#### Document Pattern Tips

```json
{
  "document_patterns": [
    "\\.pdf$",                   // PDF files
    "\\.docx?$",                 // DOC and DOCX files
    "/download/\\d+",            // Download URLs with numeric IDs
    "document_id=[0-9a-f]{32}"   // URLs with hex document IDs
  ]
}
```

### API with Page Numbers

For REST APIs using page-based pagination.

```json
{
  "discovery": {
    "type": "api_paginated",
    "api": {
      "base_url": "https://api.example.gov/v1",
      "endpoints": [
        {
          "path": "/documents",
          "params": {
            "per_page": 100,
            "status": "public"
          },
          "url_extractors": [
            {
              "path": "data.documents",
              "url_field": "download_url"
            }
          ]
        }
      ],
      "pagination": {
        "page_param": "page",
        "results_key": "data.documents",
        "page_size": 100
      }
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `api.base_url` | API base URL |
| `api.endpoints[].path` | Endpoint path |
| `api.endpoints[].params` | Query parameters |
| `api.endpoints[].url_extractors` | How to extract URLs from response |
| `api.pagination.page_param` | Query param for page number |
| `api.pagination.results_key` | JSON path to results array |
| `api.pagination.page_size` | Results per page |

### API with Cursor Pagination

For APIs using cursor/token-based pagination.

```json
{
  "discovery": {
    "type": "api_cursor",
    "api": {
      "base_url": "https://api.example.gov/v2",
      "endpoints": [
        {
          "path": "/search",
          "params": {
            "q": "FOIA",
            "limit": 100
          },
          "url_extractors": [
            {
              "path": "results",
              "url_field": "file.url"
            }
          ]
        }
      ],
      "pagination": {
        "cursor_param": "cursor",
        "cursor_path": "pagination.next_cursor",
        "results_key": "results"
      }
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `pagination.cursor_param` | Query param for cursor token |
| `pagination.cursor_path` | JSON path to next cursor in response |

### URL Extractors

Extract document URLs from API responses:

```json
{
  "url_extractors": [
    {
      "path": "data.items",           // JSON path to array
      "url_field": "download_url"     // Field containing URL
    },
    {
      "path": "attachments",
      "url_field": "file.href",       // Nested field
      "title_field": "file.name"      // Optional title field
    }
  ]
}
```

## Fetch Configuration

### Basic HTTP Fetch

```json
{
  "fetch": {
    "use_browser": false,
    "headers": {
      "Accept": "application/pdf",
      "X-API-Key": "your-key"
    }
  }
}
```

### Browser-Based Fetch

For JavaScript-rendered content or download buttons:

```json
{
  "fetch": {
    "use_browser": true,
    "pdf_selectors": ["a.download-pdf", "button.export-pdf"],
    "title_selectors": ["h1.document-title", "meta[property='og:title']"]
  }
}
```

| Field | Description |
|-------|-------------|
| `use_browser` | Enable browser for downloads |
| `pdf_selectors` | CSS selectors for PDF download links |
| `title_selectors` | CSS selectors for document title |

## Browser Configuration

### Standard Browser

```json
{
  "browser": {
    "enabled": true,
    "engine": "standard",
    "headless": true,
    "timeout": 30
  }
}
```

### Stealth Mode

For sites with bot detection:

```json
{
  "browser": {
    "enabled": true,
    "engine": "stealth",
    "headless": true,
    "timeout": 60,
    "wait_for_selector": ".content-loaded"
  }
}
```

### With Authentication

Using saved cookies:

```json
{
  "browser": {
    "enabled": true,
    "engine": "cookies",
    "cookies_file": "./site_cookies.json"
  }
}
```

To get cookies:
```bash
foiacquire browser-test https://example.gov/login --headed --save-cookies cookies.json
```

### Remote Browser

Using a separate Chrome container:

```json
{
  "browser": {
    "enabled": true,
    "remote_url": "ws://chromium:9222"
  }
}
```

### With Proxy

```json
{
  "browser": {
    "enabled": true,
    "proxy": "socks5://127.0.0.1:1080"
  }
}
```

## Complete Examples

### FBI Vault

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

### CIA Reading Room (Browser Required)

```json
{
  "scrapers": {
    "cia_foia": {
      "discovery": {
        "type": "html_crawl",
        "base_url": "https://www.cia.gov/readingroom",
        "start_paths": ["/collection"],
        "document_links": ["a[href*='/document/']"],
        "document_patterns": ["\\.pdf$"],
        "use_browser": true,
        "pagination": {
          "next_selectors": ["a[rel='next']", ".pager-next a"]
        }
      },
      "fetch": {
        "use_browser": true,
        "pdf_selectors": ["a[href$='.pdf']"],
        "title_selectors": ["h1.page-title", "h1"]
      },
      "browser": {
        "enabled": true,
        "engine": "stealth",
        "timeout": 45,
        "wait_for_selector": ".document-content"
      }
    }
  }
}
```

### MuckRock API

```json
{
  "scrapers": {
    "muckrock": {
      "discovery": {
        "type": "api_paginated",
        "api": {
          "base_url": "https://www.muckrock.com/api_v1",
          "endpoints": [
            {
              "path": "/foia/",
              "params": {
                "page_size": 100,
                "status": "done"
              },
              "url_extractors": [
                {
                  "path": "results",
                  "url_field": "communications"
                }
              ]
            }
          ],
          "pagination": {
            "page_param": "page",
            "results_key": "results",
            "page_size": 100
          }
        }
      },
      "fetch": {
        "use_browser": false
      }
    }
  }
}
```

### Protected Site with Login

```json
{
  "scrapers": {
    "protected_archive": {
      "discovery": {
        "type": "html_crawl",
        "base_url": "https://protected.example.gov",
        "start_paths": ["/archives"],
        "document_links": ["a.document"],
        "document_patterns": ["\\.pdf$"],
        "use_browser": true
      },
      "fetch": {
        "use_browser": true
      },
      "browser": {
        "enabled": true,
        "engine": "stealth",
        "cookies_file": "./protected_cookies.json",
        "timeout": 60
      }
    }
  }
}
```

## Testing Your Configuration

### 1. Test Discovery

```bash
# Dry run to see what would be found
foiacquire crawl my_source --limit 10
```

### 2. Test Browser Setup

```bash
# Test browser connectivity
foiacquire browser-test https://example.gov/test-page --headed
```

### 3. Test Download

```bash
# Download a few documents
foiacquire download my_source --limit 5
```

### 4. Check Logs

```bash
RUST_LOG=debug foiacquire scrape my_source --limit 1
```

## Troubleshooting

### No Documents Found

1. Check CSS selectors in browser DevTools
2. Verify `base_url` doesn't have trailing slash
3. Try with `use_browser: true` if site uses JavaScript
4. Check `document_patterns` regex syntax

### Rate Limited / Blocked

1. Increase `request_delay_ms` in global config
2. Use `engine: "stealth"` for bot detection
3. Add realistic `user_agent` string
4. Use proxy if needed

### Authentication Issues

1. Export fresh cookies from browser
2. Check cookie expiration
3. Use `context_url` to visit login page first
4. Try `--headed` mode to debug

### Timeout Errors

1. Increase `browser.timeout`
2. Add `wait_for_selector` for slow pages
3. Check network connectivity
4. Try without proxy
