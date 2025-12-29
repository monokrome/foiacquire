# Configuration Reference

foiacquire uses JSON configuration files for defining scrapers and application settings.

## Configuration File Location

The configuration file is discovered in this order:

1. `--config` flag (explicit path)
2. `foiacquire.json` or `foiacquire.toml` next to the database
3. Configuration stored in database history
4. Standard config locations (`~/.config/foiacquire/`, etc.)

## Global Settings

```json
{
  "target": "./foia_documents/",
  "database": "foiacquire.db",
  "user_agent": "FOIAcquire/0.1 (academic research)",
  "request_timeout": 30,
  "request_delay_ms": 500,
  "default_refresh_ttl_days": 14,
  "rate_limit_backend": null,
  "broker_url": null,
  "llm": { ... },
  "scrapers": { ... }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `target` | string | `~/Documents/foia/` | Base directory for data and documents |
| `database` | string | `foiacquire.db` | Database filename (SQLite) |
| `user_agent` | string | `FOIAcquire/0.1...` | HTTP User-Agent header |
| `request_timeout` | integer | `30` | HTTP request timeout in seconds |
| `request_delay_ms` | integer | `500` | Delay between requests in milliseconds |
| `default_refresh_ttl_days` | integer | `14` | Days before re-checking fetched URLs |
| `rate_limit_backend` | string | `null` | Rate limit backend: `null` (memory), `"sqlite"`, or `"redis://host:port"` |
| `broker_url` | string | `null` | Job queue broker: `null` (local) or `"amqp://host:port"` |

## Environment Variables

Environment variables override configuration file settings:

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Full database URL, e.g., `postgres://user:pass@host/db` or `sqlite:path.db` |
| `BROWSER_URL` | Remote Chrome DevTools WebSocket URL |
| `LLM_ENABLED` | Enable/disable LLM (`true`/`false`) |
| `LLM_ENDPOINT` | Ollama API endpoint URL |
| `LLM_MODEL` | Model name for annotation |
| `LLM_MAX_TOKENS` | Maximum tokens in response |
| `LLM_TEMPERATURE` | Generation temperature |
| `LLM_MAX_CONTENT_CHARS` | Max document chars to send |
| `LLM_SYNOPSIS_PROMPT` | Custom synopsis prompt template |
| `LLM_TAGS_PROMPT` | Custom tags prompt template |
| `RUST_LOG` | Log level (`error`, `warn`, `info`, `debug`, `trace`) |

## LLM Configuration

Configure Ollama integration for document annotation:

```json
{
  "llm": {
    "enabled": true,
    "endpoint": "http://localhost:11434",
    "model": "llama3.2",
    "max_tokens": 512,
    "temperature": 0.3,
    "max_content_chars": 12000,
    "synopsis_prompt": "Summarize this document:\n\nTitle: {title}\n\nContent:\n{content}",
    "tags_prompt": "Generate 3-5 tags for this document..."
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable LLM annotation |
| `endpoint` | string | `http://localhost:11434` | Ollama API endpoint |
| `model` | string | `dolphin-llama3:8b` | Model to use |
| `max_tokens` | integer | `512` | Maximum response tokens |
| `temperature` | float | `0.3` | Generation temperature (0-1) |
| `max_content_chars` | integer | `12000` | Max chars sent to LLM |
| `synopsis_prompt` | string | (built-in) | Synopsis prompt with `{title}` and `{content}` placeholders |
| `tags_prompt` | string | (built-in) | Tags prompt template |

## Scraper Configuration

Each scraper is defined under the `scrapers` object with a unique ID:

```json
{
  "scrapers": {
    "my_source": {
      "discovery": { ... },
      "fetch": { ... },
      "browser": { ... },
      "refresh_ttl_days": 7
    }
  }
}
```

### Discovery Configuration

#### HTML Crawling

```json
{
  "discovery": {
    "type": "html_crawl",
    "base_url": "https://example.gov/foia",
    "start_paths": ["/documents", "/releases"],
    "document_links": ["a[href*='/doc/']", "a.document-link"],
    "document_patterns": ["\\.pdf$", "\\.doc$"],
    "use_browser": false,
    "max_depth": 3,
    "pagination": {
      "next_selectors": ["a.next", "a[rel='next']"],
      "max_pages": 100
    }
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | `"html_crawl"` for HTML page crawling |
| `base_url` | string | Base URL for the source |
| `start_paths` | array | Paths to begin crawling from |
| `document_links` | array | CSS selectors for document links |
| `document_patterns` | array | Regex patterns to match document URLs |
| `use_browser` | boolean | Use browser for discovery pages |
| `max_depth` | integer | Maximum crawl depth |
| `pagination.next_selectors` | array | CSS selectors for "next page" links |
| `pagination.max_pages` | integer | Maximum pages to crawl |

#### API Pagination

```json
{
  "discovery": {
    "type": "api_paginated",
    "api": {
      "base_url": "https://api.example.com",
      "endpoints": [
        {
          "path": "/documents",
          "params": {
            "per_page": 100,
            "status": "published"
          },
          "url_extractors": [
            {
              "path": "results",
              "url_field": "download_url"
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
  }
}
```

#### Cursor-based API

```json
{
  "discovery": {
    "type": "api_cursor",
    "api": {
      "base_url": "https://api.example.com",
      "endpoints": [
        {
          "path": "/search",
          "params": { "q": "FOIA" },
          "url_extractors": [
            {
              "path": "data.items",
              "url_field": "file_url"
            }
          ]
        }
      ],
      "pagination": {
        "cursor_param": "cursor",
        "cursor_path": "meta.next_cursor",
        "results_key": "data.items"
      }
    }
  }
}
```

### Fetch Configuration

```json
{
  "fetch": {
    "use_browser": false,
    "headers": {
      "Accept": "application/pdf"
    },
    "pdf_selectors": ["a[href$='.pdf']"],
    "title_selectors": ["h1.title", "h1", "title"]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `use_browser` | boolean | Use browser automation for downloads |
| `headers` | object | Custom HTTP headers |
| `pdf_selectors` | array | CSS selectors for PDF links on document pages |
| `title_selectors` | array | CSS selectors for document title extraction |

### Browser Configuration

```json
{
  "browser": {
    "enabled": true,
    "engine": "stealth",
    "headless": true,
    "proxy": "socks5://127.0.0.1:1080",
    "cookies_file": "./cookies.json",
    "timeout": 30,
    "wait_for_selector": ".document-loaded",
    "remote_url": "ws://localhost:9222"
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable browser for this source |
| `engine` | string | `"standard"` | Engine: `"standard"`, `"stealth"`, `"cookies"` |
| `headless` | boolean | `true` | Run browser in headless mode |
| `proxy` | string | `null` | Proxy URL (HTTP, SOCKS5) |
| `cookies_file` | string | `null` | Path to cookies JSON file |
| `timeout` | integer | `30` | Page load timeout in seconds |
| `wait_for_selector` | string | `null` | Wait for element before proceeding |
| `remote_url` | string | `null` | Remote Chrome DevTools URL |

#### Browser Engines

- **standard** - Regular Chromium, no special handling
- **stealth** - Anti-bot detection patches applied
- **cookies** - Load cookies and use regular HTTP (faster for authenticated sites)

## Database Configuration

### SQLite (Default)

SQLite is used by default. The database file is created in the target directory:

```json
{
  "target": "./foia-data/",
  "database": "foiacquire.db"
}
```

### PostgreSQL

Set `DATABASE_URL` environment variable:

```bash
export DATABASE_URL="postgres://user:password@localhost:5432/foiacquire"
```

Or in Docker:

```bash
docker run -e DATABASE_URL=postgres://... foiacquire scrape
```

PostgreSQL requires the `postgres` feature at build time.

## Rate Limiting

### In-Memory (Default)

Rate limits are tracked per-process and reset on restart:

```json
{
  "rate_limit_backend": null
}
```

### SQLite Persistence

Persist rate limits across restarts:

```json
{
  "rate_limit_backend": "sqlite"
}
```

### Redis (Distributed)

Share rate limits across multiple processes:

```json
{
  "rate_limit_backend": "redis://localhost:6379"
}
```

Requires the `redis-backend` feature.

## Complete Example

```json
{
  "target": "./foia_documents/",
  "user_agent": "ResearchBot/1.0 (university.edu; research@university.edu)",
  "request_timeout": 60,
  "request_delay_ms": 1000,
  "default_refresh_ttl_days": 30,
  "rate_limit_backend": "sqlite",
  "llm": {
    "enabled": true,
    "endpoint": "http://localhost:11434",
    "model": "llama3.2",
    "temperature": 0.2
  },
  "scrapers": {
    "fbi_vault": {
      "refresh_ttl_days": 7,
      "discovery": {
        "type": "html_crawl",
        "base_url": "https://vault.fbi.gov",
        "start_paths": ["/alphabetical-index"],
        "document_links": ["a[href*='/vault/']"],
        "document_patterns": ["\\.pdf$"],
        "pagination": {
          "next_selectors": ["a[rel='next']"]
        }
      },
      "fetch": {
        "use_browser": false
      }
    },
    "protected_site": {
      "discovery": {
        "type": "html_crawl",
        "base_url": "https://protected.example.gov",
        "start_paths": ["/documents"],
        "document_links": ["a.doc-link"],
        "use_browser": true
      },
      "fetch": {
        "use_browser": true
      },
      "browser": {
        "enabled": true,
        "engine": "stealth",
        "cookies_file": "./protected_cookies.json",
        "wait_for_selector": ".documents-loaded"
      }
    }
  }
}
```
