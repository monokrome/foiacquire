# Docker Deployment

Run foia in containers for easy deployment and isolation.

## Quick Start

```bash
# Pull the image
docker pull monokrome/foia:latest

# Run with a local data directory
docker run -v ./foia-data:/opt/foia \
  -e USER_ID=$(id -u) -e GROUP_ID=$(id -g) \
  monokrome/foia:latest scrape fbi_vault --limit 100
```

## Images

### Application Images

| Image | Description |
|-------|-------------|
| `monokrome/foia:latest` | Main application (Alpine-based) |
| `monokrome/foia:tesseract` | With Tesseract OCR included |
| `monokrome/foia:redis` | With Redis rate limiting support |

### Chromium Images

| Image | Description |
|-------|-------------|
| `monokrome/chromium:latest` | Standard Chromium for browser automation |
| `monokrome/chromium:stealth` | Chromium with anti-bot detection patches |

Both Chromium images support VNC for remote viewing (see [VNC Support](#vnc-support) below).

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `USER_ID` | `1000` | UID to run as inside container |
| `GROUP_ID` | `1000` | GID to run as inside container |
| `DATA_DIR` | `/opt/foia` | Data directory inside container |
| `DATABASE_URL` | - | Database connection string |
| `BROWSER_URL` | - | Remote Chrome DevTools URL |
| `RUST_LOG` | `info` | Log level |
| `MIGRATE` | `false` | Run database migrations on container start |

## Volume Mounts

The container expects data at `/opt/foia`:

```bash
docker run -v /path/to/data:/opt/foia ...
```

The directory should contain:
- `foia.json` - Configuration file
- `foia.db` - SQLite database (created automatically)
- `documents/` - Downloaded files (created automatically)

## Docker Compose

### Basic Setup (SQLite)

```yaml
version: '3.8'

services:
  foia:
    image: monokrome/foia:latest
    volumes:
      - ./data:/opt/foia
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - RUST_LOG=info
    command: scrape fbi_vault --daemon --interval 3600
```

### With PostgreSQL

```yaml
version: '3.8'

services:
  foia:
    image: monokrome/foia:latest
    volumes:
      - ./data:/opt/foia
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - DATABASE_URL=postgres://foia:secret@postgres:5432/foia
      - RUST_LOG=info
    depends_on:
      postgres:
        condition: service_healthy
    command: scrape --all --daemon --interval 3600

  postgres:
    image: postgres:16-alpine
    volumes:
      - postgres_data:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=foia
      - POSTGRES_PASSWORD=secret
      - POSTGRES_DB=foia
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U foia"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

### With Browser Automation

```yaml
version: '3.8'

services:
  foia:
    image: monokrome/foia:latest
    volumes:
      - ./data:/opt/foia
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - BROWSER_URL=ws://chromium:9222
      - RUST_LOG=info
    depends_on:
      - chromium
    command: scrape cia_foia --limit 100

  chromium:
    image: monokrome/chromium:stealth
    shm_size: 2g
    # No ports exposed - internal only
```

### Full Stack with Web UI

```yaml
version: '3.8'

services:
  scraper:
    image: monokrome/foia:latest
    volumes:
      - ./data:/opt/foia
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - DATABASE_URL=postgres://foia:secret@postgres:5432/foia
      - BROWSER_URL=ws://chromium:9222
      - RUST_LOG=info
    depends_on:
      postgres:
        condition: service_healthy
      chromium:
        condition: service_started
    command: scrape --all --daemon --interval 3600

  web:
    image: monokrome/foia:latest
    volumes:
      - ./data:/opt/foia:ro
    environment:
      - DATABASE_URL=postgres://foia:secret@postgres:5432/foia
    ports:
      - "3030:3030"
    depends_on:
      postgres:
        condition: service_healthy
    command: serve 0.0.0.0:3030

  analyzer:
    image: monokrome/foia:tesseract
    volumes:
      - ./data:/opt/foia
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - DATABASE_URL=postgres://foia:secret@postgres:5432/foia
      - RUST_LOG=info
    depends_on:
      postgres:
        condition: service_healthy
    command: analyze --daemon --interval 1800 --workers 2

  chromium:
    image: monokrome/chromium:stealth
    shm_size: 2g

  postgres:
    image: postgres:16-alpine
    volumes:
      - postgres_data:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=foia
      - POSTGRES_PASSWORD=secret
      - POSTGRES_DB=foia
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U foia"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

## VNC Support

The Chromium containers support VNC for remote viewing of browser activity, useful for debugging or monitoring scrapes.

### Enabling VNC

Set `VNC_PASSWORD` to enable VNC on port 5900:

```bash
docker run -d --name chromium \
  --shm-size=2g \
  -p 9222:9222 \
  -p 5900:5900 \
  -e VNC_PASSWORD=mysecretpassword \
  monokrome/chromium:stealth
```

### VNC Options

| Variable | Description |
|----------|-------------|
| `VNC_PASSWORD` | Enable VNC with this password (required for VNC) |
| `VNC_VIEWONLY` | Set to `true` for read-only VNC (default: interactive) |

### Docker Compose with VNC

```yaml
version: '3.8'

services:
  chromium:
    image: monokrome/chromium:stealth
    shm_size: 2g
    ports:
      - "9222:9222"
      - "5900:5900"
    environment:
      - VNC_PASSWORD=mysecretpassword
      - VNC_VIEWONLY=true
```

### Connecting

Use any VNC client (TigerVNC, RealVNC, macOS Screen Sharing) to connect:

```
vnc://localhost:5900
```

## Synology NAS

### Container Manager Setup

1. **Download the image:**
   - Registry → Search for `monokrome/foia`
   - Download the `latest` tag

2. **Create the container:**
   - Image → Select foia → Launch
   - **General Settings:**
     - Container Name: `foia-scraper`
     - Enable auto-restart: Yes

3. **Volume Settings:**
   - Add folder: `/volume1/docker/foia` → `/opt/foia`
   - Ensure the folder has correct permissions

4. **Environment Variables:**
   ```
   USER_ID=1024          # Match your Synology user
   GROUP_ID=100          # Usually 'users' group
   DATA_DIR=/opt/foia
   RUST_LOG=info
   ```

5. **Execution Command:**
   ```
   scrape fbi_vault --daemon --interval 3600
   ```

### With PostgreSQL on Synology

If using the Synology PostgreSQL package or a container:

```
DATABASE_URL=postgres://foia:password@localhost:5432/foia
```

### Viewing Logs

In Container Manager:
- Container → foia-scraper → Log

Or via SSH:
```bash
docker logs foia-scraper -f
```

### Troubleshooting Permissions

If you see permission errors:

1. Check the host directory ownership:
   ```bash
   ls -la /volume1/docker/foia
   ```

2. Ensure `USER_ID` matches the directory owner:
   ```bash
   id username  # Find the UID
   ```

3. Set environment variable to match:
   ```
   USER_ID=1024
   GROUP_ID=100
   ```

## Building Custom Images

The Dockerfile supports build-time arguments for customization:

### Build Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `FEATURES` | `browser,postgres` | Cargo features to enable |
| `WITH_TESSERACT` | `false` | Include Tesseract OCR |
| `WITH_TOR` | `false` | Include Tor and Snowflake |

### Examples

```bash
# Default build (browser + postgres)
docker build -t foia:local .

# With Tesseract OCR
docker build --build-arg WITH_TESSERACT=true -t foia:tesseract .

# With Tor support for privacy routing
docker build --build-arg WITH_TOR=true -t foia:tor .

# Minimal build (no browser, no postgres)
docker build --build-arg FEATURES="" -t foia:minimal .

# With Redis rate limiting
docker build --build-arg FEATURES="browser,postgres,redis-backend" -t foia:redis .

# Full build with everything
docker build \
  --build-arg FEATURES="browser,postgres,redis-backend" \
  --build-arg WITH_TESSERACT=true \
  --build-arg WITH_TOR=true \
  -t foia:full .
```

### Multi-Platform Builds

For cross-platform images (amd64 + arm64):

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg WITH_TESSERACT=true \
  -t myregistry/foia:custom \
  --push .
```

### With Additional OCR Languages

```dockerfile
FROM monokrome/foia:tesseract

# Add more Tesseract languages
RUN apk add --no-cache \
    tesseract-ocr-data-deu \
    tesseract-ocr-data-fra \
    tesseract-ocr-data-spa
```

## Health Checks

Add health checks to your compose file:

```yaml
services:
  foia:
    # ...
    healthcheck:
      test: ["CMD", "foia", "status"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

## Resource Limits

```yaml
services:
  foia:
    # ...
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M
```

## Networking

For multiple containers that need to communicate:

```yaml
services:
  foia:
    networks:
      - foia-net

  postgres:
    networks:
      - foia-net

networks:
  foia-net:
    driver: bridge
```
