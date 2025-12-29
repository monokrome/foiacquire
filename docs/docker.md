# Docker Deployment

Run foiacquire in containers for easy deployment and isolation.

## Quick Start

```bash
# Pull the image
docker pull ghcr.io/monokrome/foiacquire:latest

# Run with a local data directory
docker run -v ./foia-data:/opt/foiacquire \
  -e USER_ID=$(id -u) -e GROUP_ID=$(id -g) \
  ghcr.io/monokrome/foiacquire:latest scrape fbi_vault --limit 100
```

## Images

| Image | Description |
|-------|-------------|
| `ghcr.io/monokrome/foiacquire:latest` | Main application (Alpine-based) |
| `ghcr.io/monokrome/foiacquire:latest-tesseract` | With Tesseract OCR included |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `USER_ID` | `1000` | UID to run as inside container |
| `GROUP_ID` | `1000` | GID to run as inside container |
| `TARGET_PATH` | `/opt/foiacquire` | Data directory inside container |
| `DATABASE_URL` | - | Database connection string |
| `BROWSER_URL` | - | Remote Chrome DevTools URL |
| `RUST_LOG` | `info` | Log level |
| `MIGRATE` | `false` | Run database migrations on container start |

## Volume Mounts

The container expects data at `/opt/foiacquire`:

```bash
docker run -v /path/to/data:/opt/foiacquire ...
```

The directory should contain:
- `foiacquire.json` - Configuration file
- `foiacquire.db` - SQLite database (created automatically)
- `documents/` - Downloaded files (created automatically)

## Docker Compose

### Basic Setup (SQLite)

```yaml
version: '3.8'

services:
  foiacquire:
    image: ghcr.io/monokrome/foiacquire:latest
    volumes:
      - ./data:/opt/foiacquire
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
  foiacquire:
    image: ghcr.io/monokrome/foiacquire:latest
    volumes:
      - ./data:/opt/foiacquire
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - DATABASE_URL=postgres://foiacquire:secret@postgres:5432/foiacquire
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
      - POSTGRES_USER=foiacquire
      - POSTGRES_PASSWORD=secret
      - POSTGRES_DB=foiacquire
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U foiacquire"]
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
  foiacquire:
    image: ghcr.io/monokrome/foiacquire:latest
    volumes:
      - ./data:/opt/foiacquire
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - BROWSER_URL=ws://chromium:9222
      - RUST_LOG=info
    depends_on:
      - chromium
    command: scrape cia_foia --limit 100

  chromium:
    image: ghcr.io/monokrome/foiacquire-chromium:latest
    shm_size: 2g
    # No ports exposed - internal only
```

### Full Stack with Web UI

```yaml
version: '3.8'

services:
  scraper:
    image: ghcr.io/monokrome/foiacquire:latest
    volumes:
      - ./data:/opt/foiacquire
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - DATABASE_URL=postgres://foiacquire:secret@postgres:5432/foiacquire
      - BROWSER_URL=ws://chromium:9222
      - RUST_LOG=info
    depends_on:
      postgres:
        condition: service_healthy
      chromium:
        condition: service_started
    command: scrape --all --daemon --interval 3600

  web:
    image: ghcr.io/monokrome/foiacquire:latest
    volumes:
      - ./data:/opt/foiacquire:ro
    environment:
      - DATABASE_URL=postgres://foiacquire:secret@postgres:5432/foiacquire
    ports:
      - "3030:3030"
    depends_on:
      postgres:
        condition: service_healthy
    command: serve 0.0.0.0:3030

  analyzer:
    image: ghcr.io/monokrome/foiacquire:latest-tesseract
    volumes:
      - ./data:/opt/foiacquire
    environment:
      - USER_ID=1000
      - GROUP_ID=1000
      - DATABASE_URL=postgres://foiacquire:secret@postgres:5432/foiacquire
      - RUST_LOG=info
    depends_on:
      postgres:
        condition: service_healthy
    command: analyze --daemon --interval 1800 --workers 2

  chromium:
    image: ghcr.io/monokrome/foiacquire-chromium:latest
    shm_size: 2g

  postgres:
    image: postgres:16-alpine
    volumes:
      - postgres_data:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=foiacquire
      - POSTGRES_PASSWORD=secret
      - POSTGRES_DB=foiacquire
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U foiacquire"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

## Synology NAS

### Container Manager Setup

1. **Download the image:**
   - Registry → Search for `ghcr.io/monokrome/foiacquire`
   - Download the `latest` tag

2. **Create the container:**
   - Image → Select foiacquire → Launch
   - **General Settings:**
     - Container Name: `foiacquire-scraper`
     - Enable auto-restart: Yes

3. **Volume Settings:**
   - Add folder: `/volume1/docker/foiacquire` → `/opt/foiacquire`
   - Ensure the folder has correct permissions

4. **Environment Variables:**
   ```
   USER_ID=1024          # Match your Synology user
   GROUP_ID=100          # Usually 'users' group
   TARGET_PATH=/opt/foiacquire
   RUST_LOG=info
   ```

5. **Execution Command:**
   ```
   scrape fbi_vault --daemon --interval 3600
   ```

### With PostgreSQL on Synology

If using the Synology PostgreSQL package or a container:

```
DATABASE_URL=postgres://foiacquire:password@localhost:5432/foiacquire
```

### Viewing Logs

In Container Manager:
- Container → foiacquire-scraper → Log

Or via SSH:
```bash
docker logs foiacquire-scraper -f
```

### Troubleshooting Permissions

If you see permission errors:

1. Check the host directory ownership:
   ```bash
   ls -la /volume1/docker/foiacquire
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

### With Additional OCR Backends

```dockerfile
FROM ghcr.io/monokrome/foiacquire:latest

# Add Tesseract with additional languages
RUN apk add --no-cache \
    tesseract-ocr \
    tesseract-ocr-data-eng \
    tesseract-ocr-data-deu \
    tesseract-ocr-data-fra
```

### Development Image

```dockerfile
FROM rust:latest as builder
WORKDIR /app
COPY . .
RUN cargo build --release --features postgres,browser

FROM alpine:latest
RUN apk add --no-cache ca-certificates su-exec
COPY --from=builder /app/target/release/foiacquire /usr/local/bin/
COPY bin/foiacquire-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
```

## Health Checks

Add health checks to your compose file:

```yaml
services:
  foiacquire:
    # ...
    healthcheck:
      test: ["CMD", "foiacquire", "status"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

## Resource Limits

```yaml
services:
  foiacquire:
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
  foiacquire:
    networks:
      - foiacquire-net

  postgres:
    networks:
      - foiacquire-net

networks:
  foiacquire-net:
    driver: bridge
```
