#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/opt/foia}"
USER="${USER_ID:-1000}"
GROUP="${GROUP_ID:-$USER}"
MIGRATE="${MIGRATE:-false}"

# Default Redis URL for container deployments (expects linked 'redis' service)
export REDIS_URL="${REDIS_URL:-redis://redis:6379}"

# Allow BROWSER_LINK_NAME & BROWSER_PORT since chromium fails with HOST header
if [[ -z $BROWSER_URL ]] && [[ ! -z $BROWSER_LINK_NAME ]]; then
    BROWSER_PORT="${BROWSER_PORT:-9222}"
    BROWSER_HOST=$(nslookup "${BROWSER_LINK_NAME}" | grep Address | cut -f 2 -d \  | tail -n 1)
    BROWSER_URL="ws://${BROWSER_HOST}:${BROWSER_PORT}"
fi

# Handle Tor: start if available, otherwise enable direct mode
if command -v tor >/dev/null 2>&1; then
    # Tor is installed - start it unless direct mode is explicitly set
    if [ "$FOIA_DIRECT" != "1" ] && [ "$FOIA_DIRECT" != "true" ]; then
        echo "Starting Tor daemon..."
        mkdir -p /tmp/tor
        tor --RunAsDaemon 1 --SocksPort 9050 --DataDirectory /tmp/tor
        sleep 2
        echo "Tor daemon started"
    fi
else
    # Tor not installed (clearnet container) - enable direct mode
    export FOIA_DIRECT=1
fi

# Run migrations if MIGRATE=true
if [ "$MIGRATE" = "true" ] || [ "$MIGRATE" = "1" ] || [ "$MIGRATE" = "yes" ]; then
    echo "Running database migrations..."
    su-exec "$USER:$GROUP" foia --data "$DATA_DIR" db migrate
fi

exec su-exec "$USER:$GROUP" foia --data "$DATA_DIR" "$@"
