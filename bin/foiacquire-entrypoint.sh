#!/bin/sh
set -e

TARGET="${TARGET_PATH:-/opt/foiacquire}"
USER="${USER_ID:-1000}"
GROUP="${GROUP_ID:-$USER}"
MIGRATE="${MIGRATE:-false}"

# Run migrations if MIGRATE=true
if [ "$MIGRATE" = "true" ] || [ "$MIGRATE" = "1" ] || [ "$MIGRATE" = "yes" ]; then
    echo "Running database migrations..."
    su-exec "$USER:$GROUP" foiacquire --target "$TARGET" db migrate
fi

exec su-exec "$USER:$GROUP" foiacquire --target "$TARGET" "$@"
