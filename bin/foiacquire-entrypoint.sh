#!/bin/sh
set -e

TARGET="${TARGET_PATH:-/opt/foiacquire}"
USER="${USER_ID:-1000}"
GROUP="${GROUP_ID:-$USER}"

exec gosu "$USER:$GROUP" foiacquire --target "$TARGET" "$@"
