#!/bin/sh
set -e
echo "[foiacquire] Starting: $@"
foiacquire --target "${TARGET_PATH:-/opt/foiacquire}" "$@"
