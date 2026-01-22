#!/bin/sh
# Launch Chromium with remote debugging and socat proxy
# Chromium ignores --remote-debugging-address on Alpine/Debian, so we use socat
# to forward external connections to its localhost-bound port.

set -e

XVFB_SLEEP_TIMEOUT=${XVFB_SLEEP_TIMEOUT:-3}
BROWSER_SLEEP_TIMEOUT=${BROWSER_SLEEP_TIMEOUT:-5}
XVFB_DISPLAY=${XVFB_DISPLAY:-:99}
HEADLESS_FLAG="--headless"

# VNC_PASSWORD enables display on port 5900
# VNC_VIEWONLY=true makes it read-only (default: interactive)
if [ -n "$VNC_PASSWORD" ]; then
    HEADLESS_FLAG=""

    Xvfb ${XVFB_DISPLAY} -screen 0 1920x1080x24 &
    export DISPLAY=${XVFB_DISPLAY}

    # Give XVFB time to start up
    sleep ${XVFB_SLEEP_TIMEOUT}

    VIEWONLY_FLAG=""
    [ "$VNC_VIEWONLY" = "true" ] && VIEWONLY_FLAG="-viewonly"

    x11vnc -display ${XVFB_DISPLAY} -forever -shared $VIEWONLY_FLAG -passwd "$VNC_PASSWORD" &
fi

# Start Chromium on port 9223 (internal)
chromium-browser \
    $HEADLESS_FLAG \
    --no-sandbox \
    --disable-gpu \
    --disable-dev-shm-usage \
    --disable-software-rasterizer \
    --remote-debugging-port=9223 \
    "$@" &
CHROME_PID=$!

# Wait for Chromium to start
sleep ${BROWSER_SLEEP_TIMEOUT}

# Forward 0.0.0.0:9222 -> 127.0.0.1:9223
socat TCP-LISTEN:9222,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:9223 &
SOCAT_PID=$!

# Monitor both processes - exit if either dies
while kill -0 $CHROME_PID 2>/dev/null && kill -0 $SOCAT_PID 2>/dev/null; do
    sleep 5
done

echo "Process exited, shutting down..."
kill $CHROME_PID $SOCAT_PID 2>/dev/null || true
exit 1
