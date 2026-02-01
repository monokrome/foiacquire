#!/bin/sh
# Launch Chromium with anti-detection flags for stealth scraping
# Chromium ignores --remote-debugging-address on Alpine/Debian, so we use socat
# to forward external connections to its localhost-bound port.
#
# Tor is enabled by default. Set FOIACQUIRE_DIRECT=1 to disable.

set -e

DISPLAY_NUM=99
XVFB_PID=""
VNC_PID=""
CHROME_PID=""
SOCAT_PID=""
TOR_PID=""

cleanup() {
    echo "Shutting down..."
    kill $CHROME_PID $SOCAT_PID $VNC_PID $XVFB_PID $TOR_PID 2>/dev/null || true
    rm -f /tmp/.X${DISPLAY_NUM}-lock /tmp/.X11-unix/X${DISPLAY_NUM} 2>/dev/null || true
}

trap cleanup EXIT TERM INT

# Clean up stale X server lock files from previous runs
rm -f /tmp/.X${DISPLAY_NUM}-lock /tmp/.X11-unix/X${DISPLAY_NUM} 2>/dev/null || true

# Tor routing (default: enabled)
PROXY_FLAG=""
if [ "$FOIACQUIRE_DIRECT" != "1" ]; then
    echo "Starting Tor..."
    tor --SocksPort 9050 --DataDirectory /var/lib/tor --Log "notice file /var/log/tor/notices.log" &
    TOR_PID=$!

    # Wait for Tor to bootstrap
    for i in $(seq 1 30); do
        if [ -S /var/lib/tor/control ] || grep -q "Bootstrapped 100%" /var/log/tor/notices.log 2>/dev/null; then
            break
        fi
        sleep 1
    done

    if grep -q "Bootstrapped 100%" /var/log/tor/notices.log 2>/dev/null; then
        echo "Tor connected."
        PROXY_FLAG="--proxy-server=socks5://127.0.0.1:9050"
    else
        echo "WARNING: Tor did not fully bootstrap after 30s, continuing anyway..."
        PROXY_FLAG="--proxy-server=socks5://127.0.0.1:9050"
    fi
elif [ -n "$SOCKS_PROXY" ]; then
    PROXY_FLAG="--proxy-server=$SOCKS_PROXY"
fi

# VNC_PASSWORD enables display on port 5900
# VNC_VIEWONLY=true makes it read-only (default: interactive)
if [ -n "$VNC_PASSWORD" ]; then
    Xvfb :${DISPLAY_NUM} -screen 0 1920x1080x24 &
    XVFB_PID=$!
    sleep 1
    export DISPLAY=:${DISPLAY_NUM}
    VIEWONLY_FLAG=""
    [ "$VNC_VIEWONLY" = "true" ] && VIEWONLY_FLAG="-viewonly"
    x11vnc -display :${DISPLAY_NUM} -forever -shared $VIEWONLY_FLAG -passwd "$VNC_PASSWORD" &
    VNC_PID=$!
    HEADLESS_FLAG=""
else
    HEADLESS_FLAG="--headless"
fi

# Start Chromium with anti-detection flags
chromium-browser \
    $HEADLESS_FLAG \
    $PROXY_FLAG \
    --no-sandbox \
    --disable-gpu \
    --disable-dev-shm-usage \
    --disable-software-rasterizer \
    --disable-blink-features=AutomationControlled \
    --disable-infobars \
    --disable-background-networking \
    --disable-sync \
    --disable-translate \
    --no-first-run \
    --no-default-browser-check \
    --remote-debugging-port=9223 \
    "$@" &
CHROME_PID=$!

# Wait for Chromium to start
sleep 2

# Forward 0.0.0.0:9222 -> 127.0.0.1:9223
socat TCP-LISTEN:9222,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:9223 &
SOCAT_PID=$!

# Monitor processes - exit if Chrome or socat dies
while kill -0 $CHROME_PID 2>/dev/null && kill -0 $SOCAT_PID 2>/dev/null; do
    sleep 5
done

exit 1
