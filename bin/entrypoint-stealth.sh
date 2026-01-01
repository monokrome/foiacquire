#!/bin/sh
# Launch Chromium with anti-detection flags for stealth scraping
# Chromium ignores --remote-debugging-address on Alpine/Debian, so we use socat
# to forward external connections to its localhost-bound port.

# VNC_PASSWORD enables display on port 5900
# VNC_VIEWONLY=true makes it read-only (default: interactive)
if [ -n "$VNC_PASSWORD" ]; then
    Xvfb :99 -screen 0 1920x1080x24 &
    sleep 1
    export DISPLAY=:99
    VIEWONLY_FLAG=""
    [ "$VNC_VIEWONLY" = "true" ] && VIEWONLY_FLAG="-viewonly"
    x11vnc -display :99 -forever -shared $VIEWONLY_FLAG -passwd "$VNC_PASSWORD" &
    HEADLESS_FLAG=""
else
    HEADLESS_FLAG="--headless"
fi

# Start Chromium with anti-detection flags
chromium-browser \
    $HEADLESS_FLAG \
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

# Wait for Chromium to start
sleep 2

# Forward 0.0.0.0:9222 -> 127.0.0.1:9223
exec socat TCP-LISTEN:9222,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:9223
