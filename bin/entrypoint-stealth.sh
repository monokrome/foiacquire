#!/bin/sh

# VNC=true enables display on port 5900 and disables headless mode
if [ "$VNC" = "true" ]; then
    Xvfb :99 -screen 0 1920x1080x24 &
    sleep 1
    export DISPLAY=:99
    x11vnc -display :99 -forever -shared -nopw &
    HEADLESS_FLAG=""
else
    HEADLESS_FLAG="--headless"
fi

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
sleep 2
exec socat TCP-LISTEN:9222,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:9223
