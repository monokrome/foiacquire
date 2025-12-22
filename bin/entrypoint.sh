#!/bin/bash
# Launch Chromium with remote debugging and socat proxy
# Chromium on Debian ignores --remote-debugging-address, so we use socat
# to forward external connections to its localhost-bound port.

# Start Chromium on port 9223 (internal)
chromium \
    --headless=new \
    --no-sandbox \
    --disable-gpu \
    --disable-software-rasterizer \
    --disable-dev-shm-usage \
    --disable-blink-features=AutomationControlled \
    --disable-infobars \
    --disable-background-networking \
    --disable-sync \
    --disable-translate \
    --metrics-recording-only \
    --no-first-run \
    --no-default-browser-check \
    --user-agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
    --remote-debugging-port=9223 &

# Wait for Chromium to start
sleep 2

# Forward 0.0.0.0:9222 -> 127.0.0.1:9223
exec socat TCP-LISTEN:9222,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:9223
