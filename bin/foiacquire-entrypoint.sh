#!/bin/sh
set -e

# If running as root and USER_ID is set, adjust the foiacquire user's UID/GID
if [ "$(id -u)" = "0" ]; then
    # Adjust group ID if specified
    if [ -n "$GROUP_ID" ] && [ "$GROUP_ID" != "$(id -g foiacquire)" ]; then
        groupmod -g "$GROUP_ID" foiacquire
    fi

    # Adjust user ID if specified
    if [ -n "$USER_ID" ] && [ "$USER_ID" != "$(id -u foiacquire)" ]; then
        usermod -u "$USER_ID" foiacquire
    fi

    # Fix ownership of the data directory
    chown -R foiacquire:foiacquire /opt/foiacquire

    # Drop privileges and run as foiacquire
    exec su-exec foiacquire foiacquire --target "$TARGET_PATH" "$@"
else
    # Already running as non-root, just exec
    exec foiacquire --target "$TARGET_PATH" "$@"
fi
