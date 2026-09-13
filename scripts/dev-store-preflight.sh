#!/usr/bin/env bash
# A healthy existing store is a valid repeat start, not a port conflict.
set -euo pipefail
store_port="${WALGIT_DEV_STORE_PORT:-9000}"
console_port="${WALGIT_DEV_CONSOLE_PORT:-9001}"
for port in "$store_port" "$console_port"; do
    if [[ ! "$port" =~ ^[1-9][0-9]{0,4}$ ]] || ((10#$port > 65535)); then
        echo "Invalid local store port: $port (expected 1..65535)" >&2
        exit 1
    fi
done
if [[ "$store_port" == "$console_port" ]]; then
    echo 'Store and console ports must differ.' >&2
    exit 1
fi
if curl --max-time 2 -sf "http://127.0.0.1:$store_port/minio/health/live" >/dev/null 2>&1; then
    exit 0
fi
for port in "$store_port" "$console_port"; do
    busy=false
    if command -v ss >/dev/null 2>&1; then
        [[ -z "$(ss -tlnH "sport = :$port")" ]] || busy=true
    elif command -v lsof >/dev/null 2>&1; then
        if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then busy=true; fi
    else
        echo 'Install ss (iproute2) or lsof to check local store ports.' >&2
        exit 1
    fi
    if $busy; then
        echo "Port $port is occupied but the local store is not healthy." >&2
        echo 'Choose unused WALGIT_DEV_STORE_PORT and WALGIT_DEV_CONSOLE_PORT values; compose and dev-local use them together.' >&2
        exit 1
    fi
done
