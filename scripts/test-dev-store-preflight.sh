#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/bin"
cat > "$scratch/bin/curl" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$CALLS"
exit "$HEALTH_STATUS"
MOCK
cat > "$scratch/bin/ss" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$CALLS"
if [[ "$BUSY" == true ]]; then echo 'LISTEN 0 128 127.0.0.1:19100'; fi
MOCK
chmod +x "$scratch/bin/"*
export PATH="$scratch/bin:$PATH" CALLS="$scratch/calls"
export WALGIT_DEV_STORE_PORT=19100 WALGIT_DEV_CONSOLE_PORT=19101
export HEALTH_STATUS=0 BUSY=true
bash "$root/scripts/dev-store-preflight.sh"
[[ "$(wc -l < "$CALLS")" -eq 1 ]] # Healthy repeat start never probes busy ports.
export HEALTH_STATUS=1 BUSY=false
bash "$root/scripts/dev-store-preflight.sh"
export BUSY=true
if bash "$root/scripts/dev-store-preflight.sh" > "$scratch/error" 2>&1; then exit 1; fi
[[ "$(< "$scratch/error")" == *'Port 19100 is occupied'* ]]
for invalid in 0 65536 019100 nope; do
    if WALGIT_DEV_STORE_PORT="$invalid" bash "$root/scripts/dev-store-preflight.sh" >/dev/null 2>&1; then exit 1; fi
done
if WALGIT_DEV_CONSOLE_PORT=19100 bash "$root/scripts/dev-store-preflight.sh" >/dev/null 2>&1; then exit 1; fi
[[ "$(< "$CALLS")" == *'http://127.0.0.1:19100/minio/health/live'* ]]
echo 'PASS healthy repeat start, free/occupied alternate ports and invalid settings'
