# walgit justfile — local dev and test targets.

# `timeout` is GNU coreutils: absent on macOS, where every wrapped recipe otherwise dies with
# `sh: timeout: command not found` (exit 127) before a single test runs — pushing contributors
# onto the broad `cargo test --workspace` that AGENTS.md forbids. Prefer timeout, then gtimeout
# (brew install coreutils), else run unwrapped: no watchdog is better than no tests.
t5 := `if command -v timeout >/dev/null 2>&1; then echo "timeout 300"; elif command -v gtimeout >/dev/null 2>&1; then echo "gtimeout 300"; else echo ""; fi`
t10 := `if command -v timeout >/dev/null 2>&1; then echo "timeout 600"; elif command -v gtimeout >/dev/null 2>&1; then echo "gtimeout 600"; else echo ""; fi`
t15 := `if command -v timeout >/dev/null 2>&1; then echo "timeout 900"; elif command -v gtimeout >/dev/null 2>&1; then echo "gtimeout 900"; else echo ""; fi`

# Default: show available targets.
default:
    @just --list

# Build the Vite SPA assets embedded by walgit-server.
web-build:
    cd web && pnpm install --frozen-lockfile && pnpm run build

# Local dev = standalone: the server with every role (serve, maintain, events) at
# https://walgit.localhost:$PORT (default 8080) against local rustfs. Self-contained: starts rustfs (+ bucket) if
# it is not answering on :9000 and builds the SPA if web/dist is missing, then runs the server.
# `config` defaults to walgit.standalone.toml; point it at a real bucket by editing [store] there. The rustfs
# keys come from the environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY; compose.yaml fixes them).
# Optional: export WALGIT__SERVER__AUTH__* (OIDC client, session secret) to try browser sign-in locally.
dev-local config="walgit.standalone.toml":
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-walgit-dev}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-walgit-dev-secret}"
    if ! curl -sf http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
        echo "rustfs not running on :9000 — starting it (just dev-store)"
        just dev-store
    fi
    if [ ! -f web/dist/index.html ]; then
        echo "web/dist missing — building the SPA (just web-build)"
        just web-build
    fi
    cargo build --release --bin walgit-server
    port="${PORT:-8080}"
    echo "→ https://walgit.localhost:${port}/  (PORT=${port}, config {{config}}, store rustfs :9000, cache /tmp/walgit)"
    exec ./target/release/walgit-server --config {{config}}

# Start rustfs (S3-compatible) for local dev via podman compose (rootless, no daemon group needed;
# `podman compose` drives compose.yaml through the docker-compose binary dev.yml installs).
# `podman compose` talks to the podman API socket; rootless nix podman has no systemd unit for it, so
# `podman system service` is started (detached, idle-timeout 0) when the socket is missing.
dev-store:
    #!/usr/bin/env bash
    set -euo pipefail
    # nix podman ships no /etc/containers: give the user a signature policy + registry search list once.
    cdir="${XDG_CONFIG_HOME:-$HOME/.config}/containers"; mkdir -p "$cdir"
    [ -f "$cdir/policy.json" ] || printf '{"default":[{"type":"insecureAcceptAnything"}]}\n' > "$cdir/policy.json"
    [ -f "$cdir/registries.conf" ] || printf 'unqualified-search-registries = ["docker.io"]\n' > "$cdir/registries.conf"
    sock="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"
    if [ ! -S "$sock" ]; then
        echo "starting rootless podman API socket at $sock"
        mkdir -p "$(dirname "$sock")"
        setsid nohup podman system service --time=0 "unix://$sock" >/tmp/walgit-podman-service.log 2>&1 < /dev/null &
        for _ in $(seq 1 50); do [ -S "$sock" ] && break; sleep 0.2; done
        [ -S "$sock" ] || { echo "podman API socket did not appear; see /tmp/walgit-podman-service.log"; exit 1; }
    fi
    podman compose up -d rustfs
    echo "Waiting for rustfs to be healthy..."
    podman compose run --rm create-bucket
    echo "rustfs is running on http://127.0.0.1:9000 (console :9001)"
    echo "Credentials: walgit-dev / walgit-dev-secret"
    echo "Bucket: walgit-test"

# Stop rustfs.
dev-store-stop:
    podman compose down

# --- tests -------------------------------------------------------------------
# Tiers (all hermetic: in-memory store, tempdir caches, real `git` binary):
#   test       fast tier, < 30 s: every unit/integration test not marked #[ignore]
#   test-slow  benches/soak: #[ignore]d tests (20k-ref push, 466k-ref render, ...)
#   test-s3    store contract against local rustfs (just dev-store)
#   test-gcs   store contract against a real bucket (writes under a unique prefix)

# Fast hermetic tier (< 30 s): every test not marked #[ignore].
# Fast tier (default, < 1 min): unit tests + the quick integration suites.
# Never run `cargo test --workspace --no-fail-fast` interactively: a single
# hung test blocks for the whole timeout. Use `just e2e` / `just ci` below.
test:
    {{t5}} cargo test --workspace --lib --bins
    {{t5}} cargo test -p walgit-store -p walgit-git -p walgit-wal -p walgit-bundle --tests
    {{t5}} cargo test -p walgit-server --test web_api --test web_ui --test api_v1 --test static_http --test maintain --test routing_prefix --test lfs_upstream --test drain

# Smart-HTTP end-to-end against real git (≈ 20 s) — run when touching smart.rs/receive/upload-pack/wal.
e2e *ARGS:
    {{t10}} cargo test -p walgit-server --test e2e {{ARGS}}

# Zero rustc warnings, workspace-wide, all targets (tests, benches, examples).
# Done by grepping the normal build instead of RUSTFLAGS=-D warnings, which would
# change every crate's fingerprint and force full rebuilds in every shell.
warnings:
    #!/usr/bin/env bash
    set -uo pipefail
    # A command substitution that fails does NOT abort under `set -uo pipefail` (no -e), so a
    # workspace that does not compile used to fall through to "no rustc warnings" and exit 0 —
    # the preflight passing on a broken tree. Check the build's status before grepping it.
    if ! out="$({{t15}} cargo build --workspace --all-targets 2>&1)"; then
        printf '%s\n' "$out"
        echo; echo "cargo build failed — fix the errors above"; exit 1
    fi
    if printf '%s\n' "$out" | grep -qE '^warning: (unused|function|variable|field|method|struct|enum|never|dead|irrefutable|unreachable|value assigned|deprecated|trait|type|constant|static|associated)'; then
        printf '%s\n' "$out" | grep -E '^warning' -A4 | grep -vE '^warning: `walgit-[a-z]+`'
        echo; echo "rustc warnings present — fix them (just warnings is part of just ci and the deploy preflight)"; exit 1
    fi
    echo "no rustc warnings"

# Clippy, workspace-wide, all targets, warnings are errors. The lint set lives in
# [workspace.lints] in Cargo.toml; test code is exempt from the panic-path restriction
# lints via clippy.toml (allow-unwrap-in-tests etc.).
clippy:
    {{t15}} cargo clippy --workspace --all-targets -- -D warnings

# Everything that must be green before a merge (what CI runs).
ci: warnings clippy test e2e

# Slow tier: #[ignore]d benches/soaks (20k-ref push, 466k-ref render, ...).
test-slow:
    cargo test --workspace -- --ignored --nocapture

# Store contract against a real GCS bucket (unique prefix per run, cleaned up).
test-gcs bucket:
    WALGIT_TEST_GCS_BUCKET={{bucket}} cargo test -p walgit-store --features gcs --test contract -- gcs_contract --nocapture

# Store contract against a real Azure storage account (`az login` first; RBAC per docs/superpowers/specs/2026-08-31-azure-blob-store-design.md).
test-azure account container="walgit-test":
    WALGIT_TEST_AZURE_ACCOUNT={{account}} WALGIT_TEST_AZURE_CONTAINER={{container}} cargo test -p walgit-store --features azure --test contract -- azure_contract --nocapture

# Run walgit-store contract tests against memory only.
store-test:
    cargo test -p walgit-store --test contract -- memory_contract

# Run walgit-store contract tests against rustfs (requires `just dev-store` first).
# Store contract against local rustfs (run `just dev-store` first).
test-s3: store-test-s3

store-test-s3:
    WALGIT_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
    WALGIT_TEST_BUCKET=walgit-test \
    AWS_ACCESS_KEY_ID=walgit-dev \
    AWS_SECRET_ACCESS_KEY=walgit-dev-secret \
    cargo test -p walgit-store --test contract -- --nocapture

# Run all walgit-store tests (memory + S3 if env set).
store-test-all:
    cargo test -p walgit-store
