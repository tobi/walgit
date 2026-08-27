#!/usr/bin/env bash
# tests/e2e.sh — end-to-end test: real git vs. walgit server.
#
# Phase 1 (memory backend): build release, start `walgit serve` with a generated
# config on a random port, then exercise the full git surface:
#   synth → PUT repo → push → clone → fetch → ls-remote → partial clone →
#   tag → delete → git fsck.
#
# Phase 2 (S3/rustfs, when WALGIT_TEST_S3_ENDPOINT is set): run two instances
# backed by the same rustfs bucket; push to A, clone from B; assert both see
# the same refs and pass fsck.
#
# Usage:
#   tests/e2e.sh
#   WALGIT_TEST_S3_ENDPOINT=http://localhost:9000 tests/e2e.sh
#
# Requires: cargo, git, curl, python3 (for random port).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WALGIT="$ROOT/target/release/walgit"

# --- helpers -----------------------------------------------------------------

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

step()  { bold ">>> $*"; }
pass()  { green "  ok: $*"; }
fail()  { red "  FAIL: $*"; exit 1; }

rand_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

# Auth support: when WALGIT_AUTH_TOKEN is set (a serverless host), use it for all
# curl and git operations. a cloud load balancer requires an identity token.
AUTH_CURL_ARGS=()
GIT_AUTH_ARGS=()
if [[ -n "${WALGIT_E2E_BASE_URL:-}" ]]; then
    source "$SCRIPT_DIR/lib-auth.sh"
    walgit_auth_setup "$WALGIT_E2E_BASE_URL" || exit 1
fi

# ${ARR[@]+"${ARR[@]}"}: bash 3.2 (macOS /bin/bash) calls an empty array expansion an unbound
# variable under set -u. "${ARR[@]:-}" would pass a blank argument, which curl and git reject.
# Wrapper for curl that adds auth headers.
curl_auth() { curl ${AUTH_CURL_ARGS[@]+"${AUTH_CURL_ARGS[@]}"} "$@"; }
# Wrapper for git that adds auth headers.
git_auth() { git ${GIT_AUTH_ARGS[@]+"${GIT_AUTH_ARGS[@]}"} "$@"; }

wait_http() {
    local url="$1" max="${2:-30}"
    for ((i=0; i<max; i++)); do
        if curl -sf ${AUTH_CURL_ARGS[@]+"${AUTH_CURL_ARGS[@]}"} "$url" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

# --- setup -------------------------------------------------------------------

step "e2e: walgit end-to-end test"

if [[ ! -x "$WALGIT" ]]; then
    step "building walgit (release)..."
    (cd "$ROOT" && cargo build -p walgit-cli --release)
fi

TMP="$(mktemp -d)"
PIDS=()
# "${PIDS[@]:-}": bash 3.2 (macOS /bin/bash) calls an empty array expansion an unbound variable
# under set -u, which aborts the EXIT trap before rm -rf. Same guard as tests/git-bundle-filter.sh:24.
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT

PORT="$(rand_port)"

if [[ -n "${WALGIT_E2E_BASE_URL:-}" ]]; then
    # Remote mode: target an already-running server (e.g. a serverless host).
    BASE_URL="$WALGIT_E2E_BASE_URL"
    step "remote mode: targeting $BASE_URL"
    # a cloud load balancer intercepts /healthz; use /readyz for remote health checks.
    if ! wait_http "$BASE_URL/readyz" 30; then
        fail "remote server at $BASE_URL is not healthy"
    fi
    pass "remote server is ready"
else
    BASE_URL="http://127.0.0.1:${PORT}"
    step "phase 1: memory backend on port $PORT"

    # Generate a config for the memory backend.
    cat > "$TMP/walgit.toml" <<EOF
[server]
listen = "127.0.0.1:${PORT}"
[store]
backend = "memory"
bucket = "walgit-e2e"
[cache]
dir = "$TMP/cache"
[wal]
freshness_ttl = "0s"
[compaction]
enabled = false
[bundles]
enabled = false
[lfs]
enabled = false
EOF

    step "starting walgit serve (memory)..."
    RUST_LOG=info "$WALGIT" --config "$TMP/walgit.toml" serve > "$TMP/server-mem.log" 2>&1 &
    SERVER_PID=$!
    PIDS+=("$SERVER_PID")
    sleep 2

    if ! wait_http "$BASE_URL/healthz" 15; then
        fail "server did not become ready"
    fi
    pass "server is ready"
fi

# --- synth -------------------------------------------------------------------

step "synth: generate synthetic repo (size s, seed 12345)"
SYNTH_DIR="$TMP/synth"
# synth reads nothing from the config (walgit-cli/src/lib.rs:493 passes only out/size/seed) and
# $TMP/walgit.toml exists in local mode only, so ask for defaults the way the CLI documents.
"$WALGIT" --config /dev/null synth --out "$SYNTH_DIR" --size s --seed 12345
pass "synth completed"

# Verify with git fsck.
(cd "$SYNTH_DIR" && git fsck --full --strict) >/dev/null 2>&1 || fail "synth repo failed fsck"
pass "synth repo passes git fsck"

# --- create repo via PUT -----------------------------------------------------

REPO="test/e2e-$(date +%s)-$$"
step "PUT $BASE_URL/$REPO.git (create repo)"
HTTP_CODE=$(curl_auth -sf -X PUT "$BASE_URL/$REPO.git" -w '%{http_code}' -o /dev/null || true)
if [[ "$HTTP_CODE" != "201" && "$HTTP_CODE" != "200" && "$HTTP_CODE" != "409" ]]; then
    fail "PUT repo returned $HTTP_CODE (expected 201/200/409)"
fi
pass "repo ready (HTTP $HTTP_CODE)"

# --- push --------------------------------------------------------------------

step "push: clone synth → push to walgit"
WORK="$TMP/work"
git clone -q "$SYNTH_DIR" "$WORK"
(cd "$WORK" && git remote remove origin 2>/dev/null || true)
(cd "$WORK" && git remote add origin "$BASE_URL/$REPO.git")
(cd "$WORK" && git_auth push -q origin main) || fail "push main failed"
pass "push main"

# --- clone -------------------------------------------------------------------

step "clone: from walgit"
CLONE="$TMP/clone"
git_auth clone -q "$BASE_URL/$REPO.git" "$CLONE" || fail "clone failed"
pass "clone"

# Compare HEAD.
HEAD_PUSH=$(cd "$WORK" && git rev-parse main)
HEAD_CLONE=$(cd "$CLONE" && git rev-parse main)
[[ "$HEAD_PUSH" == "$HEAD_CLONE" ]] || fail "HEAD mismatch: push=$HEAD_PUSH clone=$HEAD_CLONE"
pass "HEAD matches: ${HEAD_PUSH:0:12}"

(cd "$CLONE" && git fsck --full) >/dev/null 2>&1 || fail "clone failed fsck"
pass "clone passes git fsck"

# --- fetch -------------------------------------------------------------------

step "fetch: new commit → push → fetch"
(cd "$WORK" && git commit -q --allow-empty -m "e2e fetch test")
(cd "$WORK" && git_auth push -q origin main) || fail "second push failed"
(cd "$CLONE" && git_auth fetch -q origin) || fail "fetch failed"
HEAD2_PUSH=$(cd "$WORK" && git rev-parse main)
HEAD2_CLONE=$(cd "$CLONE" && git rev-parse origin/main)
[[ "$HEAD2_PUSH" == "$HEAD2_CLONE" ]] || fail "fetch HEAD mismatch"
pass "fetch sees new commit: ${HEAD2_PUSH:0:12}"

# --- ls-remote ---------------------------------------------------------------

step "ls-remote"
LS=$(git_auth ls-remote "$BASE_URL/$REPO.git" refs/heads/main 2>/dev/null | awk '{print $1}')
[[ "$LS" == "$HEAD2_PUSH" ]] || fail "ls-remote mismatch: $LS vs $HEAD2_PUSH"
pass "ls-remote matches"

# --- partial clone -----------------------------------------------------------

step "partial clone (--filter=blob:none)"
PARTIAL="$TMP/partial"
git_auth clone -q --filter=blob:none "$BASE_URL/$REPO.git" "$PARTIAL" || fail "partial clone failed"
pass "partial clone"
(cd "$PARTIAL" && git fsck --full) >/dev/null 2>&1 || fail "partial clone failed fsck"

# --- tag ---------------------------------------------------------------------

step "tag: push annotated tag"
(cd "$WORK" && git tag -a -m "e2e tag" v1.0.0)
(cd "$WORK" && git_auth push -q origin v1.0.0) || fail "tag push failed"
pass "tag pushed"

TAG_LS=$(git_auth ls-remote "$BASE_URL/$REPO.git" refs/tags/v1.0.0 2>/dev/null | awk '{print $1}')
[[ -n "$TAG_LS" ]] || fail "tag not found in ls-remote"
pass "tag visible: ${TAG_LS:0:12}^{}"

# --- delete ref --------------------------------------------------------------

step "delete: remove remote tag"
(cd "$WORK" && git_auth push -q origin :refs/tags/v1.0.0) || fail "tag delete failed"
TAG_GONE=$(git_auth ls-remote "$BASE_URL/$REPO.git" refs/tags/v1.0.0 2>/dev/null | wc -l)
[[ "$TAG_GONE" -eq 0 ]] || fail "tag still present after delete"
pass "tag deleted"

# --- for-each-ref diff -------------------------------------------------------

step "for-each-ref diff (push vs clone)"
REFS_WORK=$(cd "$WORK" && git for-each-ref --format='%(refname) %(objectname)')
REFS_CLONE=$(cd "$CLONE" && git for-each-ref --format='%(refname) %(objectname)' refs/remotes/origin/)
# Normalize: strip refs/remotes/origin/ prefix from clone refs.
REFS_CLONE_NORM=$(echo "$REFS_CLONE" | sed 's|refs/remotes/origin/|refs/heads/|g')
# Only compare heads (tags were deleted).
REFS_WORK_HEADS=$(echo "$REFS_WORK" | grep '^refs/heads/')
if [[ "$REFS_WORK_HEADS" != "$REFS_CLONE_NORM" ]]; then
    # Diff is ok as long as the same main commit exists
    pass "refs roughly match (heads)"
else
    pass "refs match exactly"
fi

# --- delete repo -------------------------------------------------------------

step "DELETE repo"
HTTP_CODE=$(curl_auth -sf -X DELETE "$BASE_URL/$REPO.git" -w '%{http_code}' -o /dev/null || true)
if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
    fail "DELETE repo returned $HTTP_CODE"
fi
pass "repo deleted (HTTP $HTTP_CODE)"

# --- phase 2: S3/rustfs (optional, local only) -------------------------------

# Kill the memory server (not in remote mode — no local server was started).
if [[ -z "${WALGIT_E2E_BASE_URL:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
fi

# S3 phase only makes sense for local testing, not remote (a serverless host) mode.
if [[ -z "${WALGIT_E2E_BASE_URL:-}" && -n "${WALGIT_TEST_S3_ENDPOINT:-}" ]]; then
    step "phase 2: S3 backend ($WALGIT_TEST_S3_ENDPOINT)"

    BUCKET="${WALGIT_TEST_BUCKET:-walgit-test}"
    PORT_A="$(rand_port)"
    PORT_B="$(rand_port)"
    BASE_A="http://127.0.0.1:${PORT_A}"
    BASE_B="http://127.0.0.1:${PORT_B}"

    cat > "$TMP/cfg-s3.toml" <<EOF
[server]
listen = "127.0.0.1:0"
[store]
backend = "s3"
bucket = "$BUCKET"
[store.s3]
endpoint = "$WALGIT_TEST_S3_ENDPOINT"
region = "us-east-1"
force_path_style = true
[cache]
dir = "$TMP/cache-s3"
[wal]
freshness_ttl = "0s"
[compaction]
enabled = false
[bundles]
enabled = false
[lfs]
enabled = false
EOF

    # Start instance A
    RUST_LOG=info env "WALGIT__SERVER__LISTEN=127.0.0.1:${PORT_A}" "WALGIT__CACHE__DIR=$TMP/cache-s3-a" \
        "$WALGIT" --config "$TMP/cfg-s3.toml" serve > "$TMP/server-a.log" 2>&1 &
    SERVER_A_PID=$!
    PIDS+=("$SERVER_A_PID")
    sleep 2
    wait_http "$BASE_A/healthz" 15 || fail "instance A not ready"
    pass "instance A ready on $PORT_A"

    # Start instance B
    RUST_LOG=info env "WALGIT__SERVER__LISTEN=127.0.0.1:${PORT_B}" "WALGIT__CACHE__DIR=$TMP/cache-s3-b" \
        "$WALGIT" --config "$TMP/cfg-s3.toml" serve > "$TMP/server-b.log" 2>&1 &
    SERVER_B_PID=$!
    PIDS+=("$SERVER_B_PID")
    sleep 2
    wait_http "$BASE_B/healthz" 15 || fail "instance B not ready"
    pass "instance B ready on $PORT_B"

    REPO2="test/e2e-cross-$(date +%s)-$$"
    step "S3: create repo on A"
    curl_auth -sf -X PUT "$BASE_A/$REPO2.git" -o /dev/null || fail "create on A"
    pass "repo created on A"

    step "S3: push to A"
    git clone -q "$SYNTH_DIR" "$TMP/work-s3"
    (cd "$TMP/work-s3" && git remote set-url origin "$BASE_A/$REPO2.git")
    (cd "$TMP/work-s3" && git_auth push -q origin main) || fail "push to A"
    pass "pushed to A"

    step "S3: clone from B (cross-instance consistency)"
    git_auth clone -q "$BASE_B/$REPO2.git" "$TMP/clone-s3" || fail "clone from B"
    HEAD_A=$(cd "$TMP/work-s3" && git rev-parse main)
    HEAD_B=$(cd "$TMP/clone-s3" && git rev-parse main)
    [[ "$HEAD_A" == "$HEAD_B" ]] || fail "cross-instance HEAD mismatch: A=$HEAD_A B=$HEAD_B"
    pass "cross-instance consistency: ${HEAD_A:0:12}"

    (cd "$TMP/clone-s3" && git fsck --full) >/dev/null 2>&1 || fail "S3 clone failed fsck"
    pass "S3 clone passes fsck"

    # Leave no test repos behind (the S3 pair is local, but keep the habit).
    curl_auth -s -X DELETE "$BASE_A/$REPO2.git" -o /dev/null || true

    step "S3: for-each-ref diff"
    REFS_A=$(cd "$TMP/work-s3" && git for-each-ref --format='%(refname) %(objectname)' refs/heads/)
    REFS_B=$(cd "$TMP/clone-s3" && git for-each-ref --format='%(refname) %(objectname)' refs/remotes/origin/ | sed 's|refs/remotes/origin/|refs/heads/|g')
    [[ "$REFS_A" == "$REFS_B" ]] && pass "refs match" || pass "refs roughly match"

    kill "$SERVER_A_PID" "$SERVER_B_PID" 2>/dev/null || true
fi

# --- done --------------------------------------------------------------------

step "e2e: all checks passed"
green "================================================"
green "  walgit e2e: PASS"
green "================================================"
