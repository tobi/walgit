# Local development setup

One-time setup to build, run and test walgit on your machine. If anything here is
wrong or incomplete for your platform, that is a bug — the build should be hermetic
(tracked as an open issue).

## Prerequisites

| Tool | Version | Install (macOS) | Install (Linux) |
|---|---|---|---|
| Rust | per `rust-toolchain.toml` | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` (rustup reads the toolchain file) | same |
| git | ≥ 2.46 | `brew install git` | distro package |
| just | any recent | `brew install just` | `cargo install just` or distro package |
| protoc | any recent | `brew install protobuf` | `apt install protobuf-compiler` |
| Node | 24 | `brew install node@24` or nvm | nvm |
| pnpm | via corepack | `corepack enable && corepack prepare pnpm@latest --activate` | same |
| container runtime | podman **or** docker | `brew install podman && podman machine init && podman machine start` | docker or podman |

Notes:

- **PATH**: after installing rustup, open a new shell or `source ~/.cargo/env` so
  `cargo` is on `PATH`. The repo's `rust-toolchain.toml` pins the exact Rust version;
  rustup installs it automatically on the first `cargo` invocation.
- **corepack**: Node ≥ 16 ships corepack but it may need `corepack enable` once so the
  `pnpm` shim appears. The web build runs `pnpm` via `just web-build`.
- **macOS `timeout`**: `just` recipes prefer GNU `timeout` (or `gtimeout` from
  `brew install coreutils`) but run without it — slower, still correct.

## Build

```sh
just web-build                 # web/dist (React SPA + SDK), embedded into the binary
cargo build --release -p walgit-cli
```

The release binary is `target/release/walgit-server` (and `target/release/walgit`).

## Run (one box, everything local)

```sh
just dev-local
```

This is self-contained:

1. starts **rustfs** (S3-compatible store) in a container if nothing answers on
   `127.0.0.1:9000`, and creates the `walgit-test` bucket;
2. builds the SPA if `web/dist` is missing;
3. builds and runs `walgit-server --config walgit.standalone.toml`.

Then open **https://walgit.localhost:8080/** (self-signed TLS: accept the browser
warning once, or trust the CA the server publishes at `/services/public/ca.pem`).

Auth is `mode = "none"` on loopback: everyone is `anon` with write. A push to a new
name creates the repository:

```sh
git -c http.sslCAInfo=<(curl -sk https://walgit.localhost:8080/services/public/ca.pem) \
    push https://walgit.localhost:8080/acme/app.git main
```

or fetch the CA once and configure git:

```sh
curl -sk https://walgit.localhost:8080/services/public/ca.pem -o ~/.walgit-ca.pem
git config --global http."https://walgit.localhost:8080".sslCAInfo ~/.walgit-ca.pem
```

### macOS caveat: port 9000 already bound

Some corporate agents (e.g. Zscaler) bind `0.0.0.0:9000` on macOS, which makes the
documented rustfs port unreachable even though nothing shows in `lsof` for your user.
Check first:

```sh
nc -z 127.0.0.1 9000 && echo "9000 taken" || echo "9000 free"
```

If taken, run rustfs on another port — either a container with a different published
port or a native binary — and override the endpoint via env (no config edit needed):

```sh
export WALGIT__STORE__S3__ENDPOINT=http://127.0.0.1:19100
```

Any S3-compatible store works (`walgit.example.toml` lists the keys; MinIO, R2, Ceph…).

## Test

```sh
just test     # fast tier, < 1 min: unit + quick integration, in-memory store
just e2e      # real git against the server (~20 s)
just warnings # zero rustc warnings across all targets
just ci       # all of the above (what CI runs)
just test-s3  # store contract against local rustfs (needs the dev store running)
```

## Stop

```sh
just dev-store-stop   # stop the rustfs container
```
