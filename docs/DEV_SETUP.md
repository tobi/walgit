# Developer setup

Start with `GOAL.md` and `AGENTS.md`. Linux is the CI reference platform; macOS
has local recipe support. Windows is not currently qualified (issue #16).

## Tools and reproducibility

`nix develop` uses the checked-in `flake.lock` and `rust-toolchain.toml` for the
devshell. Keep lockfiles unchanged when reproducing CI. Network downloads and
container images tagged `latest` mean the entire local workflow is not hermetic.

| Tool | Repository requirement |
|---|---|
| Rust | `rust-toolchain.toml`, currently 1.97.1, with rustfmt and Clippy |
| Git / Git LFS | Server Git ≥2.47; install git-lfs for LFS integration tests |
| Node / pnpm | Node 24 and pnpm 10, matching CI; use `web/pnpm-lock.yaml` |
| Protobuf | `protoc` plus well-known imports (Debian: protobuf-compiler and libprotobuf-dev) |
| Native compiler | C/C++ compiler, pkg-config, CMake, Perl and platform development libraries |
| Shell tools | Bash, just, curl, ripgrep, GNU sort/comm/timeout; lsof or ss for port checks |
| TLC | Java 11+, checksum-pinned jar fetched by scripts/ensure-tla-tools.sh |
| Local S3 rig | Podman and its compose provider; macOS also needs a running podman machine |

Without Nix, install the same tools through your platform package manager and
rustup. On macOS, GNU coreutils may need its `gnubin` directory on PATH for the
TLC runner; `gtimeout` alone only covers recipes that explicitly select it.
Homebrew protobuf supplies the imports; `podman machine init` is a one-time step,
and `podman machine start` starts its VM. The recipes give a useful error if that
VM is not running. No test should use or alter your personal Git configuration.

## Build and run

```sh
nix develop
just web-build
cargo build --locked --release -p walgit-cli
just dev-local
```

The web build produces the SPA and SDK embedded by the server. Open
`https://walgit.localhost:8080/`; standalone uses a self-signed development CA and
loopback-only unauthenticated access. Follow the served setup instructions for
trusting that CA. `PORT` changes the application listener, not the store listener.

`just dev-store` starts the local RustFS service and ensures the test bucket
exists. It accepts a healthy existing store on repeat startup. A listener on the
selected ports that does not answer the health check produces an actionable error
before the container bootstrap. Compose owns the final bind and detects races.

To avoid a port collision, change both the host binding and application endpoint
through these shared variables:

```sh
export WALGIT_DEV_STORE_PORT=19100
export WALGIT_DEV_CONSOLE_PORT=19101
just dev-local
```

Compose binds those ports on loopback. `dev-local` derives its S3 endpoint from
the store port; changing only `WALGIT__STORE__S3__ENDPOINT` does not remap containers.
An explicit endpoint override instead uses a store you have already started.
The checked-in standalone TOML still defaults to port 9000 when invoked directly.
Stop the compose services with `just dev-store-stop`; it does not delete volumes.

## Validate

```sh
just ci          # warnings, Clippy, fast tests, e2e, serial simulations, standalone smoke
just spec        # bounded models, exact negative controls, fast reference
just spec-full   # larger reference arms too; explicit JVM budgets in docs/spec/README.md
just test-slow   # opt-in ignored stress/benchmark tests
just test-s3     # local store contract; start the local S3 rig first
```

For a nondefault local port, set `WALGIT_TEST_S3_ENDPOINT` to that endpoint when
running `just test-s3`. Credentials come from the test environment; the local
compose fixture uses synthetic development credentials. Full cloud/edge/client
qualification is separate from these local checks. Use `--locked` for ad-hoc Cargo
builds and the documented bounded test tiers rather than an unbounded workspace run.
