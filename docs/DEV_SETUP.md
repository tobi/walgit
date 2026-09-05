# Developer setup — per-platform tool installation

walgit needs: Rust (pinned by `rust-toolchain.toml`), `protoc`, Node.js 24, `pnpm`, `just`, and `git` (>= 2.46).

## Nix devshell (recommended)

`flake.nix` provides a shell with every tool pinned to a known-good version. This is the canonical "it works" path.

```sh
nix develop
```

After entering the shell, all tools are available:

```sh
which rustc protoc node pnpm just git podman
```

## macOS (Apple Silicon)

Install the required tools via Homebrew:

```sh
brew install rustup-init protobuf node@24 pnpm just git git-lfs podman podman-compose
rustup-init -y --default-toolchain stable
source ~/.cargo/env
rustup toolchain install $(cat rust-toolchain.toml | grep channel | head -1 | sed 's/.*"\(.*\)".*/\1/')
```

Enable pnpm via corepack:

```sh
corepack enable pnpm
```

If `just dev-store` fails because port 9000 is in use (common with Zscaler or other corporate tools), free it or set a different port:

```sh
# Check what is using port 9000
lsof -i :9000

# Set a different port for the local S3 store
export WALGIT__STORE__S3__ENDPOINT=http://127.0.0.1:9001
```

## Linux (Ubuntu/Debian)

```sh
sudo apt-get update
sudo apt-get install -y curl build-essential pkg-config libssl-dev protobuf-compiler
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
rustup toolchain install $(cat rust-toolchain.toml | grep channel | head -1 | sed 's/.*"\(.*\)".*/\1/')

# Node.js 24
curl -fsSL https://deb.nodesource.com/setup_24.x | sudo -E bash -
sudo apt-get install -y nodejs
corepack enable pnpm

# just
cargo install just

# podman (rootless)
sudo apt-get install -y podman podman-compose
```

## Linux (Arch)

```sh
sudo pacman -S rustup protobuf nodejs pnpm just git git-lfs podman podman-compose
rustup default stable
rustup toolchain install $(cat rust-toolchain.toml | grep channel | head -1 | sed 's/.*"\(.*\)".*/\1/')
```

## Verify your setup

After installing the tools, verify they are available:

```sh
rustc --version
protoc --version
node --version
pnpm --version
just --version
git --version
```

Then run the fast test tier:

```sh
just test
```