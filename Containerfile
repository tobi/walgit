# walgit container image — one binary in front of an object store.
#
#   podman build -t walgit -f Containerfile .
#   podman run --rm -p 8080:8080 \
#       -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
#       -v ./walgit.toml:/etc/walgit/walgit.toml:ro \
#       -v walgit-cache:/var/lib/walgit \
#       walgit
#
# The image carries git (upload-pack, repack, bundle, index-pack run as subprocesses),
# git-lfs, CA certificates and tini. Config comes from /etc/walgit/walgit.toml or
# WALGIT__SECTION__KEY environment overrides; the local cache (materialized repositories,
# a self-signed TLS cert) lives under /var/lib/walgit and can be wiped at any time — the
# bucket is the only durable state. `nix build .#image` produces the same thing from flake.nix.

# ---- 1. web UI (embedded into the binary at compile time) ---------------------------
FROM docker.io/library/node:24-bookworm-slim AS web
RUN corepack enable && corepack prepare pnpm@10 --activate
WORKDIR /src/web
COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY web/ ./
RUN pnpm run build && test -f dist/index.html && test -f dist/repos.js

# ---- 2. rust build ------------------------------------------------------------------------
FROM docker.io/library/rust:1.97-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev pkg-config cmake perl python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY --from=web /src/web/dist ./web/dist
ARG WALGIT_BUILD_SHA=dev
ENV WALGIT_BUILD_SHA=${WALGIT_BUILD_SHA}
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p walgit-cli \
    && install -D target/release/walgit /out/bin/walgit \
    && install -D target/release/walgit-server /out/bin/walgit-server

# ---- 3. runtime -----------------------------------------------------------------------------
# trixie ships git 2.47+: walgit wants >= 2.47 on the server (pack.writeReverseIndex, bundle-uri,
# `index-pack --rev-index`); clients need >= 2.46.
FROM docker.io/library/debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends git git-lfs ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/* \
    && git --version
RUN useradd --uid 1000 --create-home --shell /bin/sh walgit \
    && mkdir -p /etc/walgit /var/lib/walgit && chown walgit:walgit /var/lib/walgit
COPY --from=build /out/bin/walgit /out/bin/walgit-server /usr/local/bin/
COPY walgit.example.toml /etc/walgit/walgit.toml
ENV RUST_LOG=info,walgit=debug \
    WALGIT_CONFIG=/etc/walgit/walgit.toml \
    WALGIT__CACHE__DIR=/var/lib/walgit \
    WALGIT__SERVER__LISTEN=0.0.0.0:8080
USER walgit
WORKDIR /home/walgit
EXPOSE 8080
VOLUME ["/var/lib/walgit"]
HEALTHCHECK --interval=30s --timeout=5s CMD curl -fsS http://127.0.0.1:8080/readyz || exit 1
ENTRYPOINT ["tini", "--", "walgit"]
CMD ["serve"]
