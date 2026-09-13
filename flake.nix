{
  # walgit — a git server that is one binary in front of an object store.
  #
  #   nix run github:<you>/walgit -- --config walgit.toml      # run it
  #   nix build .#walgit                                       # result/bin/{walgit,walgit-server}
  #   nix build .#image && podman load < result                # an OCI image (same bytes as the Containerfile)
  #   nix develop                                              # rust, protobuf, pnpm, git, just, rustfs-compatible tools
  #
  # The web UI is built from web/ with pnpm and embedded into the binary at compile time
  # (crates/walgit-server/build.rs reads web/dist). `pnpmDeps.hash` below must be bumped when
  # web/pnpm-lock.yaml changes: run `nix build .#web` and paste the hash nix prints.
  description = "walgit — git hosting on an object store";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, crane, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
        inherit (pkgs) lib;
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        version = (lib.importTOML ./Cargo.toml).workspace.package.version;

        rustSrc = lib.fileset.toSource {
          root = ./.;
          fileset = lib.fileset.unions [
            ./Cargo.toml
            ./Cargo.lock
            ./crates
            ./rust-toolchain.toml
          ];
        };

        web = pkgs.stdenv.mkDerivation (finalAttrs: {
          pname = "walgit-web";
          inherit version;
          src = lib.fileset.toSource {
            root = ./web;
            fileset = lib.fileset.unions [
              ./web/package.json
              ./web/pnpm-lock.yaml
              ./web/tsconfig.json
              ./web/vite.config.ts
              ./web/vite.sdk.config.ts
              ./web/index.html
              ./web/.oxlintrc.json
              ./web/src
              ./web/sdk
              ./web/plugins
            ];
          };
          pnpmDeps = pkgs.pnpm.fetchDeps {
            inherit (finalAttrs) pname version src;
            fetcherVersion = 2;
            hash = "sha256-bx1xLn1o1K4ECqdUvX9w5kNHxBD1R6L41pvhdtZg2Os=";
          };
          nativeBuildInputs = [ pkgs.nodejs_24 pkgs.pnpm.configHook ];
          buildPhase = ''
            runHook preBuild
            pnpm run build
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            cp -r dist "$out"
            test -f "$out/index.html" && test -f "$out/repos.js" && test -f "$out/repos.mjs"
            runHook postInstall
          '';
        });

        commonArgs = {
          pname = "walgit";
          inherit version;
          src = rustSrc;
          strictDeps = true;
          nativeBuildInputs = with pkgs; [ protobuf pkg-config cmake perl python3 ];
          cargoExtraArgs = "-p walgit-cli --locked";
          doCheck = false;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        walgit = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          env.WALGIT_BUILD_SHA = self.shortRev or self.dirtyShortRev or "dev";
          preConfigure = ''
            mkdir -p web
            cp -a ${web} web/dist
          '';
          # `walgit serve` shells out to git and GNU sort/comm for bounded pack planning.
          nativeBuildInputs = commonArgs.nativeBuildInputs ++ [ pkgs.makeWrapper ];
          postInstall = ''
            for b in walgit walgit-server; do
              wrapProgram "$out/bin/$b" --prefix PATH : ${lib.makeBinPath [ pkgs.git pkgs.git-lfs pkgs.coreutils ]}
            done
          '';
          meta = {
            description = "git hosting on an object store: smart HTTP, LFS, web UI — one binary";
            mainProgram = "walgit";
            license = lib.licenses.mit;
          };
        });

        image = pkgs.dockerTools.buildLayeredImage {
          name = "walgit";
          tag = version;
          contents = [ walgit pkgs.git pkgs.git-lfs pkgs.cacert pkgs.tini pkgs.dockerTools.binSh pkgs.coreutils ];
          extraCommands = ''
            mkdir -p etc/walgit tmp
            chmod 1777 tmp
            printf 'root:x:0:0:root:/root:/bin/sh\nwalgit:x:1000:1000:walgit:/home/walgit:/bin/sh\n' > etc/passwd
            printf 'root:x:0:\nwalgit:x:1000:\n' > etc/group
            mkdir -p home/walgit && chown 1000:1000 home/walgit
            cp ${./walgit.example.toml} etc/walgit/walgit.toml
          '';
          config = {
            Entrypoint = [ "${pkgs.tini}/bin/tini" "--" "${walgit}/bin/walgit" "serve" ];
            Cmd = [ "--config" "/etc/walgit/walgit.toml" ];
            User = "1000:1000";
            WorkingDir = "/home/walgit";
            ExposedPorts."8080/tcp" = { };
            Env = [
              "RUST_LOG=info,walgit=debug"
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
            ];
          };
        };
      in
      {
        packages = {
          default = walgit;
          inherit walgit web image;
        };

        apps.default = {
          type = "app";
          program = "${walgit}/bin/walgit";
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ walgit ];
          packages = with pkgs; [
            rustToolchain
            protobuf
            just
            git
            git-lfs
            curl
            lsof
            jdk_headless
            coreutils
            jq
            ripgrep
            fd
            nodejs_24
            pnpm
            podman
            podman-compose
          ];
          RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
        };

        formatter = pkgs.nixfmt-rfc-style;
      });
}
