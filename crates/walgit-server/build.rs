//! Make `cargo build`/`cargo test` work in a fresh checkout without running the
//! web build. `rust-embed` requires `../../web/dist` to exist at compile time;
//! when the SPA has not been built (`just web-build`) we drop in a placeholder
//! `index.html` so the server compiles and the HTML routes still answer 200.
//! A real `pnpm run build` overwrites the placeholder.

use std::fs;
use std::path::Path;

const PLACEHOLDER: &str = "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><title>walgit</title></head>\n\
<body><p>walgit web UI is not built in this binary. Run <code>just web-build</code> (vite via pnpm) and rebuild.</p></body></html>\n";

fn main() -> std::io::Result<()> {
    println!("cargo:rustc-env=WALGIT_BUILD_SHA={}", build_sha());
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dist = manifest.join("../../web/dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    let index = dist.join("index.html");
    if !index.exists() {
        fs::create_dir_all(&dist)?;
        fs::write(&index, PLACEHOLDER)?;
        println!(
            "cargo:warning=web/dist was missing; wrote a placeholder index.html (run `just web-build` for the real UI)"
        );
    }
    Ok(())
}

/// Build identity for `/healthz` (`version`) and `walgit --version`: the commit
/// the binary was built from. A container or package build may pass it as
/// `WALGIT_BUILD_SHA` (an archived source tree has no `.git`); a checkout
/// falls back to `git rev-parse --short=12 HEAD`; otherwise "dev".
fn build_sha() -> String {
    println!("cargo:rerun-if-env-changed=WALGIT_BUILD_SHA");
    if let Ok(s) = std::env::var("WALGIT_BUILD_SHA") {
        let s = s.trim().to_string();
        if !s.is_empty() {
            return s;
        }
    }
    std::process::Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "dev".to_string())
}
