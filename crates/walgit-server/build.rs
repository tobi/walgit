//! Make `cargo build`/`cargo test` work in a fresh checkout without running the
//! web build. `rust-embed` requires `../../web/dist` to exist at compile time.
//! Development builds may use a placeholder when the SPA has not been built;
//! release builds fail instead so a deployable artifact cannot silently omit UI.

use std::fs;
use std::path::Path;

const PLACEHOLDER: &str = "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><title>walgit</title></head>\n\
<body><p>walgit web UI is not built in this binary. Run <code>just web-build</code> (vite via pnpm) and rebuild.</p></body></html>\n";

fn main() {
    println!("cargo:rustc-env=WALGIT_BUILD_SHA={}", build_sha());
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dist = manifest.join("../../web/dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    println!("cargo:rerun-if-env-changed=PROFILE");
    let index = dist.join("index.html");
    if !index.exists() {
        let release = std::env::var("PROFILE").as_deref() == Ok("release");
        if release {
            panic!("web/dist/index.html is missing in a release build; run `just web-build` first");
        }
        fs::create_dir_all(&dist).expect("create web/dist");
        fs::write(&index, PLACEHOLDER).expect("write placeholder web/dist/index.html");
        println!(
            "cargo:warning=web/dist was missing; wrote a development placeholder index.html (run `just web-build` for the real UI)"
        );
    }
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
