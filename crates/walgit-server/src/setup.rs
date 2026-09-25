//! Client setup: the one-time installer (`/services/public/install.sh`), the git
//! credential helper it installs, the JSON recipes the web UI renders
//! (`/services/setup.json`) and the copy-pasteable text the auth errors show.
//! Everything derives from the request's public base URL and `server.auth`.
//!
//! The credential is a **token**: a walgit access token a signed-in browser minted
//! at `/_auth/tokens` (`oidc` mode), a static token from the server's config
//! (`token` mode), or an ID token a CI job mints itself. The installer takes it
//! from `WALGIT_TOKEN` or asks for it on the terminal, stores it in a file only the
//! user can read, and wires a small helper into git that answers `get` with it
//! (`authtype=Bearer`, git ≥ 2.46) and deletes it on `erase` (git's reaction to a
//! real 401), pointing at the token page. With `server.auth.mode = "none"` nothing
//! is asked. The UI is a client of these recipes, never a fork.

use serde::Serialize;
use walgit_config::{AuthMode, Config, TlsMode};

fn slug(host: &str) -> String {
    host.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Name of the helper the installer writes and wires into git (absolute path, not
/// looked up on `$PATH`): `<host>-credential-helper`, one per host so two walgit
/// servers coexist on one machine.
pub fn helper_file(host: &str) -> String {
    format!("{}-credential-helper", slug(host))
}

/// Where the helper keeps the token (mode 0600).
pub fn token_file(host: &str) -> String {
    format!("{}-token", slug(host))
}

/// Where the installer keeps a self-signed host certificate for git (`http.<url>.sslCAInfo`).
fn ca_file(host: &str) -> String {
    format!("{}-ca.pem", slug(host))
}

/// `server.tls.mode = "self_signed"`: clients have to pin our certificate.
fn self_signed(cfg: &Config) -> bool {
    cfg.server.tls.mode == TlsMode::SelfSigned
}

/// Whether clients need a credential at all.
fn needs_token(cfg: &Config) -> bool {
    cfg.server.auth.mode != AuthMode::None
}

/// Whether reads need one: packfile URI downloads then do too, so a clone asks for URIs with
/// `server-option=uri-auth=proactive` (`smart::uri_auth_proactive`).
fn protected_reads(cfg: &Config) -> bool {
    needs_token(cfg) && !cfg.server.auth.anonymous_read
}

#[derive(Debug, Clone, Serialize)]
pub struct Recipes {
    pub base_url: String,
    pub host: String,
    /// Where a signed-in browser mints an access token (`oidc` mode with a session secret),
    /// `None` when tokens come from the server's config or no credential is needed.
    pub token_url: Option<String>,
    /// `sh -c "$(curl … /services/public/install.sh[?repo=owner/name])"` — installs the helper
    /// and, with `repo`, execs straight into `git clone`.
    pub install: String,
    pub install_url: String,
    /// One-shot clone with a token in the environment, no helper.
    pub manual_clone: String,
    /// Plain clone once the helper is installed.
    pub plain_clone: String,
    /// The developer shape for big repositories: blobless and sparse.
    pub blobless_clone: String,
    /// Multi-line setup text (auth errors, overview `setup` field).
    pub setup_text: String,
    /// Self-signed TLS: where this host's certificate is published, and the one-time
    /// command that pins it for git. `None` when the certificate chains to a public CA.
    pub ca_url: Option<String>,
    pub trust: Option<String>,
}

pub fn host_of(base_url: &str) -> &str {
    base_url
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or("localhost")
}

/// `repo` is `owner/name` (no `.git`) when the recipes are for one repository.
pub fn recipes(cfg: &Config, base_url: &str, repo: Option<&str>) -> Recipes {
    let base_url = base_url.trim_end_matches('/').to_string();
    let host = host_of(&base_url).to_string();
    let token_url = (cfg.server.auth.mode == AuthMode::Oidc
        && cfg
            .server
            .auth
            .session_secret
            .as_deref()
            .is_some_and(|s| !s.is_empty()))
    .then(|| format!("{base_url}/_auth/tokens"));
    let install_url = match repo {
        Some(r) => format!("{base_url}/services/public/install.sh?repo={r}"),
        None => format!("{base_url}/services/public/install.sh"),
    };
    // `sh -c "$(curl …)"`, not `curl | sh`: the script's stdin stays the terminal, so the token
    // prompt can read it. The piped form still works — the script talks to /dev/tty when it can.
    // A self-signed origin: curl cannot verify us before the installer pinned the certificate, so
    // the bootstrap fetch is `-k`; everything after it is verified.
    let insecure = if self_signed(cfg) { "k" } else { "" };
    // The URL is single-quoted inside the substitution: an unquoted `?repo=` is a glob in zsh.
    let install = format!("sh -c \"$(curl -fsSL{insecure} '{install_url}')\"");
    let ca_url = self_signed(cfg).then(|| format!("{base_url}/services/public/ca.pem"));
    let trust = ca_url.as_ref().map(|ca| {
        let file = format!("${{XDG_CONFIG_HOME:-$HOME/.config}}/git/{}", ca_file(&host));
        format!("mkdir -p \"$(dirname \"{file}\")\" && curl -fsSk \"{ca}\" -o \"{file}\" && git config --global http.https://{host}/.sslCAInfo \"{file}\"")
    });
    let url = match repo {
        Some(r) => format!("{base_url}/{r}.git"),
        None => format!("{base_url}/<owner>/<repo>.git"),
    };
    let manual_clone = if needs_token(cfg) {
        format!("git -c http.extraHeader=\"Authorization: Bearer $WALGIT_TOKEN\" clone {url}")
    } else {
        format!("git clone {url}")
    };
    // Recorded in the clone's config, so later fetches ask too. Git reads it from 2.48; an
    // older git ignores it and fetches dynamically.
    let opt_in = if protected_reads(cfg) {
        " -c remote.origin.serverOption=uri-auth=proactive"
    } else {
        ""
    };
    let plain_clone = format!("git clone{opt_in} {url}");
    let blobless_clone = format!("git clone{opt_in} --filter=blob:none --sparse {url}");
    let trust_text = match &trust {
        Some(_) => format!(
            "# {host} presents a self-signed certificate: the installer pins it for git; browsers accept it once.\n"
        ),
        None => String::new(),
    };
    let where_from = match (&token_url, needs_token(cfg)) {
        (Some(u), _) => format!("# Tokens: sign in at {u} and create one (the installer asks for it; CI: export WALGIT_TOKEN).\n"),
        (None, true) => "# Tokens: issued by whoever runs this server (the installer asks for it; CI: export WALGIT_TOKEN).\n".to_string(),
        (None, false) => String::new(),
    };
    let setup_text = match repo {
        Some(_) => format!(
            "{trust_text}{where_from}# Run once per machine (installs the git credential helper and clones; safe to re-run):\n\
             {install}\n\
             \n\
             # Already set up? {plain_clone}\n\
             # One-shot (CI): {manual_clone}\n"
        ),
        None => format!(
            "{trust_text}{where_from}# Run once per machine (installs the git credential helper; safe to re-run):\n\
             {install}\n\
             {plain_clone}\n\
             \n\
             # One-shot (CI): {manual_clone}\n"
        ),
    };
    Recipes {
        base_url,
        host,
        token_url,
        install,
        install_url,
        manual_clone,
        plain_clone,
        blobless_clone,
        setup_text,
        ca_url,
        trust,
    }
}

/// The credential helper body (`~/.config/git/<helper_file(host)>`). Pure POSIX sh.
///
/// `get`: answers git with the stored token (`authtype=Bearer`, git ≥ 2.46) or `$WALGIT_TOKEN`.
/// `store`: keeps the password git hands it (so `git credential approve` works). `erase`: git
/// got a real 401 — delete the token and say where a new one comes from. `token`: print it.
pub fn helper_script(cfg: &Config, base_url: &str, host: &str) -> String {
    let token_url = recipes(cfg, base_url, None).token_url;
    let hint = match token_url {
        Some(u) => format!(
            "create a new one at {u}, then re-run the installer or: printf 'protocol=https\\nhost={host}\\nusername=token\\npassword=<token>\\n' | git credential approve"
        ),
        None => format!(
            "ask the operator of {host} for a new one, then re-run the installer or: printf 'protocol=https\\nhost={host}\\nusername=token\\npassword=<token>\\n' | git credential approve"
        ),
    };
    format!(
        r#"#!/bin/sh
# git credential helper for https://{host} (installed by {host}/services/public/install.sh).
# get: answers git with the stored token (authtype Bearer, git >= 2.46); store: saves the token
# git hands it; erase: the server answered 401 -> drop the token and say where a new one comes from.
F="${{XDG_CONFIG_HOME:-$HOME/.config}}/git/{token_file}"
token() {{
  [ -n "${{WALGIT_TOKEN:-}}" ] && {{ printf '%s\n' "$WALGIT_TOKEN"; return; }}
  [ -s "$F" ] && {{ cat "$F"; return; }}
  echo "{host}: no token stored; {hint}" >&2
  return 1
}}
case "${{1:-get}}" in
  token) token ;;
  get)
    while IFS= read -r line; do [ -z "$line" ] && break; done   # consume the request
    t="$(token)" || exit 1
    printf 'capability[]=authtype\nauthtype=Bearer\ncredential=%s\nusername=token\npassword=%s\n\n' "$t" "$t"
    ;;
  store)
    p=""; while IFS= read -r line; do [ -z "$line" ] && break; case "$line" in password=*) p="${{line#password=}}" ;; esac; done
    [ -z "$p" ] || {{ umask 077; mkdir -p "$(dirname "$F")"; printf '%s\n' "$p" > "$F.tmp" && mv "$F.tmp" "$F"; }}
    ;;
  erase)
    while IFS= read -r line; do [ -z "$line" ] && break; done
    [ -n "${{WALGIT_TOKEN:-}}" ] || {{ rm -f "$F"; echo "{host}: the token was rejected and has been removed; {hint}" >&2; }}
    ;;
  *) exit 0 ;;
esac
"#,
        token_file = token_file(host),
    )
}

/// The installer served at `/services/public/install.sh[?repo=owner/name]` (`sh -c "$(curl -fsSL …)"`).
/// Pure POSIX sh, **idempotent**: every step checks before it acts, re-running converges on the same state.
///
/// git ≥ 2.46 + curl → (self-signed: pin `/services/public/ca.pem`) → the credential helper → a token
/// (`$WALGIT_TOKEN`, an already stored one, or asked for on the terminal; no terminal: exit 2 with the
/// two things to do) → git config for the host (`credential.<host>.helper` = exactly ours,
/// stale `extraHeader` removed, `fetch.uriProtocols https`, and with a token
/// `http.<host>.proactiveAuth auto`)
/// → self-test (`/api/v1/me`, or `git ls-remote` of `repo`) → with `repo`, `git clone`.
pub fn install_script(cfg: &Config, base_url: &str, repo: Option<&str>) -> String {
    let r = recipes(cfg, base_url, repo);
    let host = &r.host;
    let delim = "__WALGIT_CREDENTIAL_HELPER__";
    let trust = match &r.ca_url {
        Some(ca) => format!(
            "# Self-signed origin: pin its certificate for git (browsers accept it once themselves).\n\
             CA=\"$DIR/{ca_file}\"\n\
             curl -fsSk \"{ca}\" -o \"$CA.tmp\" && mv \"$CA.tmp\" \"$CA\"\n\
             git config --global \"http.https://$HOST/.sslCAInfo\" \"$CA\"\n",
            ca_file = ca_file(host)
        ),
        None => String::new(),
    };
    let curl_ca = if r.ca_url.is_some() {
        "--cacert \"$CA\""
    } else {
        ""
    };
    let token_from = match &r.token_url {
        Some(u) => format!("sign in at {u} and create one"),
        None => format!("the operator of {host} issues them"),
    };
    let token = if needs_token(cfg) {
        format!(
            "# The credential: $WALGIT_TOKEN, else the stored one, else ask ({token_from}).\n\
             TF=\"$DIR/{token_file}\"\n\
             if [ -n \"${{WALGIT_TOKEN:-}}\" ]; then\n\
             \x20 ( umask 077; printf '%s\\n' \"$WALGIT_TOKEN\" > \"$TF.tmp\" && mv \"$TF.tmp\" \"$TF\" )\n\
             elif [ ! -s \"$TF\" ]; then\n\
             \x20 TTY=\"${{WALGIT_INSTALL_TTY:-/dev/tty}}\"   # tests point this at a file\n\
             \x20 if ! ( exec <\"$TTY\" >>\"$TTY\" ) 2>/dev/null; then\n\
             \x20   echo \"$HOST: no token and no terminal to ask on. Get one ({token_from}), then either:\" >&2\n\
             \x20   echo \"  WALGIT_TOKEN=<token> {reinstall}\" >&2\n\
             \x20   echo \"  or re-run the installer from a terminal\" >&2\n\
             \x20   exit 2\n\
             \x20 fi\n\
             \x20 printf '%s: paste an access token (%s): ' \"$HOST\" \"{token_from}\" >>\"$TTY\"\n\
             \x20 stty -echo <\"$TTY\" 2>/dev/null || true\n\
             \x20 IFS= read -r T <\"$TTY\" || T=\"\"\n\
             \x20 stty echo <\"$TTY\" 2>/dev/null || true\n\
             \x20 echo >>\"$TTY\"\n\
             \x20 [ -n \"$T\" ] || {{ echo \"$HOST: no token given\" >&2; exit 1; }}\n\
             \x20 ( umask 077; printf '%s\\n' \"$T\" > \"$TF.tmp\" && mv \"$TF.tmp\" \"$TF\" )\n\
             fi\n\
             TOKEN=\"${{WALGIT_TOKEN:-$(cat \"$TF\")}}\"\n\
             AUTH=\"Authorization: Bearer $TOKEN\"\n",
            token_file = token_file(host),
            // Inside a double-quoted sh string: the command carries `"`, `$(` and `'`.
            reinstall = r
                .install
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('$', "\\$"),
        )
    } else {
        "AUTH=\"X-Walgit-Anonymous: 1\"\n".to_string()
    };
    // Packfile URIs: git asks for them. With a token it also sends the token up front: a URI
    // download is a request of its own, and git gives up on its 401 instead of asking the helper.
    let uris = if needs_token(cfg) {
        "git config --global fetch.uriProtocols https\n\
         git config --global \"http.https://$HOST/.proactiveAuth\" auto\n"
    } else {
        "git config --global fetch.uriProtocols https\n"
    };
    let self_test = match repo {
        Some(rp) => format!(
            "# Self-test: the real thing, through the helper. Fails loudly with git's error.\n\
             git ls-remote \"{base}/{rp}.git\" HEAD >/dev/null || {{ echo \"$HOST: self-test failed — git ls-remote {base}/{rp}.git did not succeed (see above)\" >&2; exit 1; }}\n",
            base = r.base_url,
        ),
        None => format!(
            "# Self-test: the token against the server (repository-independent, so it holds on an empty server).\n\
             WHO=\"$(curl -fsS {curl_ca} -H \"$AUTH\" \"{base}/api/v1/me\")\" || {{ echo \"$HOST: self-test failed — {base}/api/v1/me refused the token ({token_from})\" >&2; exit 1; }}\n\
             ACCOUNT=\"$(printf '%s' \"$WHO\" | sed -n 's/.*\"principal\":\"\\([^\"]*\\)\".*/\\1/p')\"\n",
            base = r.base_url,
        ),
    };
    let tail = match repo {
        Some(_) => format!(
            "echo \"$HOST: ready — cloning\"\n\
             exec {plain}\n",
            plain = r.plain_clone,
        ),
        None => format!(
            "# $1 = optional owner/repository  (`sh -c \"$(curl …/install.sh)\" -- owner/repository`)\n\
             REPO=\"${{1:-}}\"\n\
             case \"$REPO\" in\n\
             \"\") echo \"$HOST: ready — git authenticates as ${{ACCOUNT:-$HOST user}}\"\n\
             \x20    echo \"From a local git tree: {install} -- owner/repository && git push -u origin HEAD\"\n\
             \x20    ;;\n\
             *) case \"$REPO\" in *.git) REPO=\"${{REPO%.git}}\" ;; esac\n\
             \x20  case \"$REPO\" in *[!A-Za-z0-9._/-]*|*/*/*|/*|*/*/) echo \"$HOST: repository must be owner/repository[.git]\" >&2; exit 1 ;; esac\n\
             \x20  case \"$REPO\" in */*) ;; *) echo \"$HOST: repository must be owner/repository[.git]\" >&2; exit 1 ;; esac\n\
             \x20  URL=\"{base}/$REPO.git\"\n\
             \x20  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then\n\
             \x20    git remote get-url origin >/dev/null 2>&1 && git remote set-url origin \"$URL\" || git remote add origin \"$URL\"\n\
             \x20    echo \"$HOST: origin → $URL  (git push -u origin HEAD)\"\n\
             \x20  else\n\
             \x20    echo \"$HOST: ready. Then: git remote add origin $URL && git push -u origin HEAD\"\n\
             \x20  fi\n\
             \x20  ;;\n\
             esac\n",
            install = r.install,
            base = r.base_url,
        ),
    };
    format!(
        r#"#!/bin/sh
# {host} — git setup in one command, idempotent (re-run any time; each step converges):
#   stores an access token for https://{host} in a file only you can read, installs a git
#   credential helper that hands it to git, self-tests, and with
#   ?repo=owner/name clones that repository right away.
# Remove: git config --global --remove-section credential.https://{host}; rm -f ~/.config/git/{token_file}
set -eu
HOST="{host}"
DIR="${{XDG_CONFIG_HOME:-$HOME/.config}}/git"
HELPER="$DIR/{helper_file}"
command -v git >/dev/null 2>&1 || {{ echo "$HOST: git not found" >&2; exit 1; }}
command -v curl >/dev/null 2>&1 || {{ echo "$HOST: curl not found" >&2; exit 1; }}
GV="$(git --version | sed 's/^git version //; s/[^0-9.].*$//')"
GMAJ="${{GV%%.*}}"; GREST="${{GV#*.}}"; GMIN="${{GREST%%.*}}"
[ "$GMAJ" -gt 2 ] 2>/dev/null || [ "$GMAJ" -eq 2 ] && [ "${{GMIN:-0}}" -ge 46 ] || {{
  echo "$HOST: git $GV is too old — need git >= 2.46 (credential authtype)" >&2; exit 1; }}
mkdir -p "$DIR"
{trust}{token}cat > "$HELPER.tmp" <<'{delim}'
{helper}{delim}
chmod 755 "$HELPER.tmp"
if cmp -s "$HELPER.tmp" "$HELPER" 2>/dev/null; then rm -f "$HELPER.tmp"; else mv "$HELPER.tmp" "$HELPER"; fi

# helper list for this host = exactly ours ("" resets inherited helpers).
git config --global --replace-all "credential.https://$HOST.helper" ""
git config --global --add "credential.https://$HOST.helper" "$HELPER"
git config --global --unset-all "http.https://$HOST/.extraHeader" 2>/dev/null || true
{uris}{self_test}{tail}"#,
        helper_file = helper_file(host),
        token_file = token_file(host),
        helper = helper_script(cfg, &r.base_url, host),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::{Command, Stdio};

    const HOST: &str = "git.example.com";
    const BASE: &str = "https://git.example.com";

    fn oidc_cfg() -> Config {
        let mut cfg = Config::default();
        cfg.server.auth.mode = AuthMode::Oidc;
        cfg.server.auth.anonymous_read = false;
        cfg.server.auth.session_secret = Some("0123456789abcdef0123456789abcdef-secret".into());
        cfg.server.auth.allowed_domains = vec!["example.com".into()];
        cfg
    }

    /// `sh -n` (and dash/bash when present): every generated script must parse.
    fn assert_posix(script: &str) {
        for sh in ["sh", "dash", "bash"] {
            let Ok(mut child) = Command::new(sh)
                .arg("-n")
                .stdin(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
            else {
                continue;
            };
            child
                .stdin
                .take()
                .unwrap()
                .write_all(script.as_bytes())
                .unwrap();
            let out = child.wait_with_output().unwrap();
            assert!(
                out.status.success(),
                "{sh} -n rejected the script: {}\n{script}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    #[test]
    fn recipes_point_at_the_token_page_and_ask_for_packfile_uris() {
        let cfg = oidc_cfg();
        let r = recipes(&cfg, "https://git.example.com/", Some("acme/monorepo"));
        assert_eq!(r.host, HOST);
        assert_eq!(
            r.token_url.as_deref(),
            Some("https://git.example.com/_auth/tokens")
        );
        assert_eq!(
            r.install,
            "sh -c \"$(curl -fsSL 'https://git.example.com/services/public/install.sh?repo=acme/monorepo')\"",
            "URL quoted (zsh globs `?`)"
        );
        assert!(
            r.manual_clone
                .contains("Authorization: Bearer $WALGIT_TOKEN")
        );
        assert!(
            r.manual_clone
                .ends_with("clone https://git.example.com/acme/monorepo.git")
        );
        assert_eq!(
            r.plain_clone,
            "git clone -c remote.origin.serverOption=uri-auth=proactive https://git.example.com/acme/monorepo.git"
        );
        assert_eq!(
            r.blobless_clone,
            "git clone -c remote.origin.serverOption=uri-auth=proactive --filter=blob:none --sparse https://git.example.com/acme/monorepo.git"
        );
        assert!(
            r.setup_text
                .starts_with("# Tokens: sign in at https://git.example.com/_auth/tokens"),
            "{}",
            r.setup_text
        );
        assert_eq!(
            r.install_url,
            "https://git.example.com/services/public/install.sh?repo=acme/monorepo"
        );

        // Static-token server: no token page, the operator issues them.
        let mut st = Config::default();
        st.server.auth.mode = AuthMode::Token;
        let r = recipes(&st, BASE, None);
        assert!(r.token_url.is_none());
        assert!(r.setup_text.contains("issued by whoever runs this server"));
        // Anonymous reads get URIs without asking.
        assert!(!r.plain_clone.contains("serverOption"), "{}", r.plain_clone);

        // No auth at all: nothing about tokens.
        let r = recipes(&Config::default(), "http://localhost:8080", None);
        assert!(!r.setup_text.contains("oken"), "{}", r.setup_text);
        assert!(!r.manual_clone.contains("Authorization"));
        assert!(!r.plain_clone.contains("serverOption"), "{}", r.plain_clone);
        let script = install_script(&Config::default(), "http://localhost:8080", None);
        assert!(script.contains("fetch.uriProtocols https"));
        assert!(!script.contains("proactiveAuth"), "no credential to send");
    }

    #[test]
    fn self_signed_host_pins_its_certificate_for_git() {
        let mut cfg = Config::default();
        cfg.server.tls.mode = TlsMode::SelfSigned;
        let r = recipes(&cfg, "https://walgit.localhost:8888", Some("me/repo"));
        assert_eq!(
            r.ca_url.as_deref(),
            Some("https://walgit.localhost:8888/services/public/ca.pem")
        );
        let trust = r.trust.as_deref().unwrap();
        assert!(
            trust.contains("http.https://walgit.localhost:8888/.sslCAInfo"),
            "{trust}"
        );
        assert!(
            r.install.starts_with("sh -c \"$(curl -fsSLk "),
            "{}",
            r.install
        );
        assert!(
            r.setup_text
                .starts_with("# walgit.localhost:8888 presents a self-signed certificate")
        );
        let script = install_script(&cfg, "https://walgit.localhost:8888", Some("me/repo"));
        assert_posix(&script);
        assert!(script.contains("walgit-localhost-8888-ca.pem"));
        assert!(script.contains("walgit-localhost-8888-credential-helper"));
        assert!(script.contains("sslCAInfo"));
        assert_eq!(helper_file(HOST), "git-example-com-credential-helper");
        assert_eq!(token_file(HOST), "git-example-com-token");
    }

    #[test]
    fn helper_and_installer_are_posix() {
        let cfg = oidc_cfg();
        let helper = helper_script(&cfg, BASE, HOST);
        assert_posix(&helper);
        assert!(helper.contains("capability[]=authtype"));
        assert!(
            helper.contains("_auth/tokens"),
            "erase says where a new token comes from"
        );
        for repo in [None, Some("acme/monorepo")] {
            let script = install_script(&cfg, BASE, repo);
            assert_posix(&script);
            assert!(script.contains("-ge 46"), "git >= 2.46 required");
            assert!(
                script.contains("stty -echo"),
                "the token prompt does not echo"
            );
            assert_eq!(
                script.contains("git ls-remote \"https://git.example.com/acme/monorepo.git\" HEAD"),
                repo.is_some(),
                "with a repo the self-test is ls-remote of that repo"
            );
            assert_eq!(
                script.contains("/api/v1/me"),
                repo.is_none(),
                "without one it is the token against /api/v1/me (no repo assumed)"
            );
            assert!(script.contains("git-example-com-credential-helper"));
            assert!(
                script.contains(&helper),
                "the installer embeds the helper verbatim"
            );
            assert_eq!(
                script.contains("exec git clone -c remote.origin.serverOption=uri-auth=proactive https://git.example.com/acme/monorepo.git"),
                repo.is_some(),
                "the installer clones the requested repository, asking for packfile URIs"
            );
        }
        // No auth: the installer never asks for a token.
        let open = install_script(&Config::default(), "http://localhost:8080", None);
        assert_posix(&open);
        assert!(!open.contains("paste an access token"));
    }

    #[test]
    fn helper_get_store_erase_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let helper = dir.path().join("helper");
        std::fs::write(&helper, helper_script(&oidc_cfg(), BASE, HOST)).unwrap();
        let run = |op: &str, input: &str, env_token: Option<&str>| {
            let mut cmd = Command::new("sh");
            cmd.arg(&helper)
                .arg(op)
                .env("HOME", dir.path())
                .env("XDG_CONFIG_HOME", dir.path().join("xdg"))
                .env_remove("WALGIT_TOKEN");
            if let Some(t) = env_token {
                cmd.env("WALGIT_TOKEN", t);
            }
            let mut child = cmd
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        };
        let req = "protocol=https\nhost=git.example.com\n\n";
        // Nothing stored: get fails and says so.
        let out = run("get", req, None);
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("no token stored"));
        // store → get
        let out = run(
            "store",
            "protocol=https\nhost=git.example.com\nusername=token\npassword=wgt_abc\n\n",
            None,
        );
        assert!(out.status.success());
        let tf = dir.path().join("xdg/git").join(token_file(HOST));
        assert_eq!(std::fs::read_to_string(&tf).unwrap(), "wgt_abc\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&tf).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let out = run("get", req, None);
        assert!(out.status.success());
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            "capability[]=authtype\nauthtype=Bearer\ncredential=wgt_abc\nusername=token\npassword=wgt_abc\n\n"
        );
        // The environment wins over the file.
        let out = run("get", req, Some("wgt_env"));
        assert!(String::from_utf8_lossy(&out.stdout).contains("credential=wgt_env"));
        // erase (a 401) removes the file and points at the token page.
        let out = run("erase", req, None);
        assert!(out.status.success());
        assert!(!tf.exists());
        assert!(String::from_utf8_lossy(&out.stderr).contains("_auth/tokens"));
    }

    /// The installer run twice with a fake `curl` and real git against a private global config:
    /// same config after both runs, one helper, every key exactly once, the token asked for once.
    #[test]
    fn installer_is_idempotent() {
        let dir = installer_harness();
        let gitconfig = dir.path().join("gitconfig");
        std::fs::write(dir.path().join("tty"), "wgt_pasted\n").unwrap();
        let run = || {
            let out = run_installer(&dir, &install_script(&oidc_cfg(), BASE, None), &[]);
            assert!(
                out.status.success(),
                "installer failed:\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            std::fs::read_to_string(&gitconfig).unwrap()
        };
        let first = run();
        let second = run();
        assert_eq!(first, second, "a second run must change nothing");
        let tf = dir.path().join("xdg/git").join(token_file(HOST));
        assert_eq!(std::fs::read_to_string(&tf).unwrap(), "wgt_pasted\n");
        assert_eq!(first.matches("helper = \n").count(), 1, "{first}");
        for key in ["helper = /", "uriProtocols = https", "proactiveAuth = auto"] {
            assert_eq!(first.matches(key).count(), 1, "{key}: {first}");
        }
        let files: Vec<_> = std::fs::read_dir(dir.path().join("xdg/git"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            files.len(),
            2,
            "helper + token, no .tmp left behind: {files:?}"
        );
    }

    #[test]
    fn installer_takes_the_token_from_the_environment() {
        let dir = installer_harness();
        let out = run_installer(
            &dir,
            &install_script(&oidc_cfg(), BASE, None),
            &[("WALGIT_TOKEN", "wgt_ci")],
        );
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let tf = dir.path().join("xdg/git").join(token_file(HOST));
        assert_eq!(std::fs::read_to_string(&tf).unwrap(), "wgt_ci\n");
        assert!(
            std::fs::read_to_string(dir.path().join("tty"))
                .unwrap()
                .is_empty(),
            "nothing asked on the terminal"
        );
    }

    /// Fake `curl` (the self-test's server) under `<dir>/bin`; real git throughout.
    fn installer_harness() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(
            bin.join("curl"),
            "#!/bin/sh\necho '{\"principal\":\"dev@example.com\",\"write\":true}'\nexit 0\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(bin.join("curl"), std::fs::Permissions::from_mode(0o755))
                .unwrap();
        }
        std::fs::write(dir.path().join("tty"), "").unwrap();
        dir
    }

    fn run_installer(
        dir: &tempfile::TempDir,
        script: &str,
        env: &[(&str, &str)],
    ) -> std::process::Output {
        let bin = dir.path().join("bin");
        let mut cmd = Command::new("sh");
        cmd.arg("-s")
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    bin.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .env("HOME", dir.path())
            .env("XDG_CONFIG_HOME", dir.path().join("xdg"))
            .env("GIT_CONFIG_GLOBAL", dir.path().join("gitconfig"))
            .env("WALGIT_INSTALL_TTY", dir.path().join("tty"))
            .env_remove("WALGIT_TOKEN");
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                c.stdin
                    .take()
                    .unwrap()
                    .write_all(script.as_bytes())
                    .unwrap();
                c.wait_with_output()
            })
            .unwrap()
    }

    /// `curl … | sh` on a headless box with no token: the script must not try to read one — it
    /// prints what to do and exits 2.
    #[test]
    fn installer_without_a_terminal_and_no_token_exits_2_with_instructions() {
        let dir = installer_harness();
        let script = install_script(&oidc_cfg(), BASE, Some("acme/monorepo"));
        let out = Command::new("sh")
            .arg("-s")
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    dir.path().join("bin").display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .env("HOME", dir.path())
            .env("XDG_CONFIG_HOME", dir.path().join("xdg"))
            .env("GIT_CONFIG_GLOBAL", dir.path().join("gitconfig"))
            .env("WALGIT_INSTALL_TTY", dir.path().join("no-such-tty/none"))
            .env_remove("WALGIT_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                c.stdin
                    .take()
                    .unwrap()
                    .write_all(script.as_bytes())
                    .unwrap();
                c.wait_with_output()
            })
            .unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{err}");
        assert!(err.contains("no token and no terminal"), "{err}");
        assert!(
            err.contains("https://git.example.com/_auth/tokens"),
            "{err}"
        );
        assert!(err.contains("WALGIT_TOKEN=<token> sh -c \"$(curl -fsSL 'https://git.example.com/services/public/install.sh?repo=acme/monorepo')\""), "{err}");
    }
}
