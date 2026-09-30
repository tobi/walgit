//! Ref discovery and named object packaging policies. Neither grants access.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackGroupKind {
    Code,
    Meta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackGroupConfig {
    pub kind: PackGroupKind,
    pub include: Vec<String>,
    #[serde(default)]
    pub subtract: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RefsConfig {
    pub advertise: Vec<String>,
    pub packfiles: BTreeMap<String, PackGroupConfig>,
}

impl Default for RefsConfig {
    fn default() -> Self {
        Self {
            advertise: vec!["refs/heads/*".into(), "refs/tags/*".into()],
            packfiles: BTreeMap::from([
                (
                    "code".into(),
                    PackGroupConfig {
                        kind: PackGroupKind::Code,
                        include: vec!["refs/*".into()],
                        subtract: vec![],
                    },
                ),
                (
                    "meta".into(),
                    PackGroupConfig {
                        kind: PackGroupKind::Meta,
                        include: vec!["refs/meta".into(), "refs/meta/*".into()],
                        subtract: vec![],
                    },
                ),
            ]),
        }
    }
}

pub fn is_metadata_ref(name: &str) -> bool {
    name == "refs/meta" || name.starts_with("refs/meta/")
}

#[expect(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "Git refname rules reject the case-sensitive .lock suffix"
)]
fn valid_ref(name: &str) -> bool {
    name.starts_with("refs/")
        && !name.ends_with('/')
        && !name.ends_with('.')
        && !name.contains("..")
        && !name.contains("@{")
        && !name
            .bytes()
            .any(|b| b <= b' ' || b == 127 || b"~^:?*[\\".contains(&b))
        && name
            .split('/')
            .all(|p| !p.is_empty() && !p.starts_with('.') && !p.ends_with(".lock"))
}

/// `refs/heads/<name>` for a short branch name (`main`, `release/1.x`), refused
/// unless Git would accept it as a branch: HEAD's target is always a branch, and
/// a full ref name here (`refs/heads/main`) would silently become
/// `refs/heads/refs/heads/main`. One rule for `git.default_branch`, the create
/// override and the HEAD admin route.
pub fn branch_ref(name: &str) -> Result<String> {
    ensure!(
        !name.is_empty() && name != "HEAD" && !name.starts_with('-'),
        "invalid branch name {name:?}"
    );
    ensure!(
        !name.starts_with("refs/"),
        "a branch name is short (\"main\"), not a ref (got {name:?})"
    );
    let full = format!("refs/heads/{name}");
    ensure!(valid_ref(&full), "invalid branch name {name:?}");
    Ok(full)
}

fn validate_selectors(selectors: &[String]) -> Result<()> {
    ensure!(selectors.len() <= 256, "at most 256 ref selectors per list");
    for selector in selectors {
        let pattern = selector.strip_prefix('!').unwrap_or(selector);
        let valid = if pattern == "HEAD" {
            true
        } else if let Some(prefix) = pattern.strip_suffix('*') {
            // Complete the trailing byte prefix into a syntactically valid ref.
            valid_ref(&format!("{prefix}x"))
        } else {
            valid_ref(pattern)
        };
        ensure!(valid, "invalid ref selector {selector:?}");
    }
    Ok(())
}

/// Match a validated selector's positive pattern. HEAD uses the captured symbolic target.
pub fn selector_matches(pattern: &str, name: &str, head_target: &str) -> bool {
    if pattern == "HEAD" {
        return !head_target.is_empty() && name == head_target;
    }
    pattern
        .strip_suffix('*')
        .map_or(name == pattern, |p| name.starts_with(p))
}

/// Ordered exclusions, last matching selector wins. No match means excluded.
pub fn selectors_match(selectors: &[String], name: &str, head_target: &str) -> bool {
    let mut included = false;
    for selector in selectors {
        let (negative, pattern) = selector
            .strip_prefix('!')
            .map_or((false, selector.as_str()), |s| (true, s));
        if selector_matches(pattern, name, head_target) {
            included = !negative;
        }
    }
    included
}

impl PackGroupConfig {
    pub fn matches_ref(&self, name: &str, head_target: &str) -> bool {
        (is_metadata_ref(name) == (self.kind == PackGroupKind::Meta))
            && selectors_match(&self.include, name, head_target)
    }
}

impl RefsConfig {
    pub fn validate(&self) -> Result<()> {
        fn visit<'a>(
            name: &'a str,
            groups: &'a BTreeMap<String, PackGroupConfig>,
            visiting: &mut BTreeSet<&'a str>,
            done: &mut BTreeSet<&'a str>,
        ) -> Result<()> {
            if done.contains(name) {
                return Ok(());
            }
            ensure!(
                visiting.insert(name),
                "pack group dependency cycle at {name}"
            );
            if let Some(group) = groups.get(name) {
                for dep in &group.subtract {
                    visit(dep, groups, visiting, done)?;
                }
            }
            visiting.remove(name);
            done.insert(name);
            Ok(())
        }

        ensure!(self.packfiles.len() <= 32, "at most 32 pack groups");
        validate_selectors(&self.advertise)?;
        let mut bytes = self.advertise.iter().map(String::len).sum::<usize>();
        for (name, group) in &self.packfiles {
            ensure!(
                !name.is_empty()
                    && !name.starts_with('_')
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "invalid pack group name {name:?}"
            );
            validate_selectors(&group.include)?;
            ensure!(
                group.subtract.len() <= 32,
                "too many dependencies for group {name}"
            );
            let unique: BTreeSet<_> = group.subtract.iter().collect();
            ensure!(
                unique.len() == group.subtract.len(),
                "duplicate dependencies for group {name}"
            );
            bytes += name.len()
                + group.include.iter().map(String::len).sum::<usize>()
                + group.subtract.iter().map(String::len).sum::<usize>();
            for dep in &group.subtract {
                let target = self
                    .packfiles
                    .get(dep)
                    .ok_or_else(|| anyhow::anyhow!("unknown dependency {dep} for group {name}"))?;
                ensure!(
                    group.kind == PackGroupKind::Code && target.kind == PackGroupKind::Code,
                    "subtraction is permitted only between code groups"
                );
            }
        }
        ensure!(bytes <= 16 * 1024, "ref policy exceeds 16 KiB");
        let mut done = BTreeSet::new();
        for name in self.packfiles.keys() {
            visit(name, &self.packfiles, &mut BTreeSet::new(), &mut done)?;
        }
        Ok(())
    }

    /// Stable length-framed group policy hash. Advertisement is intentionally excluded.
    pub fn policy_identity(&self) -> String {
        fn field(hash: &mut Sha256, value: &[u8]) {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        let mut hash = Sha256::new();
        hash.update(b"walgit-pack-policy-v1\0");
        hash.update((self.packfiles.len() as u64).to_be_bytes());
        for (name, group) in &self.packfiles {
            field(&mut hash, name.as_bytes());
            field(
                &mut hash,
                match group.kind {
                    PackGroupKind::Code => b"code",
                    PackGroupKind::Meta => b"meta",
                },
            );
            hash.update((group.include.len() as u64).to_be_bytes());
            for selector in &group.include {
                field(&mut hash, selector.as_bytes());
            }
            let deps: BTreeSet<_> = group.subtract.iter().collect();
            hash.update((deps.len() as u64).to_be_bytes());
            for dep in deps {
                field(&mut hash, dep.as_bytes());
            }
        }
        hex::encode(hash.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_keeps_metadata_separate_and_resolves_head() {
        let cfg = RefsConfig::default();
        cfg.validate().unwrap();
        assert!(cfg.packfiles["code"].matches_ref("refs/heads/topic", "refs/heads/main"));
        assert!(!cfg.packfiles["code"].matches_ref("refs/meta/main", "refs/heads/main"));
        assert!(cfg.packfiles["meta"].matches_ref("refs/meta/main", "refs/heads/main"));
        let selectors = ["refs/*", "!refs/heads/*", "HEAD"].map(str::to_owned);
        assert!(selectors_match(
            &selectors,
            "refs/heads/trunk",
            "refs/heads/trunk"
        ));
        assert!(!selectors_match(
            &selectors,
            "refs/heads/main",
            "refs/heads/trunk"
        ));
    }
    #[test]
    fn policy_identity_ignores_discovery_but_tracks_packaging() {
        let mut cfg = RefsConfig::default();
        let id = cfg.policy_identity();
        cfg.advertise = vec!["HEAD".into()];
        assert_eq!(id, cfg.policy_identity());
        cfg.packfiles.get_mut("code").unwrap().include = vec!["HEAD".into()];
        assert_ne!(id, cfg.policy_identity());
    }
    #[test]
    fn invalid_selectors_dependencies_cycles_and_limits_fail() {
        for bad in [
            "*",
            "refs/**",
            "refs/a?",
            "refs/a..b",
            "refs/.hidden",
            "refs/a.lock",
            "!",
            "refs//x",
            "refs/x@{a",
        ] {
            let cfg = RefsConfig {
                advertise: vec![bad.into()],
                ..Default::default()
            };
            assert!(cfg.validate().is_err(), "{bad}");
        }
        let mut cfg = RefsConfig::default();
        cfg.packfiles.get_mut("code").unwrap().subtract = vec!["meta".into()];
        assert!(cfg.validate().is_err());
        cfg.packfiles.get_mut("code").unwrap().subtract = vec!["missing".into()];
        assert!(cfg.validate().is_err());
        cfg.packfiles.get_mut("code").unwrap().subtract = vec!["code".into()];
        assert!(cfg.validate().is_err());
        let cfg = RefsConfig {
            advertise: vec!["HEAD".into(); 257],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }
    #[test]
    fn branch_names_are_short_and_git_valid() {
        assert_eq!(branch_ref("main").unwrap(), "refs/heads/main");
        assert_eq!(branch_ref("release/1.x").unwrap(), "refs/heads/release/1.x");
        for bad in [
            "",
            "HEAD",
            "-x",
            "refs/heads/main",
            "a..b",
            "a b",
            "a/",
            "a.lock",
            ".hidden",
            "a//b",
            "x@{1}",
            "a\nb",
        ] {
            assert!(branch_ref(bad).is_err(), "{bad:?}");
        }
    }
}

#[cfg(test)]
mod config_tests {
    use crate::Config;
    #[test]
    fn settings_merge_group_fields_and_require_new_group_definitions() {
        let base = Config::default();
        let effective=base.with_settings("[refs.packfiles.code]\ninclude = [\"HEAD\"]\n[packfile_uri]\nmax_uris_per_fetch = 8\n").unwrap();
        assert_eq!(effective.refs.packfiles["code"].include, ["HEAD"]);
        assert_eq!(
            effective.refs.packfiles["code"].kind,
            super::PackGroupKind::Code
        );
        assert!(effective.refs.packfiles.contains_key("meta"));
        assert_eq!(effective.packfile_uri.max_uris_per_fetch, 8);
        for text in [
            "[refs.packfiles.new]\ninclude = []",
            "[refs.packfiles.new]\nkind = \"code\"",
            "[refs]\nunknown = true",
            "[packfile_uri]\nunknown = true",
            "[packfile_uri]\nmax_uris_per_fetch = 0",
        ] {
            assert!(base.with_settings(text).is_err(), "{text}");
        }
        let effective=base.with_settings("[refs.packfiles.extra]\nkind = \"code\"\ninclude = [\"refs/heads/*\"]\nsubtract = [\"code\"]").unwrap();
        assert_eq!(effective.refs.packfiles.len(), 3);
    }
}
