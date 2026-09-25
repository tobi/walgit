//! Immutable pack downloads use ordinary read auth and exact durable membership.
use axum::http::{HeaderMap, Method};
use axum::response::Response;

use crate::{AppState, error::ApiError, repo::RepoRoute, smart, static_object};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use walgit_git::packfile_uri::{PackfileUri, Selection};
use walgit_proto::v1::{PackAudience, PackGroupCoverage, PackKind, RefSnapshot};
use walgit_wal::{FetchView, RepoHandle};

/// Called only after native-engine dispatch. A protected repository reaches it only
/// for a client that sent `server-option=uri-auth=proactive` (its URI downloads carry
/// the same credential); every other protected client stays on dynamic transfer.
pub async fn select(
    handle: &RepoHandle,
    request: &walgit_git::UploadPackRequest,
    base_url: &str,
) -> Option<Selection> {
    if !request.done
        || request.wants.is_empty()
        || !request.want_refs.is_empty()
        || !request.shallow.is_empty()
        || request.deepen.is_some()
        || request.deepen_since.is_some()
        || !request.deepen_not.is_empty()
        || request.filter.as_deref().is_some_and(|f| f != "blob:none")
    {
        return None;
    }
    let protocol = base_url.split_once("://")?.0;
    if !request
        .packfile_uris_protocols
        .iter()
        .any(|p| p == protocol)
    {
        return None;
    }
    let view = handle.fetch_view().await.ok()?;
    let policy = view.config.refs.policy_identity();
    let mut candidates: Vec<_> = view
        .manifest
        .packs
        .iter()
        .flat_map(|p| &p.group_coverages)
        .filter(|c| {
            c.ref_policy == policy
                && c.covers_seq <= view.manifest.head_seq
                && !c.refs_key.is_empty()
        })
        .map(|c| (c.covers_seq, c.refs_key.clone()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    candidates.reverse();
    // Shared request budget, not three attempts per group or dependency.
    for (seq, key) in candidates.into_iter().take(3) {
        let snapshot = match handle.read_coverage_snapshot(&view, &key, seq).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(repo = %handle.id(), %error, "coverage snapshot unavailable; dynamic fetch remains available");
                continue;
            }
        };
        if let Some(mut selection) = select_snapshot(&view, request, &snapshot, &key, base_url) {
            if request.packfile_indexes {
                for pack in &mut selection.packs {
                    pack.index = local_index_uri(handle, pack, base_url).await;
                }
            }
            return Some(selection);
        }
    }
    None
}

async fn local_index_uri(
    handle: &RepoHandle,
    pack: &PackfileUri,
    base: &str,
) -> Option<walgit_git::packfile_uri::IndexUri> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = tokio::fs::File::open(
        handle
            .local()
            .pack_path(&pack.checksum)
            .with_extension("idx"),
    )
    .await
    .ok()?;
    let length = pack.checksum.kind().len_in_bytes();
    file.seek(std::io::SeekFrom::End(-i64::try_from(length).ok()?))
        .await
        .ok()?;
    let mut trailer = vec![0; length];
    file.read_exact(&mut trailer).await.ok()?;
    Some(walgit_git::packfile_uri::IndexUri {
        checksum: gix_hash::ObjectId::from_hex(hex::encode(trailer).as_bytes()).ok()?,
        url: format!(
            "{}/packfiles/{}.idx",
            base.trim_end_matches('/'),
            pack.checksum
        ),
    })
}

fn select_snapshot(
    view: &FetchView,
    request: &walgit_git::UploadPackRequest,
    snapshot: &RefSnapshot,
    key: &str,
    base_url: &str,
) -> Option<Selection> {
    let cfg = &view.config;
    let policy = cfg.refs.policy_identity();
    let live: BTreeMap<_, _> = view
        .manifest
        .packs
        .iter()
        .map(|p| (p.checksum.as_str(), p))
        .collect();
    let mut proofs: BTreeMap<&str, &PackGroupCoverage> = BTreeMap::new();
    for carrier in &view.manifest.packs {
        for proof in &carrier.group_coverages {
            if proof.refs_key != key
                || proof.covers_seq != snapshot.seq
                || proof.ref_policy != policy
            {
                continue;
            }
            if !proof.packs.contains(&carrier.checksum)
                || !carrier.pack_groups.contains(&proof.group)
            {
                return None;
            }
            if let Some(previous) = proofs.insert(&proof.group, proof)
                && previous != proof
            {
                return None;
            }
        }
    }
    let wants: HashSet<_> = request.wants.iter().map(ToString::to_string).collect();
    let mut covered_wants = HashSet::new();
    let mut groups = BTreeSet::new();
    for reference in &snapshot.refs {
        let Some(current) = view.refs.get(&reference.name) else {
            continue;
        };
        if !wants.contains(&current) {
            continue;
        }
        if let Some((name, _)) = cfg.refs.packfiles.iter().find(|(name, group)| {
            group.kind == walgit_config::PackGroupKind::Code
                && group.matches_ref(&reference.name, view.refs.head_target())
                && proofs.contains_key(name.as_str())
        }) {
            covered_wants.insert(current);
            groups.insert(name.as_str());
        }
    }
    if covered_wants != wants {
        return None;
    }
    let mut pending: Vec<_> = groups.iter().copied().collect();
    while let Some(name) = pending.pop() {
        let group = cfg.refs.packfiles.get(name)?;
        if group.kind != walgit_config::PackGroupKind::Code {
            return None;
        }
        for dependency in &group.subtract {
            if groups.insert(dependency.as_str()) {
                pending.push(dependency.as_str());
            }
        }
    }
    let mut selected = BTreeSet::new();
    for group in &groups {
        let proof = proofs.get(group)?;
        let members: BTreeSet<_> = proof.packs.iter().collect();
        if members.is_empty() || members.len() != proof.packs.len() {
            return None;
        }
        for id in &proof.packs {
            let pack = live.get(id.as_str())?;
            if pack.ref_policy != policy
                || pack.covers_seq != snapshot.seq
                || pack.coverage_refs_key != key
                || !pack.pack_groups.iter().any(|g| g == group)
                || pack.audience != PackAudience::Code as i32
            {
                return None;
            }
            let kind = PackKind::try_from(pack.kind).ok()?;
            if request.filter.as_deref() == Some("blob:none") {
                match kind {
                    PackKind::Blobs => continue,
                    PackKind::History => {}
                    PackKind::Objects => return None,
                }
            }
            selected.insert(id.as_str());
        }
    }
    if selected.is_empty()
        || selected.len() > cfg.packfile_uri.max_uris_per_fetch
        || !selected.iter().any(|id| {
            live.get(id)
                .is_some_and(|p| p.pack_size >= cfg.packfile_uri.uri_min_bytes.as_u64())
        })
    {
        return None;
    }
    // Only commits under branch refs become synthetic haves. Tags and other
    // object kinds can be delivered statically but do not broaden the cut.
    let mut roots = BTreeSet::new();
    for reference in &snapshot.refs {
        if reference.name.starts_with("refs/heads/")
            && groups.iter().any(|name| {
                cfg.refs
                    .packfiles
                    .get(*name)
                    .is_some_and(|g| g.matches_ref(&reference.name, &snapshot.head_target))
            })
        {
            roots.insert(gix_hash::ObjectId::from_hex(reference.oid.as_bytes()).ok()?);
        }
    }
    if roots.is_empty() || roots.iter().all(|oid| request.haves.contains(oid)) {
        return None;
    }
    let mut ordered: Vec<_> = selected.into_iter().collect();
    ordered.sort_by_key(|id| {
        (
            std::cmp::Reverse(live.get(id).map_or(0, |p| p.pack_size)),
            *id,
        )
    });
    let packs = ordered
        .into_iter()
        .map(|id| {
            Some(PackfileUri {
                checksum: gix_hash::ObjectId::from_hex(id.as_bytes()).ok()?,
                url: format!("{}/packfiles/{id}.pack", base_url.trim_end_matches('/')),
                index: None,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Selection {
        packs,
        roots: roots.into_iter().collect(),
    })
}

pub async fn get(
    state: &AppState,
    route: &RepoRoute,
    method: &Method,
    headers: &HeaderMap,
    peer: Option<std::net::SocketAddr>,
) -> Result<Response, ApiError> {
    state
        .auth
        .require_read(headers)
        .await
        .map_err(smart::auth_err)?;
    if !state
        .cfg
        .placement
        .serves(route.id.owner(), route.id.name())
    {
        return Err(ApiError::ServiceUnavailable(format!(
            "{} is not served by this host; retry shortly",
            route.id
        )));
    }
    let name = route
        .subpath
        .strip_prefix("packfiles/")
        .ok_or_else(|| ApiError::NotFound("packfile".into()))?;
    let (checksum, extension) = name
        .rsplit_once('.')
        .ok_or_else(|| ApiError::NotFound("packfile".into()))?;
    if !matches!(extension, "pack" | "idx")
        || !matches!(checksum.len(), 40 | 64)
        || !checksum
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ApiError::NotFound("packfile".into()));
    }
    let handle = smart::open_repo(state, &route.id, false).await?;
    if checksum.len() != handle.local().object_format().kind().len_in_hex()
        || !handle
            .serves_pack_fresh(checksum)
            .await
            .map_err(smart::wal_err)?
    {
        return Err(ApiError::NotFound(
            "packfile is not in the live or retired inventory".into(),
        ));
    }
    // Auth and manifest admission precede conditional responses, cache admission,
    // Range handling and optional loopback edge offload alike.
    static_object::serve(
        handle.store(),
        &format!("wal/{checksum}.{extension}"),
        method,
        headers,
        static_object::ServeOptions {
            cache_control: (state.cfg.server.auth.mode != walgit_config::AuthMode::None
                && !state.cfg.server.auth.anonymous_read)
                .then_some("private, max-age=31536000, immutable"),
            accel: state.cfg.server.accel_redirect,
            peer,
            ..Default::default()
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use walgit_config::{Config, PackGroupConfig, PackGroupKind};
    use walgit_proto::v1::{Manifest, PackRef, Ref};

    fn fixture() -> (FetchView, walgit_git::UploadPackRequest, RefSnapshot) {
        let mut cfg = Config::default();
        cfg.refs.packfiles.remove("code");
        for (name, reference, subtract) in [
            ("base", "refs/heads/main", vec![]),
            ("topic", "refs/heads/topic", vec!["base".into()]),
        ] {
            cfg.refs.packfiles.insert(
                name.into(),
                PackGroupConfig {
                    kind: PackGroupKind::Code,
                    include: vec![reference.into()],
                    subtract,
                },
            );
        }
        cfg.packfile_uri.uri_min_bytes = bytesize::ByteSize::b(100);
        let policy = cfg.refs.policy_identity();
        let snapshot = RefSnapshot {
            seq: 10,
            object_format: "sha1".into(),
            head_target: "refs/heads/main".into(),
            refs: vec![
                Ref {
                    name: "refs/heads/main".into(),
                    oid: format!("{:040x}", 1),
                    ..Default::default()
                },
                Ref {
                    name: "refs/heads/topic".into(),
                    oid: format!("{:040x}", 2),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut manifest = Manifest {
            head_seq: 11,
            ..Default::default()
        };
        for (group, start) in [("base", 10), ("topic", 20)] {
            let ids = [format!("{start:040x}"), format!("{:040x}", start + 1)];
            for (n, id) in ids.iter().enumerate() {
                manifest.packs.push(PackRef {
                    checksum: id.clone(),
                    pack_size: if start == 20 && n == 1 { 100 } else { 1 },
                    kind: if n == 0 {
                        PackKind::History
                    } else {
                        PackKind::Blobs
                    } as i32,
                    audience: PackAudience::Code as i32,
                    pack_groups: vec![group.into()],
                    covers_seq: 10,
                    coverage_refs_key: "captured-key".into(),
                    ref_policy: policy.clone(),
                    group_coverages: if n == 0 {
                        vec![PackGroupCoverage {
                            group: group.into(),
                            covers_seq: 10,
                            refs_key: "captured-key".into(),
                            ref_policy: policy.clone(),
                            packs: ids.to_vec(),
                        }]
                    } else {
                        vec![]
                    },
                    ..Default::default()
                });
            }
        }
        let mut refs = walgit_git::RefView::new(Arc::new(snapshot.clone().into()));
        let new_tip = gix_hash::ObjectId::from_hex(format!("{:040x}", 3).as_bytes()).unwrap();
        refs.set("refs/heads/topic", new_tip.to_string());
        (
            FetchView {
                manifest: Arc::new(manifest),
                refs,
                config: Arc::new(cfg),
            },
            walgit_git::UploadPackRequest {
                wants: vec![new_tip],
                done: true,
                ..Default::default()
            },
            snapshot,
        )
    }

    #[test]
    fn complete_dependencies_include_small_companions_and_reject_scope_mutations() {
        let (view, request, snapshot) = fixture();
        let chosen = select_snapshot(
            &view,
            &request,
            &snapshot,
            "captured-key",
            "https://example.invalid/o/r",
        )
        .unwrap();
        assert_eq!(chosen.packs.len(), 4);
        assert_eq!(chosen.roots.len(), 2);
        for mutation in 0..6 {
            let (mut view, request, snapshot) = fixture();
            let manifest = Arc::make_mut(&mut view.manifest);
            match mutation {
                0 => {
                    manifest.packs.remove(1);
                } // retired dependency member
                1 => manifest.packs[0].group_coverages.clear(), // missing dependency proof
                2 => manifest.packs[1].covers_seq = 9,          // mixed generation
                3 => manifest.packs[1].pack_groups = vec!["meta".into()],
                4 => manifest.packs[1].ref_policy = "old-policy".into(),
                5 => {
                    Arc::make_mut(&mut view.config)
                        .packfile_uri
                        .max_uris_per_fetch = 3;
                }
                _ => unreachable!(),
            }
            assert!(
                select_snapshot(
                    &view,
                    &request,
                    &snapshot,
                    "captured-key",
                    "https://example.invalid/o/r"
                )
                .is_none(),
                "mutation {mutation}"
            );
        }
    }

    #[test]
    fn blobless_delivery_uses_history_and_never_treats_objects_packs_as_history() {
        let (mut view, mut request, snapshot) = fixture();
        request.filter = Some("blob:none".into());
        Arc::make_mut(&mut view.config).packfile_uri.uri_min_bytes = bytesize::ByteSize::b(1);
        let selected = select_snapshot(
            &view,
            &request,
            &snapshot,
            "captured-key",
            "https://example.invalid/o/r",
        )
        .unwrap();
        assert_eq!(selected.packs.len(), 2);
        Arc::make_mut(&mut view.manifest).packs[0].kind = PackKind::Objects as i32;
        assert!(
            select_snapshot(
                &view,
                &request,
                &snapshot,
                "captured-key",
                "https://example.invalid/o/r"
            )
            .is_none()
        );
    }
}
