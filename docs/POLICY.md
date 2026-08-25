# Push policy

Context: **normative spec** of the per-repo push policy language (`repos/<o>/<r>/policy.json`, D16), for anyone
implementing, reviewing or writing a policy. Technical detail for one sub-module (`crates/walgit-server/src/policy.rs`);
the operating decision is `AGENTS.md §3 D16`. Read when touching receive-pack authorization or the policy API.

Normative. Implementation: `crates/walgit-server/src/policy.rs`.
On-store object: `repos/<owner>/<repo>/policy.json` (not on the WAL, not in
`walgit.toml`). `GET`/`PUT`/`DELETE /{owner}/{repo}/policy` and
`walgit repo policy get|set|clear`.

This file is a **small rule language whose combination law is fixed**, then
serialized. It is not a JSON bag that mixes ACL, protection, and size ceilings
under one list. Other hosts have paid for that lesson.

## What the file is for

A repo policy answers one question at push time:

> Given `(principal, tags, old, new, ref, quarantine objects)`, is this
> publish allowed, and if not, **which named rule** said so?

If a fact cannot change that answer, it does not belong here. Timeouts,
umbrella `off|audit|enforce`, last-good path, HTTP bind address — those are
operator levers. Put them in flags / `walgit.toml` so an incident can roll a
policy back without editing the policy.

Leave out anything receive-pack cannot enforce: required reviews, required CI,
“must go through a PR.” Those are merge-queue rules. Putting them in the push
file just lies.

Missing file / empty `rules` = allow-all (anyone with write may move any
ref). That is the only implicit default.

Host-configured `git.protected_ref_prefixes` are stricter than this document:
receive-pack rejects every mutation below those namespaces before evaluating
per-repository rules or bypasses. An admin can create an existing commit there
only through `PUT /{owner}/{repo}/api/protected-ref`.

## Envelope

```json
{
  "version": 1,
  "groups": [
    { "name": "bypass-admin", "members": ["alice@example.com", "@okta:sre"] },
    { "name": "bots", "members": ["svc:ci", "svc:merge-queue"] }
  ],
  "rules": [
    {
      "name": "lock-main",
      "_comment": "only the queue and break-glass may move main",
      "match": { "refs": ["refs/heads/main"] },
      "effect": {
        "protect": {
          "restricts": ["create", "update", "delete"],
          "bypass": ["group:bypass-admin", "svc:merge-queue"]
        }
      }
    }
  ]
}
```

Two collections, not one:

| Key | What it is | Combination |
|---|---|---|
| `groups` | A roster. Names, not decisions. | Exact name lookup at eval time. |
| `rules` | Ordered named decisions. | Derived from the effect type. Never declared in the file. |

`version` is an integer. Readers accept the versions they know and refuse the
rest. Do not invent `version: "1.2-beta"`. Current: `1`.

`_comment` is legal everywhere and never read.

Unknown key **inside a rule** (or inside `match` / `effect`): parse error. A
typo `bypass_actrs` must not become an empty bypass list.

Unknown key **beside** `groups` / `rules` / `version`: ignore. During a binary
roll the fleet runs two parsers against one object. Rejecting the envelope for
a future knob reverts every host that is behind.

## One rule shape

Every rule is the same keys. Nothing else at the rule level.

```json
{
  "name": "release-tags",
  "_comment": "optional",
  "match": { "refs": ["refs/tags/v*"] },
  "effect": { "protect": { "restricts": ["update", "delete"] } },
  "mode": "enforce"
}
```

- `name` — `^[a-z][a-z0-9-]{0,62}$`, unique in the file. Metric label and the
  word in `remote: rejected by rule 'release-tags'`.
- `match` — who / what the rule applies to.
- `effect` — a tagged object with **exactly one** key. That tag is the rule
  type.
- `mode` — optional. Only narrows a process umbrella (`enforce` → `audit` is
  allowed; a file must not promote `audit` to `enforce`). No umbrella is
  configured today, so `mode` is stored and ignored.

## Match

```json
"match": {
  "refs": ["refs/heads/**", "^refs/heads/tmp/**"],
  "principals": ["@okta:platform", "group:bots", "^intern@example.com"],
  "paths": ["vendor/**"]
}
```

Three laws:

1. **Keys AND, values OR.** `refs` and `principals` both set means “this ref
   **and** this actor.” Two entries in `refs` means either.
2. **Absent key matches everything.** Empty `match` is the catch-all.
   - `protect` / `history`: spell a whole-repo rule `["refs/**"]` when
     widening is the dangerous direction.
   - `size`: spell paths explicitly; absent `paths` matching everything would
     raise the ceiling for the whole repo.
3. **One glob dialect.** Doublestar. `*` and `?` stop at `/`. `**` crosses.
   `^` is an exclusion, not a second field. Same grammar for refs, paths, and
   principal names.

Actors are three spellings, nothing else:

| Spelling | Means |
|---|---|
| `alice@example.com` | Exact principal (case-insensitive). |
| `@okta:platform` | Tag the edge already bound. |
| `group:bots` | Roster in this file. |

No implicit admin. A write bit on the request is not a bypass unless a rule
lists that principal (or a group / tag that contains them).

`group:` is resolved at **eval**, not parse. Edit the roster, next push sees
it. A missing or unreadable roster is indeterminate, and indeterminate is not
“no”: an unresolvable **include** does not admit; an unresolvable **exclude**
still excludes.

Do not allow `^` exclusions on a rule family whose combination is **union**.
Under union, a carve-out in a narrow rule is defeated by any broader admitting
rule, so the `^` is a no-op that looks like a revoke. Refuse it at load.
`protect` is most-restrictive (AND) and may use `^`.

`paths` is reserved for `size`. On a `protect` / `history` rule it is ignored
until a quarantine path walk exists.

## Effect is a tagged union

The file does not say how rules combine. The effect type does. A file that can
change its own combining rule is a programming language.

```json
"effect": { "protect":  { "restricts": ["create", "update", "delete", "force-push"], "bypass": ["group:bots"] } }
"effect": { "history": { "allowed_forwards": 50000, "allow_unrelated": false } }
"effect": { "size":    { "blob_bytes": 10485760, "push_bytes": 104857600 } }
```

| Effect | Default if omitted | Combine | Status |
|---|---|---|---|
| `protect` | restrict all four ops; empty bypass | Every matching rule applies. Bypass a rule only if **that** rule’s bypass matches. Overlap is AND. | **Enforced.** |
| `history` | compiled floors | Per field, first rule that sets it wins. | Specified. Parsed. **Not enforced.** |
| `size` | compiled ceilings | First match. Exists to **raise** a ceiling. Most-restrictive-wins would delete the feature. | Specified. Parsed. **Not enforced.** |

`restricts` is a closed enum: `create` \| `update` \| `delete` \| `force-push`.
Whitespace is not trimmed. `null` and `[]` are parse errors, not “restrict
nothing.”

`force-push` is not an OID shape. Fast-forward and force have the same wire
triple. The server runs `merge-base --is-ancestor` after ingest. Tags are
never fast-forward in any useful sense: a tag retarget is `force-push`.

Do not put secrets, signatures, or “required PR” in this file.

## A repo people would actually ship

```json
{
  "version": 1,
  "groups": [
    { "name": "admins", "members": ["@okta:sre"] },
    { "name": "queue",  "members": ["svc:merge-queue"] }
  ],
  "rules": [
    {
      "name": "lock-main",
      "match": { "refs": ["refs/heads/main"] },
      "effect": {
        "protect": {
          "restricts": ["create", "delete", "update"],
          "bypass": ["group:admins", "group:queue"]
        }
      }
    },
    {
      "name": "tags-immutable",
      "match": { "refs": ["refs/tags/**", "^refs/tags/tmp/**"] },
      "effect": {
        "protect": {
          "restricts": ["update", "delete"],
          "bypass": ["group:admins"]
        }
      }
    },
    {
      "name": "reserve-queue-ns",
      "match": { "refs": ["refs/heads/mq/**"] },
      "effect": { "protect": { "bypass": ["group:queue", "group:admins"] } }
    }
  ]
}
```

That is enough for a real host: lock the trunk, reserve bot namespaces, keep
tags still. History / size rules can be added later without changing the
envelope; they will not change a verdict until this document says they do.

## Load rules

- Fail closed on a half-wired file. A document that does not parse is a
  `400` on `PUT` and a reject on the next push (not “skip policy”).
- Last-good backup is **not implemented**. A corrupt object currently fails
  the push. Do not pretend otherwise.
- Pin the shipped file in tests. Load real JSON, drive
  `(principal, ref, op)` through the real matcher, assert allow / deny.
- Explain the verdict. The `ng` line is `rejected by rule '<name>'`.
- Overlapping-bypass lockout: if two `protect` rules can match the same ref
  and the same op, and both have non-empty bypass lists whose intersection
  is empty, load fails. AND would make the intended bot unable to land.

## What does not go in the JSON

- Umbrella mode. Incident lever (`walgit.toml`).
- `admin_bypass: true`. Implicit privilege.
- `combine: "first-match"` on the document. The effect type already decided.
- Required reviews / status checks. Wrong layer.
- Per-rule timeouts.
- Principal email lists copied from Okta. That is what `group:` + `@tags`
  are for.

The file should be boring: named rules, one match grammar, a closed set of
effects, combination you can recite without opening the parser.
