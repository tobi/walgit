# Events

Context: **the normative WAL-to-webhook event contract**, for anyone changing ref-event shapes, the events
bridge, delivery, or consumer semantics (D32). The golden tests in `crates/walgit-server/src/{events,bridge}.rs`
and `tests/events.rs` are its executable form (same discipline as `docs/POLICY.md`).

## Principle

**Events are produced from the WAL by one small service — the bridge (`Role::Events`) — never by the push
path.** It tails each repo's log from a durable per-repo cursor, converts committed entries, POSTs them to your
webhook, and advances the cursor after each accepted bounded group. So an event is delivered iff its entry is
durable, a crash can't lose one, the lag is `head_seq − cursor`, and no writer — any serving host, a push broker,
the CLI, an import — performs event delivery. Receive-pack only rejects a ref transaction that cannot fit one
configured delivery because a WAL entry is indivisible.

Invariants:

1. Webhook availability **never gates a push**: the bridge is another process reading the bucket. A down webhook
   adds zero milliseconds to receive-pack; it adds lag, which is a metric. Static delivery admission can reject a
   ref transaction that exceeds `max_batch_events` or `max_batch_bytes`.
2. No no-op events. `old == new` (and `0→0`) emits nothing.
3. No lost events: each cursor increment happens only after the webhook answered 2xx. Duplicates are possible
   (at-least-once) and carry a deterministic dedup key. A crash after a 2xx but before the cursor CAS retries that
   whole group. A later failure does not replay earlier accepted groups because their cursor increments are
   already durable. What *can* happen is a **gap when the bridge lags behind log retention** (entries folded into
   a checkpoint before they were read): counted (`events_bridge_gap_total`) and warned, never silently repaired —
   consumers backfill from the WAL.
4. One producer, one instance.

Only `ref` events exist. Not events: push denials and auth failures (metrics + logs), LFS, compaction/checkpoints
(already WAL entries), repo/policy admin (no consumer; the HTTP request log has the principal).

## `ref` event

```json
{
  "action": "update",
  "ref_type": "branch",
  "ref_name": "refs/heads/main",
  "old": "48a0637…",
  "new": "cb38da1…",
  "pusher": "alice@example.com",
  "correlation_id": "d1f916f7-…",
  "repo": "acme/monorepo",
  "_walgit": { "schema_version": 1, "seq": "42", "entry_kind": "push", "request_id": "d1f916f7-…" }
}
```

- `action` — `create` / `update` / `delete`. Force is not a wire action (consumers derive it).
- `old` / `new` — **always the full zero OID on create/delete, never `""`** (40 chars sha1, 64 sha256).
- `ref_type` — `branch` (`refs/heads/`), `tag` (`refs/tags/`), `""` otherwise.
- `pusher` — the authenticated principal in the log entry's `meta` (`X-Walgit-Principal` on forwarded pushes).
- `correlation_id` / `_walgit.request_id` — the user-visible request id: the middleware honours an incoming
  `x-request-id` (else mints one), a front forwards it when it forwards receive-pack, `push_meta` stores it.
- `_walgit.seq` — the entry's WAL seq, a JSON **string** (uint64 convention). `entry_kind` — `push` |
  `ref_update`; consumers must not care.
- One event per ref update in the transaction; symbolic (HEAD) retargets and COMPACT / CHECKPOINT / SETTINGS
  entries emit nothing.

**Dedup key (normative): `(repo, _walgit.seq, ref_name)`.** **Order: by `seq` per repo.** Nothing else is promised.

## Delivery: the webhook

Each catch-up `POST`s one or more bounded JSON **arrays** of events from `(cursor, head_seq]` to
`events.webhook_url`. A delivery contains only whole WAL entries and is limited by `max_batch_entries`,
`max_batch_events`, and `max_batch_bytes`. The bridge never allocates or sends the full cursor-to-head range.
Each request has:

```
Content-Type:        application/json
X-Walgit-Delivery:   <sha1 hex of the body>                 # the batch's id; safe to dedup on
X-Walgit-Signature:  sha256=<hex HMAC-SHA256(body, events.webhook_secret)>   # when a secret is configured
```

Answer 2xx to acknowledge. The bridge then CAS-advances the durable cursor to the last sequence represented by
that group before it reads the next group. Anything else (or a timeout, 10 s) leaves the cursor at the last
accepted group and retries only the undelivered range on the next wake-up. A consumer therefore sees
at-least-once delivery of whole-entry groups. Verify the signature with a constant-time compare before parsing.

**Backfill contract (normative for consumers):** on any gap, read the WAL log from your last known seq
(`walgit wal ls`) and treat each PUSH / REF_UPDATE entry's ref transaction as the missed events. The webhook is
a latency optimization over polling the log; correctness never depends on it.

## The bridge

```
writers (any host, broker, CLI, import) ── manifest.pb CAS ──► bucket ──► notification ──► POST /_events/notify
   (no event delivery)                                                                           │
                                                                                                 ▼
                 the events host (roles=["events"], one instance):            catch_up(repo): cursor → manifest
                 + sweep every events.sweep_interval (backstop + health check)  → bounded whole-entry group
                                                                               → webhook → CAS cursor → repeat
```

`catch_up(repo)` = read `repos/<o>/<r>/events/cursor.json` → fresh manifest → repeatedly read a bounded sequence
range after the cursor → group whole entries within the configured event and byte bounds → webhook → CAS cursor
to that group's last seq. The loop stops at the manifest head observed at the start. A webhook error leaves the
last accepted cursor; the next wake-up resumes there. A cold cursor starts at the oldest readable seq
(`min_seq − 1`: everything still in the manifest's log window is published once; pre-seed the cursor to skip
history). Cursor CAS writes are monotonic; a conflict fails the catch-up instead of overwriting another bridge's
progress. Every published event is also one structured log line (`event_type="ref"`). Metrics:
`events_published_total{sink}`, `events_bridge_lag_entries{repo}`, `events_bridge_gap_total{repo}`,
`events_bridge_sweep_found_total`; alert on lag growth, any gap, any sweep-found.

Wake-ups (both idempotent; they only ever call `catch_up`):
- `POST /_events/notify` with a **bucket notification** naming a finalized `…/manifest.pb` — the commit point
  itself as the notification. Accepted bodies: a GCS Pub/Sub push envelope (`message.attributes.eventType =
  OBJECT_FINALIZE`, `objectId`), an S3 event notification (`Records[].eventName = ObjectCreated:*`,
  `s3.object.key`; MinIO, rustfs and Ceph emit the same shape), or your own glue's `{"key": "repos/o/r/manifest.pb"}`
  / `{"repo": "o/r"}`. Everything else is acked and ignored; a webhook failure answers 503 so the notifier
  redelivers. Authenticated like every route (`require_read`): give the notifier a token.
- The sweep (`events.sweep_interval`, default 5 min): `list` + one conditional manifest GET per repo. Not needed
  for correctness; it is the backstop *and the health check* — a sweep that publishes anything means
  notifications are not flowing (`events_bridge_sweep_found_total`, warn). With no notifier at all, set the
  sweep to the latency you can live with.

```toml
[server]
roles = ["events"]            # or leave roles empty on a one-box install: every role, bridge included
[events]
webhook_url = "https://hooks.example.com/walgit"
webhook_secret = "…"          # env: WALGIT__EVENTS__WEBHOOK_SECRET
sweep_interval = "5m"
max_batch_entries = 128       # max WAL sequence span considered for one delivery
max_batch_events = 1000       # max events; one larger ref transaction is rejected
max_batch_bytes = "1 MiB"     # max JSON body; one larger ref transaction is rejected
```

## Consumer checklist

1. Verify `X-Walgit-Signature` (if you set a secret), then parse the array.
2. Dedup on `(repo, _walgit.seq, ref_name)` (or on `X-Walgit-Delivery` per batch).
3. Order by `_walgit.seq` within a repo; do not assume order across repos.
4. On a gap alert from the bridge, backfill from `walgit wal ls <repo> --from <seq>`.
