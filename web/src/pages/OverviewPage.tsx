import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { api, repoApi, runOp, type OpEvent, type OpRecord, type Overview } from "../api";
import { invalidate, useData } from "../data";
import { Box } from "../components/Layout";
import { BundlePlan } from "../components/BundlePlan";
import { BundleChain } from "../components/BundleChain";
import { useRepo } from "./RepoLayout";

const fmtBytes = (n: number) => {
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i ? 1 : 0)} ${u[i]}`;
};
const fmtTime = (s?: string) => (s && !s.startsWith("1970") ? new Date(s).toLocaleString() : "—");
const short = (s: string) => s.slice(0, 12);

function KV({ rows }: { rows: [string, ReactNode][] }) {
  return (
    <table className="kv">
      <tbody>
        {rows.map(([k, v]) => (
          <tr key={k}>
            <th>{k}</th>
            <td>{v}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function OverviewPage() {
  const { full } = useRepo();
  // Short TTL: the WAL page is operational; an op that finished invalidates it
  // explicitly (`refresh`), otherwise it revalidates in the background.
  const o: Overview = useData(`overview:${full}`, () => api.overview(full), 2_000);
  const refresh = useCallback(() => invalidate(`overview:${full}`), [full]);
  const m = o.manifest;
  return (
    <div className="overview">
      <Box
        title={
          <>
            Health <span className={`pill health-${o.health.status}`}>{o.health.status}</span>
          </>
        }
      >
        <div className="pad">
          {o.health.issues.length === 0 ? (
            <div className="muted">Manifest invariants hold; local copy on {o.hostname} is reconciled with the WAL.</div>
          ) : (
            <ul className="issues">
              {o.health.issues.map((i) => (
                <li key={i}>{i}</li>
              ))}
            </ul>
          )}
          <div className="muted small">Connectivity audit (git fsck, recorded in the store by whichever maintainer ran it): {o.health.deep}.</div>
          {o.health.suggestions.length > 0 && (
            <div className="suggestions">
              <div className="small muted" style={{ marginTop: 8 }}>
                Missing maintenance — the loop does what it can by itself; the link runs it now on this instance:
              </div>
              <ul className="issues">
                {o.health.suggestions.map((s) => (
                  <li key={`${s.op}:${s.params ?? ""}`}>
                    {s.reason}
                    {s.auto ? (
                      <span className="muted small"> · automatic: {s.auto}</span>
                    ) : (
                      <span className="pill stale small" style={{ marginLeft: 6 }}>
                        needs a human
                      </span>
                    )}{" "}
                    <a href="#ops" onClick={() => window.dispatchEvent(new CustomEvent("walgit:op", { detail: s }))}>
                      run {s.op}
                      {s.params ? ` (${s.params})` : ""} now
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </Box>

      <OpsBox repo={full} overview={o} onChanged={refresh} />

      <div className="row gap">
        <Box title="WAL manifest" className="grow">
          <KV
            rows={[
              // oxlint-disable-next-line react/jsx-key -- label/value tuples, not a rendered list
              ["version", <code>{m.version}</code>],
              ["next_seq", m.next_seq],
              ["min_seq", m.min_seq],
              ["entries reachable", m.entries],
              ["sealed segments", m.segments.length],
              ["inline tail", m.tail_entries],
              ["last push", fmtTime(m.last_push)],
              // oxlint-disable-next-line react/jsx-key
              ["advertised bundle", m.advertised_bundle_uri ? <code className="wrap">{m.advertised_bundle_uri}</code> : "—"],
            ]}
          />
        </Box>
        <Box title={`Local copy (${o.hostname})`} className="grow">
          <KV
            rows={[
              [
                "instance",
                <span key="instance">
                  {o.instance.kind === "ssd" ? "The SSD host 🚀" : o.instance.kind === "serverless" ? "a serverless host" : o.instance.kind} · {o.instance.shape} ·{" "}
                  <code>{o.instance.revision || o.instance.name}</code>
                </span>,
              ],
              ["build", <code key="build">{o.instance.version}</code>],
              // oxlint-disable-next-line react/jsx-key
              ["version", <code>{o.local.version || "—"}</code>],
              ["next_seq", o.local.next_seq],
              ["bootstrap seq", o.local.bootstrap],
              ["reconciled", o.local.reconciled ? "yes" : "no"],
              ["size on disk", fmtBytes(o.local.size_bytes)],
            ]}
          />
        </Box>
      </div>

      <div className="row gap">
        <Box title="Packs" className="grow">
          <KV
            rows={[
              ["live packs", o.packs.live],
              ["live bytes", fmtBytes(o.packs.live_bytes)],
              ["push packs (incl. compacted)", o.packs.pushes],
              ["compactions", o.compactions.length],
            ]}
          />
        </Box>
        <Box title="Checkpoints" className="grow">
          <KV
            rows={[
              [
                "pack-set",
                m.packset
                  ? `at_seq ${m.packset.at_seq}, ${m.packset.packs} pack(s), ${fmtBytes(m.packset.bytes)}, ${fmtTime(m.packset.created)} by ${m.packset.creator}`
                  : "—",
              ],
              [
                "bundle",
                m.checkpoint ? (
                  <>
                    at_seq {m.checkpoint.at_seq}, {fmtBytes(m.checkpoint.size)}, <code>{short(m.checkpoint.sha)}</code>
                  </>
                ) : (
                  "—"
                ),
              ],
            ]}
          />
        </Box>
      </div>

      <Box title={`Bundle chain (${o.bundles.length} listed)`}>
        <BundleChain bundles={o.bundles} />
      </Box>

      <Box title="Bundle slots (what the maintainer will cut next)">
        <BundlePlan plan={o.bundle_plan} />
      </Box>

      <Box title={`Compactions (${o.compactions.length})`}>
        {o.compactions.length === 0 ? (
          <div className="muted pad">No compactions yet.</div>
        ) : (
          <table className="grid">
            <thead>
              <tr>
                <th>seq</th>
                <th>level</th>
                <th>folds</th>
                <th>pack</th>
                <th>superseded</th>
                <th>at</th>
                <th>primary</th>
              </tr>
            </thead>
            <tbody>
              {o.compactions.toReversed().map((c) => (
                <tr key={c.seq}>
                  <td>{c.seq}</td>
                  <td>{c.level}</td>
                  <td>
                    {c.first_seq}..{c.last_seq}
                  </td>
                  <td>{fmtBytes(c.pack_size)}</td>
                  <td>
                    {c.superseded_packs} packs, {fmtBytes(c.superseded_bytes)}
                  </td>
                  <td>{fmtTime(c.at)}</td>
                  <td>{c.primary}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Box>

      <Segments segments={m.segments} />
    </div>
  );
}

// ---- Maintenance ops ---------------------------------------------------------

const PARAM_HELP: Record<string, string> = {
  connectivity: "connectivity-only (fast)",
  force: "force (ignore thresholds)",
  base: "rebuild bitmap'd base",
};

/** Params that take a value, not a flag: rendered as a field, not a checkbox. */
const VALUE_PARAMS = new Set(["strategy", "at_seq"]);

function OpsBox({ repo, overview, onChanged }: { repo: string; overview: Overview; onChanged: () => void }) {
  const specs = overview.ops.available;
  const head = Math.max(0, overview.manifest.next_seq - 1);
  const [running, setRunning] = useState<string | null>(null);
  const [log, setLog] = useState<string[]>([]);
  const [status, setStatus] = useState<{ ok?: boolean; text: string } | null>(null);
  const [flags, setFlags] = useState<Record<string, boolean>>({});
  const [strategy, setStrategy] = useState<string>("");
  // `snapshot`'s at_seq: which WAL sequence to rewind to. Defaults to the head,
  // where the rewind is the current state.
  const [atSeq, setAtSeq] = useState<string>(String(head));
  const abort = useRef<AbortController | null>(null);
  const logRef = useRef<HTMLPreElement>(null);

  const logLen = log.length;
  useEffect(() => {
    if (logLen) logRef.current?.scrollTo({ top: logRef.current.scrollHeight });
  }, [logLen]);

  const run = useCallback(
    async (op: string, params: Record<string, string>) => {
      // One op at a time per page: `abort.current` is set while one runs.
      if (abort.current && !abort.current.signal.aborted) return;
      setRunning(op);
      setLog([]);
      setStatus({ text: `starting ${op}…` });
      abort.current = new AbortController();
      const t0 = performance.now();
      try {
        await runOp(
          repo,
          op,
          params,
          (ev: OpEvent) => {
            if (ev.event === "log") setLog((l) => [...l, ev.line]);
            else if (ev.event === "started") setStatus({ text: `${op} running on ${ev.record.hostname}…` });
            else if (ev.event === "done") setStatus({ ok: true, text: ev.record.summary });
            else if (ev.event === "error") setStatus({ ok: false, text: ev.message });
          },
          abort.current.signal,
        );
      } catch (e) {
        setStatus({ ok: false, text: (e as Error).message });
      } finally {
        setLog((l) => [...l, `— finished in ${((performance.now() - t0) / 1000).toFixed(1)}s`]);
        abort.current = null;
        setRunning(null);
        onChanged();
      }
    },
    [repo, onChanged], // oxlint-disable-line react/memo-dependencies -- refs are stable
  );

  // Health suggestions dispatch "walgit:op" with {op, params}.
  useEffect(() => {
    const h = (e: Event) => {
      const d = (e as CustomEvent<{ op: string; params?: string }>).detail;
      const params: Record<string, string> = {};
      if (d.params) for (const [k, v] of new URLSearchParams(d.params)) params[k] = v;
      void run(d.op, params);
    };
    window.addEventListener("walgit:op", h);
    return () => window.removeEventListener("walgit:op", h);
  }, [run]);

  const paramsFor = (op: string, spec: { params: string[] }) => {
    const p: Record<string, string> = {};
    for (const k of spec.params) if (!VALUE_PARAMS.has(k) && flags[`${op}.${k}`]) p[k] = "1";
    if (op === "bundle" && strategy) p.strategy = strategy;
    if (spec.params.includes("at_seq") && atSeq) p.at_seq = atSeq;
    return p;
  };

  return (
    <Box title="Maintenance" id="ops">
      <div className="pad">
        <p className="small muted">
          Every action runs on the instance that answers this request ({overview.hostname}) under the repo's GCS lease
          where exclusivity matters, and publishes its result to the WAL like any other writer. Output streams below.
        </p>
        <table className="kv ops-table">
          <tbody>
            {specs.map((s) => (
              <tr key={s.id}>
                <th>
                  <button className="btn small" disabled={!!running} onClick={() => void run(s.id, paramsFor(s.id, s))}>
                    {running === s.id ? "running…" : s.label}
                  </button>
                </th>
                <td>
                  <div>{s.description}</div>
                  <div className="op-params">
                    {s.params.flatMap((k) =>
                      VALUE_PARAMS.has(k) ? [] : (
                        <label key={k} className="small muted">
                          <input
                            type="checkbox"
                            checked={!!flags[`${s.id}.${k}`]}
                            onChange={(e) => setFlags({ ...flags, [`${s.id}.${k}`]: e.target.checked })}
                          />{" "}
                          {PARAM_HELP[k] ?? k}
                        </label>
                      ),
                    )}
                    {s.params.includes("at_seq") && (
                      <span className="small muted">
                        <label>
                          at_seq{" "}
                          <input type="number" min={1} max={head} step={1} value={atSeq} onChange={(e) => setAtSeq(e.target.value)} />
                        </label>{" "}
                        of {head} — the state the WAL was in at that sequence; live refs stay at the head.{" "}
                        {atSeq && (
                          <a href={`${repoApi(repo)}/snapshot/${encodeURIComponent(atSeq)}`} target="_blank" rel="noreferrer">
                            JSON
                          </a>
                        )}
                      </span>
                    )}
                    {s.params.includes("strategy") && (
                      <label className="small muted">
                        strategy{" "}
                        <select value={strategy} onChange={(e) => setStrategy(e.target.value)}>
                          <option value="">default (first full)</option>
                          <option value="due">due (per schedule)</option>
                          {overview.ops.bundle_strategies.map((b) => (
                            <option key={b} value={b}>
                              {b}
                            </option>
                          ))}
                        </select>
                      </label>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {(status || log.length > 0) && (
          <div className="op-output">
            {status && (
              <div className={`op-status ${status.ok === undefined ? "" : status.ok ? "health-ok" : "health-error"}`}>
                {status.text}
              </div>
            )}
            <pre className="code-block op-log" ref={logRef}>
              {log.join("\n")}
            </pre>
          </div>
        )}
      </div>
      {overview.ops.recent.length > 0 && <OpLog recent={overview.ops.recent} />}
    </Box>
  );
}


// ---- Op log ------------------------------------------------------------------

/** The same op with the same outcome shape (summary minus digits) in a row: one line, a count. */
function shape(r: OpRecord): string {
  return `${r.kind}|${r.ok === undefined ? "running" : r.ok ? "ok" : "failed"}|${r.summary.replace(/[0-9a-f]{7,}|\d+/g, "#")}`;
}

function OpLog({ recent }: { recent: OpRecord[] }) {
  const groups: { first: OpRecord; last: OpRecord; n: number; hosts: Set<string> }[] = [];
  for (const r of recent) {
    const g = groups.at(-1);
    if (g && shape(g.first) === shape(r)) {
      g.n += 1;
      g.last = r;
      g.hosts.add(r.hostname);
    } else {
      groups.push({ first: r, last: r, n: 1, hosts: new Set([r.hostname]) });
    }
  }
  return (
    <table className="grid">
      <thead>
        <tr>
          <th>op</th>
          <th>when</th>
          <th>took</th>
          <th>result</th>
          <th>instance</th>
        </tr>
      </thead>
      <tbody>
        {groups.map((g) => {
          const r = g.first;
          return (
            <tr key={r.id} className={g.n > 1 ? "muted" : ""}>
              <td>
                <code>{r.kind}</code>
                {g.n > 1 && <span className="pill small" title={`${g.n} consecutive runs with the same outcome`}> ×{g.n}</span>}
              </td>
              <td className="small">
                {g.n > 1 ? (
                  <>
                    {fmtTime(g.last.started)} → {fmtTime(r.started)}
                  </>
                ) : (
                  fmtTime(r.started)
                )}
              </td>
              <td>{r.ok === undefined ? "…" : `${(r.elapsed_ms / 1000).toFixed(1)}s`}</td>
              <td>
                <span className={`pill ${r.ok === undefined ? "" : r.ok ? "health-ok" : "health-error"}`}>
                  {r.ok === undefined ? "running" : r.ok ? "ok" : "failed"}
                </span>{" "}
                {g.n > 1 ? r.summary.replace(/\d{10}/, "…") : r.summary}
              </td>
              <td className="small">{[...g.hosts].map((h) => h.slice(0, 8)).join(", ")}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}


// ---- Segments ----------------------------------------------------------------

/** One object per publish batch: the list is the WAL's length. Show its shape, not every row. */
function Segments({ segments }: { segments: Overview["manifest"]["segments"] }) {
  const [all, setAll] = useState(false);
  if (segments.length === 0) {
    return (
      <Box title="Segments (0)">
        <div className="muted pad">No sealed segments; all entries inline in the manifest tail.</div>
      </Box>
    );
  }
  const bytes = segments.reduce((n, x) => n + x.size, 0);
  const first = segments[0]!;
  const last = segments.at(-1)!;
  const shown = all ? segments.toReversed() : segments.slice(-5).toReversed();
  return (
    <Box title={`Segments (${segments.length})`}>
      <div className="pad small muted">
        seq {first.first_seq}..{last.last_seq} in {segments.length} object{segments.length === 1 ? "" : "s"} · {fmtBytes(bytes)} · a cold refs sync reads
        the checkpoint plus the segments after it, so the checkpoint is what keeps this cheap.{" "}
        {segments.length > 5 && (
          <button className="btn link small" onClick={() => setAll(!all)}>
            {all ? "newest 5" : `all ${segments.length}`}
          </button>
        )}
      </div>
      <table className="grid">
        <thead>
          <tr>
            <th>key</th>
            <th>seqs</th>
            <th>size</th>
          </tr>
        </thead>
        <tbody>
          {shown.map((x) => (
            <tr key={x.key}>
              <td>
                <code>{x.key}</code>
              </td>
              <td>
                {x.first_seq}..{x.last_seq}
              </td>
              <td>{fmtBytes(x.size)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Box>
  );
}
