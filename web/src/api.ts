/**
 * The SPA's data layer = the public SDK (`web/sdk/repos.ts`, served as
 * `/repos.js`) bound to this origin (same-origin lane, session cookie). The
 * dogfood rule (D20): the UI never re-implements the API client; when the SDK
 * cannot support a screen, the SDK is what gets fixed. This file only adapts
 * it to the UI's loading/activity plumbing and keeps the old names.
 */
import { createClient, ReposError, type Progress } from "../sdk/repos";
import { setActivity, track } from "./data";

export type {
  RefInfo,
  Refs,
  RefPage,
  Resolved,
  Commit,
  CommitTrailer,
  TreeEntry,
  Tree,
  Blob,
  FileStat,
  CommitDetail,
  TaskProgress,
  TaskRecord,
  Tasks as TasksResponse,
  OpSpec,
  OpRecord,
  OpEvent,
  RepoSummary,
  Me,
  OwnerProfile,
  RepoDetail,
  RepoDescription,
} from "../sdk/repos";
import type { RefInfo, OpEvent, OpSpec, OpRecord, Tasks } from "../sdk/repos";
export type { SettingsDescribe, SettingsValidation, SettingsHistory, SettingsField, Policy, PolicyValidation, PolicyDryRun, RepoSettings } from "../sdk/repos";

/** Kept for callers: the SDK's error class under the UI's historical name. */
export const ApiError = ReposError;
export type ApiError = ReposError;

let redirecting = false;
if (window.location.search.includes("walgit_retry=1")) {
  const u = new URL(window.location.href);
  u.searchParams.delete("walgit_retry");
  window.history.replaceState(null, "", u.pathname + (u.search || "") + u.hash);
}

/** Envelope packets → the top-bar activity line (API.md §2b). */
function onProgress(p: Progress) {
  switch (p.kind) {
    case "notice":
      setActivity(p.url, { text: p.text });
      break;
    case "progress":
      setActivity(p.url, { text: p.label, done: p.done, total: p.total, percent: p.percent });
      break;
    case "task":
      if (p.task?.summary) setActivity(p.url, { text: p.task.summary });
      break;
    case "done":
      setActivity(p.url, null);
      break;
  }
}

/** The SDK client for this origin. Same-origin lane: `/api/v1`; identity is the session cookie. */
export const client = createClient({ base: window.location.origin, lane: "same-origin", onProgress, interactive: false });

/** 401 = the session lapsed (fetches are not redirected): reload so the sign-in redirect runs again. */
async function authRedirect<T>(p: Promise<T>): Promise<T> {
  try {
    return await track(p);
  } catch (e) {
    if (e instanceof ReposError && e.status === 401 && !redirecting) {
      redirecting = true;
      window.location.reload();
      await new Promise(() => {}); // never resolves; the page is reloading
    }
    throw e;
  }
}

/** Base of one repository's JSON API. */
/** D26: a repository's API lives under its own prefix. */
export const repoApi = (repo: string) => `/${enc(repo)}/api`;

const enc = (s: string) => s.split("/").map(encodeURIComponent).join("/");

export const api = {
  /** Owners with their profiles (display name, description) — the home page. */
  owners: () => authRedirect(client.owners.listDetail()),
  ownerProfile: (owner: string) => authRedirect(client.owners.profile.get(owner)),
  /** One owner's repositories with their descriptions. */
  repos: (owner: string) => authRedirect(client.owners.reposDetail(owner)),
  description: (repo: string) => authRedirect(client.repo(repo).description.get()),
  refs: (repo: string) => authRedirect(client.repo(repo).refs()),
  refList: (repo: string, kind: "branches" | "tags", q: { q?: string; prefix?: string; after?: string; n?: number } = {}) =>
    authRedirect(kind === "branches" ? client.repo(repo).branches(q) : client.repo(repo).tags(q)),
  resolve: (repo: string, rest: string) => authRedirect(client.repo(repo).resolve(rest)),
  tree: (repo: string, sha: string, path: string) => authRedirect(client.repo(repo).tree(sha, path)),
  blob: (repo: string, sha: string, path: string) => authRedirect(client.repo(repo).blob(sha, path)),
  commits: (repo: string, sha: string, path: string, skip: number) => authRedirect(client.repo(repo).commits({ ref: sha, path, skip })),
  commit: (repo: string, sha: string) => authRedirect(client.repo(repo).commit(sha)),
  overview: (repo: string) => authRedirect(client.repo(repo).overview() as unknown as Promise<Overview>),
  /** What is happening to this repo on the instance that answers (API.md §2c). Never cached. */
  tasks: (repo: string): Promise<Tasks> => client.repo(repo).tasks(),
  /** D24 settings + policy (Settings tab). Writes are never cached. */
  settings: (repo: string) => client.repo(repo).settings,
  policy: (repo: string) => client.repo(repo).policy,
  /** Clone/setup recipes rendered by the server (`setup::Recipes`) — one source of truth. */
  setupRecipes: async (repo?: string): Promise<SetupRecipes> => {
    const u = repo ? `/services/setup.json?repo=${enc(repo)}` : "/services/setup.json";
    const r = await track(fetch(u, { headers: { Accept: "application/json" } }));
    if (!r.ok) throw new Error(`setup.json: HTTP ${r.status}`);
    return r.json();
  },
  /** The installer script text (`/services/public/*` — open, no credential). */
  installScript: async (repo?: string): Promise<string> => {
    const u = repo ? `/services/public/install.sh?repo=${enc(repo)}` : "/services/public/install.sh";
    const r = await track(fetch(u, { headers: { Accept: "text/x-shellscript, text/plain" } }));
    if (!r.ok) throw new Error(`install.sh: HTTP ${r.status}`);
    return r.text();
  },
};

/** Mirror of crates/walgit-server/src/setup.rs `Recipes`. */
export interface SetupRecipes {
  base_url: string;
  host: string;
  /** Where a signed-in browser mints an access token (null when tokens come from the server's config / no auth). */
  token_url: string | null;
  install: string;
  install_url: string;
  manual_clone: string;
  plain_clone: string;
  /** Blobless + sparse: full history with blobs fetched on demand. */
  blobless_clone: string;
  setup_text: string;
  /** Self-signed TLS: the CA to pin and the one-liner that does it (null behind a public certificate). */
  ca_url: string | null;
  trust: string | null;
}

export interface Overview {
  repo: string;
  instance: { kind: string; name: string; revision: string; instance: string; version: string; roles: string[]; disk: string; shape: string; cpus: number; memory_bytes: number };
  clone_url: string;
  setup: string;
  hostname: string;
  health: {
    status: "ok" | "degraded" | "error";
    issues: string[];
    deep: string;
    suggestions: { op: string; params?: string; reason: string; auto?: string }[];
  };
  ops: { available: OpSpec[]; recent: OpRecord[] };
  /** `sh -c "$(curl -fsSL …/services/public/install.sh)"` (open route; stdin stays the terminal for the token prompt). */
  install: string;
  /** Absolute URL of the installer. */
  install_url: string;
  clone: { manual: string; plain: string };
  manifest: {
    version: string;
    next_seq: number;
    min_seq: number;
    segments: { key: string; first_seq: number; last_seq: number; size: number }[];
    tail_entries: number;
    entries: number;
    packset?: { at_seq: number; packs: number; bytes: number; created: string; creator: string };
    last_push?: string;
  };
  local: { version: string; next_seq: number; bootstrap: number; reconciled: boolean; size_bytes: number };
  packs: { live: number; live_bytes: number; pushes: number };
  maintenance: {
    maintainers: { host: string; disk: string; max_pack_bytes: number; last_pass_age_secs: number | null; alive: boolean; passes: number; last_unit: string }[];
    orphaned: boolean;
  };
  compactions: {
    seq: number;
    level: number;
    first_seq: number;
    last_seq: number;
    pack_size: number;
    superseded_packs: number;
    superseded_bytes: number;
    at: string;
    primary: string;
  }[];
  node: Record<string, number>;
}

// ---- maintenance ops (WAL tab) ----------------------------------------------

/** POST …/ops/{op}?params and stream SSE events until done/error. */
export function runOp(
  repo: string,
  op: string,
  params: Record<string, string>,
  onEvent: (ev: OpEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  return client.repo(repo).ops.run(op, params, onEvent, { signal });
}

/**
 * Streaming form of `refList` (API.md §refs: `Accept: text/event-stream` →
 * `event: ref` per match, `event: done` with `{more}`): the picker paints
 * matches as the server finds them instead of waiting for the whole page.
 */
export function refListStream(
  repo: string,
  kind: "branches" | "tags",
  q: { q?: string; prefix?: string; after?: string; n?: number },
  onRef: (r: RefInfo) => void,
  signal?: AbortSignal,
): Promise<{ more: boolean }> {
  return client.repo(repo).refStream(kind, q, onRef, { signal });
}
