# repos.js — walgit in two script tags

Context: **user-facing docs for `repos.js`**, for people building on a walgit host from a browser page or
script. Wire details are `../API.md`; the decision is `AGENTS.md §4 D20`.

Every walgit host serves its SDK at `/repos.js` (and `/repos.mjs`); the examples below use
`https://git.example.com` for the host. The full wire contract is `../API.md`; the SDK maps all of it.

```html
<script src="https://git.example.com/repos.js"></script>
<script type="module">
  const repo = repos.repo("acme/monorepo");
  const { head } = await repo.refs();           // O(1): default branch only
  const tree = await repo.tree(head.sha, "areas");
  render(tree.entries);
</script>
```

No proxy or extra service: the browser talks to the walgit host, which authenticates every request itself; the
SDK picks the lane, handles the sign-in popup, and unwraps long answers.

## Lanes (picked automatically)

| You are… | Lane | What the SDK does |
|---|---|---|
| a page on another origin listed in `server.cors_origins` | browser `/{owner}/{repo}/api-browser/*` | `fetch` with `credentials: "include"`. On `401` it opens `<host>/api-browser/v1/authenticate?origin=<page-origin>`; walgit's sign-in (`/_auth/login`, your OIDC issuer) runs, the landing page posts `repos:authenticated` only to the validated opener origin, and the SDK retries once. |
| the bundled UI on git.example.com | same-origin `/{owner}/{repo}/api/*` | the session cookie; a lapsed session is sent to `/_auth/login` |
| a script, agent, CI job, Node | bearer `/{owner}/{repo}/api/*` | `createClient({ token })` with a walgit access token (`/_auth/tokens`), a static token, or an ID token → `Authorization: Bearer` |

The repository prefix comes first on every lane (D26/D27): `/{owner}/{repo}/api/*` (bearer, same-origin) and
`/{owner}/{repo}/api-browser/*` (browser: the cross-origin cookie lane) — so one edge rule routes a whole
repository (e.g. `acme/monorepo`) to its host; the application authenticates every request. `me`, `owners`, `authenticate` live under
`/api/v1/*` resp. `/api-browser/v1/*`. There are no other forms (prototyping phase: no aliases).

```js
import { createClient } from "https://git.example.com/repos.mjs";
// A token from https://git.example.com/_auth/tokens (or the server's static tokens), e.g. from the environment:
const repos = createClient({ base: "https://git.example.com", token: process.env.WALGIT_TOKEN });
```

## Surface

```ts
repos.me()                                   → { principal, write, anonymous }
repos.owners.list()                          → ["acme", …]
repos.owners.repos("acme")                   → ["monorepo", …]
repos.repo("acme/monorepo")                     → RepoClient (no request)

r.get()                                      → { owner, name, full_name, head, branches, tags, clone_url, html_url, api_url }
r.create() / r.delete()                      → write permission
r.refs()                                     → { head: {name, sha} | null }
r.branches({ prefix, q, after, n })          → { refs: [{name, sha}], more }      (one page; tags likewise)
r.tags({ … })
r.refStream("branches", q, onRef)            → streams matches as found; resolves { more }
r.resolve("feature/x/src/main.go")           → { ref, sha, path, kind }           (server splits ref/path, API.md §3)
r.tree(rev, path?)                           → { ref, sha, path, entries, commit?, readme? }
r.blob(rev, path)                            → { …, contents | binary | too_large }
r.raw(rev, path)                             → string
r.commits({ ref, path, skip, n })            → { ref, sha, commits, more }
r.commit(sha)                                → { commit, stats, patch }
r.overview()                                 → WAL overview (walgit-specific)
r.tasks()  /  r.task(id, onEvent?)           → what the answering instance is doing; attach to a task stream
r.ops.list()  /  r.ops.run(op, params, onEvent)
r.policy.get() / .put(doc) / .delete()       → push policy (docs/POLICY.md)
r.policy.validate(doc) / .dryRun(doc, last)  → validate or replay a policy against recent pushes
r.settings.get() / .put(toml, message) / .delete()
r.settings.effective() / .history() / .describe() / .validate(toml)
                                             → per-repository WAL settings and their effective host overlay
r.urls.{html, clone, api, raw(rev,path), tree(rev,path), blob(rev,path), commit(sha)}

repos.configure({ token, base, lane, onProgress, interactive })
repos.createClient(opts)   repos.ReposError   repos.version
```

Every call takes a trailing `{ signal, onProgress, headers }`. Errors are
`ReposError { status, message, url }` (`.notFound`, `.unauthorized`). Arrays
are never `null`.

### Addressing: resolve once, then by sha

Everything addressed by a **full sha** (`tree`, `blob`, `commits({ref: sha})`,
`commit`) is immutable and cached by the browser for a year; everything
addressed by a **name** is `stale-while-revalidate` + ETag. The cheap pattern
is one `resolve()` (or `refs()`) per navigation, then sha-addressed calls:

```js
const r = repos.repo("acme/monorepo");
const { sha, path } = await r.resolve("main/areas/core");
const tree = await r.tree(sha, path);       // immutable → free on revisit
```

### Long answers

A cold instance may need seconds to minutes to answer (pack indexes of a
huge repository, remote object reads). The server then streams an SSE
envelope (API.md §2b); the SDK consumes it and resolves the same JSON, while
`onProgress` receives `{kind: "notice"|"progress"|"task"|"done", …}` packets
you can show. Nothing to opt into.

## Dogfood rule

The bundled UI is built on this SDK (`web/src/api.ts` is
a ~100-line adapter). If the SDK cannot support a screen, the SDK is what
gets fixed — never a private client.

## Files

| | |
|---|---|
| `repos.ts` | the SDK (TypeScript, no dependencies) |
| `../dist/repos.js` | IIFE build: registers `window.repos`; served at `<host>/repos.js` |
| `../dist/repos.mjs` | ESM build: `import { createClient, ReposError } from …` |
| `../vite.sdk.config.ts` | the build (`pnpm run build:sdk`; part of `pnpm run build`) |

The SDK URL (`/repos.js`, `/repos.mjs`) is an API contract; no SDK-path compatibility shim is retained.
