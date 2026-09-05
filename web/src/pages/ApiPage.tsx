import { useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { Box } from "../components/Layout";
import { CodeSample } from "../components/CopyButton";
import { useRecipes } from "../components/CloneSetup";

/**
 * The API page: one API at `/api/v1`, two lanes (bearer / browser), one
 * SDK (`/repos.js`). This page advertises it and documents the surface for
 * humans; `/api/v1` is the machine-readable discovery document and
 * web/API.md the full contract. `?repo=owner/name` pre-fills the examples
 * (the title-bar tab links here with it from any repo page).
 */
export function ApiPage() {
  const [sp] = useSearchParams();
  const [repo, setRepo] = useState(sp.get("repo") || "acme/monorepo");
  const origin = window.location.origin;
  const r = repo.trim() || "owner/repo";
  const base = `${origin}/${r}/api`;
  const curl = `curl -fsS -H "Authorization: Bearer $token" -H "Accept: application/json"`;
  const recipes = useRecipes();

  return (
    <div className="api-page">
      <h1 className="page-title">API</h1>
      <p className="lead">
        Everything on this host is one read-mostly JSON API at <code>/api/v1</code> — the same one this UI runs on —
        plus a dependency-free SDK, <code>repos.js</code>, that a web page, an agent or a script can load from a
        single URL. One request per question, answered from the WAL in milliseconds; no clone, no checkout, no packs.
        Sha-addressed answers are immutable and cached forever; name-addressed answers are as fresh as a{" "}
        <code>git fetch</code>.
      </p>

      <div className="row gap">
        <Box title="In a browser — two script tags" className="grow">
          <div className="pad">
            <CodeSample
              code={`<script src="${origin}/repos.js"></script>
<script type="module">
  const r = repos.repo("${r}");
  const { head } = await r.refs();    // O(1): default branch only
  const tree = await r.tree(head.sha, "");  // by sha: immutable, cached
  render(tree.entries);
</script>`}
            />
            <div className="small muted">
              The SDK picks the lane (bearer token, same-origin cookie, or cross-origin cookie with a sign-in popup on 401) —
              same paths on every lane — and unwraps long answers. ESM: <code>import {"{ createClient }"} from "{origin}/repos.mjs"</code>.
            </div>
          </div>
        </Box>
        <Box title="From a shell / CI / agent — a bearer token" className="grow">
          <div className="pad">
            <label className="small strong api-repo-input">
              Repository{" "}
              <input value={repo} onChange={(e) => setRepo(e.target.value)} placeholder="owner/repo" spellCheck={false} />
            </label>
            <div className="small muted">
              Once per machine (token + git setup, idempotent): <code>{recipes.install}</code>
              {recipes.token_url && (
                <>
                  {" "}
                  Tokens: <a href={recipes.token_url}>{recipes.token_url.replace(/^https?:\/\//, "")}</a>.
                </>
              )}
            </div>
            <CodeSample code={`token="$WALGIT_TOKEN"\n${curl} ${base}`} />
            <CodeSample code={`${curl} "${base}/tree/main/"`} />
            <CodeSample code={`${curl} "${base}/commits?ref=main&n=10" | jq -r '.commits[] | .sha[:8] + " " + .subject'`} />
            <div className="small muted">
              Same token git uses; nothing is anonymous. Signed in here? Then just{" "}
              <a href={`/${r}/api`} target="_blank" rel="noreferrer">
                open <code>/{r}/api</code>
              </a>{" "}
              — or <a href="/api/v1" target="_blank" rel="noreferrer"><code>/api/v1</code></a> for the discovery document.
            </div>
          </div>
        </Box>
      </div>

      <Box title="Endpoints (GET unless noted)">
        <table className="api-table">
          <thead>
            <tr>
              <th>Path</th>
              <th>Returns</th>
              <th>Cache</th>
            </tr>
          </thead>
          <tbody>
            <Row path="/api/v1" desc={<>Discovery: <code>{`{base, browser_base, sdk, auth, endpoints}`}</code>.</>} cache="—" />
            <Row path="/api/v1/me" desc={<><code>{`{principal, write, anonymous}`}</code> — who you are on this host.</>} cache="no-store" />
            <Row path="/api/v1/owners" desc="Owners (namespaces), sorted." cache="SWR" />
            <Row path="/api/v1/owners/{owner}/repos" desc="Repository names under one owner." cache="SWR" />
            <Row
              path={`/${r}/api`}
              desc={<>Repo summary: <code>{`{owner,name,full_name,head,branches,tags,clone_url,html_url,api_url}`}</code> (O(1) ref counts). <code>PUT</code> creates (write), <code>DELETE</code> removes (admin).</>}
              cache="SWR + ETag"
            />
            <Row path={`/${r}/api/refs`} desc={<>Default branch only: <code>{`{head:{name,sha}|null}`}</code>. O(1) whatever the ref count.</>} cache="SWR + ETag" />
            <Row
              path={`/${r}/api/refs/{branches|tags}?prefix=&q=&after=&n=`}
              desc={<>One name-sorted page <code>{`{refs:[{name,sha}],more}`}</code>; tags peeled; <code>n</code> ≤ 1000. With <code>Accept: text/event-stream</code>: one <code>ref</code> event per match as found.</>}
              cache="SWR"
            />
            <Row
              path={`/${r}/api/resolve/{ref/path…}`}
              desc={<>Splits a GitHub-shaped <code>ref/path</code> into <code>{`{ref,sha,path,kind}`}</code>; longest existing branch/tag wins, then a revision. Do this once, then address by sha.</>}
              cache="SWR + ETag"
            />
            <Row
              path={`/${r}/api/tree/{rev}/{path}`}
              desc={<>Directory listing <code>{`{entries:[{name,type,mode,size,sha}],commit?,readme?}`}</code>, dirs first, with the latest commit touching the path and README contents.</>}
              cache="sha → immutable · name → SWR + ETag"
            />
            <Row
              path={`/${r}/api/blob/{rev}/{path}[?raw]`}
              desc={<><code>{`{name,size,contents}`}</code> or <code>binary:true</code> / <code>too_large:true</code>; <code>?raw</code> returns the bytes as <code>text/plain</code>.</>}
              cache="sha → immutable · name → SWR + ETag"
            />
            <Row
              path={`/${r}/api/commits?ref=&path=&skip=&n=`}
              desc={<>History page <code>{`{commits:[Commit],more}`}</code>, optionally for one path; <code>n</code> ≤ 200; paginate with <code>skip += commits.length</code>.</>}
              cache="sha → immutable · name → SWR + ETag"
            />
            <Row
              path={`/${r}/api/commit/{sha}`}
              desc={<><code>{`{commit,stats:[{path,additions,deletions}],patch}`}</code> — unified diff against the first parent; any revision accepted.</>}
              cache="full sha → immutable · else SWR + ETag"
            />
            <Row path={`/${r}/api/policy`} desc={<>Push policy document (<code>GET</code> read; <code>PUT</code>/<code>DELETE</code> admin).</>} cache="no-store" />
            <Row
              path={`/${r}/api/settings`}
              desc={<>Per-repository settings in the WAL (<code>GET</code> read; <code>PUT</code>/<code>DELETE</code> admin); also <code>/effective</code>, <code>/history</code>, <code>/describe</code> and <code>POST /validate</code>.</>}
              cache="no-store"
            />
            <Row path={`/${r}/api/overview`} desc="WAL health, manifest, packs, bundles (what the WAL tab shows)." cache="no-store" />
            <Row
              path={`/${r}/api/tasks · /tasks/{id} · POST /ops/{op}`}
              desc={<>What the answering instance is doing to the repo (<code>{`{hostname,running,recent}`}</code>); attach to a task or start a maintenance op (write) as an SSE stream.</>}
              cache="no-store"
            />
          </tbody>
        </table>
      </Box>

      <div className="row gap">
        <Box title="Lanes & auth" className="grow">
          <ul className="api-notes">
            <li>
              <strong>Bearer lane</strong>: <code>Authorization: Bearer &lt;token&gt;</code> — a walgit access token,
              a static token from the server's config, or an ID token from the OIDC issuer; exactly what git uses. The bundled UI uses the same paths with its session cookie. Repository calls live
              under the repository's own prefix, <code>/{"{owner}/{repo}"}/api/*</code> — one rule routes a whole
              repository to its host.
            </li>
            <li>
              <strong>Browser lane</strong> <code>/{"{owner}/{repo}"}/api-browser/*</code>: other configured origins call with{" "}
              <code>credentials: "include"</code>; CORS is granted to the configured origins only; sign-in happens in a popup
              at <code>/api-browser/v1/authenticate</code>. The SDK does all of this for you.
            </li>
            <li>
              <strong>Errors</strong> are plain text with the right status: <code>404</code> for unknown owner / repo / ref /
              path / sha, <code>401</code> when not signed in, <code>5xx</code> for faults. No JSON envelope to unwrap.
            </li>
            <li>
              <strong>Shapes</strong>: arrays are <code>[]</code> when empty, never null; timestamps RFC 3339; shas full 40-hex;
              sizes in bytes; path segments URL-encoded per segment. Additive changes ship in <code>v1</code>.
            </li>
          </ul>
        </Box>
        <Box title="Caching & long answers" className="grow">
          <ul className="api-notes">
            <li>
              <strong>Resolve once, then by sha.</strong> Anything addressed by a full sha is <code>immutable</code> — cache it
              anywhere, forever. Anything addressed by a name is <code>stale-while-revalidate=60</code> plus an{" "}
              <code>ETag</code> for <code>If-None-Match</code> → 304.
            </li>
            <li>
              <strong>Fresh</strong>: after a push is acknowledged, the next call on any instance reflects it; no hot path
              scales with ref count or pack size.
            </li>
            <li>
              <strong>Long answers</strong>: send <code>Accept: application/json, text/event-stream</code> and a cold instance
              streams <code>notice</code> / <code>progress</code> / <code>task</code> packets before exactly one{" "}
              <code>result</code> (the JSON) or <code>error</code>. Without it the request simply waits. The SDK unwraps this
              and feeds <code>onProgress</code>.
            </li>
          </ul>
          <div className="pad" style={{ paddingTop: 0 }}>
            <CodeSample code={`${curl.replace("application/json", "application/json, text/event-stream")} -N "${base}/tree/main/"`} />
          </div>
        </Box>
      </div>

      <p className="small muted">
        Full contract (caching rules, SSE envelope, tasks, conformance checklist): <code>web/API.md</code>; SDK reference:{" "}
        <code>web/sdk/README.md</code> in the walgit source tree. Pushes and clones stay on the git smart-HTTP
        routes — see the Clone button on any repository.
      </p>
    </div>
  );
}

function Row({ path, desc, cache }: { path: string; desc: ReactNode; cache: string }) {
  return (
    <tr>
      <td>
        <code>{path}</code>
      </td>
      <td>{desc}</td>
      <td className="muted small">{cache}</td>
    </tr>
  );
}
