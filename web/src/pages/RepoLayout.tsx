import { Suspense, createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { Link, NavLink, Outlet, useLocation, useParams } from "react-router-dom";
import { api, type Refs, type RepoDescription } from "../api";
import { useData } from "../data";
import { RouteBoundary, Skeleton } from "../components/Loading";
import { CloneSetup } from "../components/CloneSetup";
import { TasksOverlay } from "../components/TasksOverlay";
import "../clone.css";

/** "Clone" dropdown: recipes come from `/services/setup.json` (cached after the first open). */
function CloneMenu({ full }: { full: string }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);
  return (
    <div className="clone-menu" ref={ref}>
      <button type="button" className="btn btn-primary" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
        Clone
      </button>
      {open && (
        <div className="clone-pop">
          <Suspense fallback={<Skeleton title={false} rows={4} />}>
            <CloneSetup repo={full} compact />
          </Suspense>
        </div>
      )}
    </div>
  );
}

export interface RepoCtx {
  owner: string;
  name: string;
  full: string; // owner/name
  refs: Refs;
}

const Ctx = createContext<RepoCtx | null>(null);

export function useRepo(): RepoCtx {
  const c = useContext(Ctx);
  if (!c) throw new Error("useRepo outside RepoLayout");
  return c;
}

/** Repo shell. The header (title, Clone, tabs) is static and paints at once;
 * only the body waits (Suspense skeleton) for the `refs` request, and page
 * navigations inside the repo keep the shell while the next page loads. */
export function RepoLayout() {
  const { owner = "", repo = "" } = useParams();
  const full = `${owner}/${repo}`;
  const { pathname } = useLocation();
  useEffect(() => {
    document.title = `${full} · walgit`;
    return () => {
      document.title = "walgit";
    };
  }, [full]);
  const walActive = pathname.endsWith("/wal");
  const settingsActive = pathname.endsWith("/settings");
  const codeActive = !walActive && !settingsActive && !/\/commits?(\/|$)/.test(pathname);
  return (
    <>
      <div className="repo-head">
        <h1 className="repo-title">
          <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden className="muted">
            <path
              fill="currentColor"
              d="M2 2.5A2.5 2.5 0 0 1 4.5 0h8.75a.75.75 0 0 1 .75.75v12.5a.75.75 0 0 1-.75.75h-2.5a.75.75 0 0 1 0-1.5h1.75v-2h-8a1 1 0 0 0-.714 1.7.75.75 0 1 1-1.072 1.05A2.495 2.495 0 0 1 2 11.5Zm10.5-1h-8a1 1 0 0 0-1 1v6.708A2.486 2.486 0 0 1 4.5 9h8ZM5 12.25a.25.25 0 0 1 .25-.25h3.5a.25.25 0 0 1 .25.25v3.25a.25.25 0 0 1-.4.2l-1.45-1.087a.25.25 0 0 0-.3 0L5.4 15.7a.25.25 0 0 1-.4-.2Z"
            />
          </svg>
          <Link to={`/${owner}`}>{owner}</Link>
          <span className="muted">/</span>
          <Link to={`/${full}`} className="strong">
            {repo}
          </Link>
        </h1>
        <CloneMenu full={full} />
        <Suspense fallback={null}>
          <RepoDescriptionLine full={full} />
        </Suspense>
        <nav className="tabs">
          <NavLink to={`/${full}`} className={() => (codeActive ? "tab active" : "tab")} end>
            Code
          </NavLink>
          <NavLink to={`/${full}/commits`} className={() => (codeActive || walActive || settingsActive ? "tab" : "tab active")}>
            Commits
          </NavLink>
          <NavLink to={`/${full}/wal`} className={() => (walActive ? "tab active" : "tab")}>
            WAL
          </NavLink>
          <NavLink to={`/${full}/settings`} className={() => (settingsActive ? "tab active" : "tab")}>
            Settings
          </NavLink>
          <TasksOverlay repo={full} />
        </nav>
      </div>
      <RouteBoundary fallback={<Skeleton title={false} rows={8} />}>
        <RepoBody owner={owner} repo={repo} full={full} />
      </RouteBoundary>
    </>
  );
}

/** The repository's description under its title; nothing when unset or unreadable (the
 * header must never fail on a label — the body reports real errors). */
function RepoDescriptionLine({ full }: { full: string }) {
  const d = useData(`description:${full}`, () => api.description(full).catch((): RepoDescription => ({})));
  return d.description ? <p className="repo-desc muted">{d.description}</p> : null;
}

function RepoBody({ owner, repo, full }: { owner: string; repo: string; full: string }) {
  const refs = useData(`refs:${full}`, () => api.refs(full));
  const ctx = useMemo(() => ({ owner, name: repo, full, refs }), [owner, repo, full, refs]);
  return (
    <Ctx.Provider value={ctx}>
      <Outlet />
    </Ctx.Provider>
  );
}
