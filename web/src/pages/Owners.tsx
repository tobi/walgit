import { Link } from "react-router-dom";
import { api } from "../api";
import { useData } from "../data";
import { Box } from "../components/Layout";
import { Hero } from "../components/Hero";
import { CodeSample } from "../components/CopyButton";

export function Owners() {
  const owners = useData("owners", api.owners);
  if (owners.length === 0) {
    return <BlankSlate />;
  }
  return (
    <>
      <Hero />
      <h2 className="page-title">Repositories by owner</h2>
      <Box>
        <ul className="list">
          {owners.map((o) => (
            <li key={o.name}>
              <Link to={`/${o.name}`} className="strong">
                {o.display_name ?? o.name}
              </Link>
              {o.display_name && <code className="muted small id-aside">{o.name}</code>}
              {o.description && <div className="muted small">{o.description}</div>}
            </li>
          ))}
        </ul>
      </Box>
    </>
  );
}

/** First repo: install.sh (this host:port) sets helper + proactiveAuth + origin. */
function BlankSlate() {
  const origin = window.location.origin;
  const host = window.location.host;
  const install = `sh -c "$(curl -fsSLk '${origin}/services/public/install.sh')" -- area/repository`;
  return (
    <div className="blankslate">
      <h1>Nothing here yet</h1>
      <p>
        This host has no repositories. From a local git tree, run the installer with{" "}
        <code>area/repository</code> — it turns on <code>http.https://{host}/.proactiveAuth=auto</code>{" "}
        (git must send a token up front) and points <code>origin</code> at{" "}
        <code>{origin}/area/repository.git</code>. Then push. Anything but{" "}
        <code>area/repository.git</code> is refused.
      </p>
      <Box title="Once, from your repo">
        <CodeSample code={`${install}\ngit push -u origin HEAD`} />
      </Box>
      <p className="muted small">
        <code>area</code> and <code>repository</code> are <code>[A-Za-z0-9._-]</code>, 1–100 characters.
      </p>
    </div>
  );
}
