import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { useData } from "../data";
import { Box } from "../components/Layout";

export function Repos() {
  const { owner = "" } = useParams();
  // One key for both: two `useData` calls would suspend on the first and start the second after it.
  const [profile, repos] = useData(`repos:${owner}`, () => Promise.all([api.ownerProfile(owner), api.repos(owner)]));
  return (
    <>
      <h1 className="page-title">
        <Link to="/">Repositories</Link> <span className="muted">/</span> {profile.display_name ?? owner}
        {profile.display_name && <code className="muted small id-aside">{owner}</code>}
      </h1>
      {profile.description && <p className="muted page-lede">{profile.description}</p>}
      <Box>
        {repos.length === 0 && (
          <div className="muted pad">
            No repositories under <code>{owner}</code>. Push to{" "}
            <code>{location.origin}/{owner}/repository.git</code>.
          </div>
        )}
        <ul className="list">
          {repos.map((r) => (
            <li key={r.name}>
              <Link to={`/${owner}/${r.name}`} className="strong">
                {owner}/{r.name}
              </Link>
              {r.description && <div className="small">{r.description}</div>}
              <div className="muted small">
                <code>
                  git clone {location.origin}/{owner}/{r.name}.git
                </code>
              </div>
            </li>
          ))}
        </ul>
      </Box>
    </>
  );
}
