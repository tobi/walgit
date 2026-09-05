//! HTTP serving of immutable store objects (bundles, LFS objects, packs) with
//! the complete conditional/range contract a CDN or `git` expects:
//!
//! * strong `ETag` = the store version (GCS generation / S3 `ETag`), quoted;
//! * `If-None-Match` (list or `*`) → `304` with the same validators;
//! * `If-Range` (`ETag` or ignored date) gating `Range`;
//! * single byte ranges incl. open-ended (`bytes=N-`) and suffix (`bytes=-N`),
//!   `206` + `Content-Range`, `416` + `Content-Range: bytes */total`;
//! * `HEAD` answered from metadata (no body download);
//! * `Content-Length` on every body so clients/proxies can stream and reuse
//!   connections; `Accept-Ranges: bytes`; `Cache-Control: public, immutable`.
//!
//! Objects addressed here are immutable by construction (content-addressed
//! names, or a generation-pinned URL), so the year-long immutable policy is
//! correct even on shared caches.

use std::ops::Range;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use walgit_store::{GetOptions, GetResult, ObjectMeta, ObjectStore, StoreError, Version};

use crate::error::ApiError;

pub const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// Request header an edge proxy (nginx, `deploy/nginx.conf.example`) sets to announce what it
/// can do on our behalf; comma/space separated tokens.
pub const CAPABILITIES_HEADER: &str = "x-walgit-capabilities";
/// Capability: the edge honours `X-Accel-Redirect: /_store/` by fetching the object from
/// the bucket itself — at the URL in `X-Walgit-Store-Url`, with `X-Walgit-Store-Authorization`
/// when present (GCS: bearer; S3: none, the URL is presigned) — slicing ranges and caching
/// the bytes on its disk under `X-Walgit-Store-Key`.
pub const CAP_ACCEL_REDIRECT: &str = "accel-redirect";
/// Internal nginx location that proxies to `X-Walgit-Store-Url`.
pub const ACCEL_LOCATION: &str = "/_store/";

/// True when the request came through an edge that advertised `accel-redirect`.
pub fn accel_requested(headers: &HeaderMap) -> bool {
    headers
        .get_all(CAPABILITIES_HEADER)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split([',', ' ']))
        .any(|t| t.trim().eq_ignore_ascii_case(CAP_ACCEL_REDIRECT))
}

/// What kind of range the client asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeSpec {
    /// `bytes=start-end` (inclusive end) or `bytes=start-`.
    From { start: u64, end: Option<u64> },
    /// `bytes=-n`: the last `n` bytes.
    Suffix(u64),
}

/// Parse a single-range `Range` header. Multi-range requests are not
/// supported: returns `None` (served as a full 200, as RFC 9110 allows).
pub fn parse_range(header: Option<&str>) -> Option<RangeSpec> {
    let spec = header?.trim().strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    let (a, b) = (a.trim(), b.trim());
    if a.is_empty() {
        return b
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0)
            .map(RangeSpec::Suffix);
    }
    let start: u64 = a.parse().ok()?;
    let end = if b.is_empty() {
        None
    } else {
        let e: u64 = b.parse().ok()?;
        if e < start {
            return None;
        }
        Some(e)
    };
    Some(RangeSpec::From { start, end })
}

impl RangeSpec {
    /// Resolve against the object size into a half-open byte range. `None`
    /// means unsatisfiable (→ 416).
    pub fn resolve(&self, total: u64) -> Option<Range<u64>> {
        match *self {
            RangeSpec::From { start, end } => {
                if start >= total {
                    return None;
                }
                let end = end.map_or(total, |e| e.saturating_add(1).min(total));
                Some(start..end)
            }
            RangeSpec::Suffix(n) => {
                if total == 0 {
                    return None;
                }
                Some(total.saturating_sub(n)..total)
            }
        }
    }
}

/// `ETag` header value for a store version.
pub fn etag_of(version: &Version) -> HeaderValue {
    let v = version.as_str().trim_matches('"');
    HeaderValue::from_str(&format!("\"{v}\"")).unwrap_or_else(|_| HeaderValue::from_static("\"-\""))
}

/// Entity tags in an `If-None-Match` / `If-Match` header (quotes stripped,
/// `W/` prefix dropped — our tags are strong, weak comparison is fine for 304).
fn etags(headers: &HeaderMap, name: header::HeaderName) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| {
            t.trim()
                .trim_start_matches("W/")
                .trim_matches('"')
                .to_string()
        })
        .filter(|t| !t.is_empty())
        .collect()
}

fn if_none_match_hit(headers: &HeaderMap, version: &Version) -> bool {
    let tags = etags(headers, header::IF_NONE_MATCH);
    let cur = version.as_str().trim_matches('"');
    tags.iter().any(|t| t == "*" || t == cur)
}

/// `If-Range`: if it names an `ETag` that does not match the current version the
/// range is ignored and the full body is sent (RFC 9110 §13.1.5). Dates are
/// not supported (we have no `Last-Modified`) and therefore also ignored.
fn if_range_allows(headers: &HeaderMap, version: &Version) -> bool {
    match headers.get(header::IF_RANGE).and_then(|v| v.to_str().ok()) {
        None => true,
        Some(v) if v.contains('"') => {
            v.trim().trim_start_matches("W/").trim_matches('"')
                == version.as_str().trim_matches('"')
        }
        Some(_) => false,
    }
}

pub struct ServeOptions<'a> {
    pub content_type: &'a str,
    /// Override for `Cache-Control` (default: immutable).
    pub cache_control: Option<&'a str>,
    /// `Content-Disposition` if the object should download under a name.
    pub filename: Option<&'a str>,
    /// When the request carries `X-Walgit-Capabilities: accel-redirect` and the store
    /// has an object URL, answer `200` + `X-Accel-Redirect` (no body) and let the
    /// edge move the bytes. `false`: always stream from here.
    pub accel: bool,
    /// TCP peer. Accel is honoured only when this is loopback (nginx on the
    /// same host). A client on a public bind cannot spoof the capability
    /// header and steal `X-Walgit-Store-Authorization`.
    pub peer: Option<std::net::SocketAddr>,
}

impl Default for ServeOptions<'_> {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream",
            cache_control: None,
            filename: None,
            accel: false,
            peer: None,
        }
    }
}

fn base_headers(resp: &mut Response, meta: &ObjectMeta, opts: &ServeOptions<'_>) {
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(opts.content_type)
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    h.insert(header::ETAG, etag_of(&meta.version));
    h.insert(header::CACHE_CONTROL, cache_control(opts));
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    if let Some(name) = opts.filename
        && let Ok(v) = HeaderValue::from_str(&format!(
            "attachment; filename=\"{}\"",
            name.replace('"', "")
        ))
    {
        h.insert(header::CONTENT_DISPOSITION, v);
    }
}

fn cache_control(opts: &ServeOptions<'_>) -> HeaderValue {
    opts.cache_control
        .and_then(|cc| HeaderValue::from_str(cc).ok())
        .unwrap_or(HeaderValue::from_static(IMMUTABLE))
}

fn not_modified(meta_version: &Version, opts: &ServeOptions<'_>) -> Response {
    let mut resp = StatusCode::NOT_MODIFIED.into_response();
    let h = resp.headers_mut();
    h.insert(header::ETAG, etag_of(meta_version));
    h.insert(header::CACHE_CONTROL, cache_control(opts));
    h.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    resp
}

fn range_not_satisfiable(meta: &ObjectMeta, opts: &ServeOptions<'_>) -> Response {
    let mut resp = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
    base_headers(&mut resp, meta, opts);
    resp.headers_mut().insert(
        header::CONTENT_RANGE,
        HeaderValue::from_str(&format!("bytes */{}", meta.size)).unwrap(),
    );
    resp.headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    resp
}

/// Serve `key` from `store` honouring the request `method` and `headers`.
/// Missing objects → `ApiError::NotFound`.
pub async fn serve(
    store: &dyn ObjectStore,
    key: &str,
    method: &Method,
    headers: &HeaderMap,
    opts: ServeOptions<'_>,
) -> Result<Response, ApiError> {
    let head = *method == Method::HEAD;
    let range = parse_range(headers.get(header::RANGE).and_then(|v| v.to_str().ok()));

    // Edge offload: the nginx in front told us it honours X-Accel-Redirect. We
    // still do auth (the caller did), existence, strong validators and 304 here;
    // nginx fetches the object with its own credentials, slices Range itself and
    // caches the bytes on its disk, so a 32 GB bundle never ties up a worker on
    // this instance. HEAD stays local (metadata only, nothing to offload).
    if opts.accel
        && !head
        && accel_requested(headers)
        && opts.peer.is_some_and(|p| p.ip().is_loopback())
        && let Some(target) = store.accel_target(key).await
    {
        let meta = match store.head(key).await {
            Ok(Some(m)) => m,
            Ok(None) => return Err(ApiError::NotFound(format!("{key} not found"))),
            Err(e) => return Err(e.into()),
        };
        if if_none_match_hit(headers, &meta.version) {
            return Ok(not_modified(&meta.version, &opts));
        }
        let mut resp = StatusCode::OK.into_response();
        base_headers(&mut resp, &meta, &opts);
        let h = resp.headers_mut();
        let hv = |s: &str| {
            HeaderValue::from_str(s).map_err(|e| ApiError::Internal(format!("accel header: {e}")))
        };
        h.insert("x-accel-redirect", HeaderValue::from_static(ACCEL_LOCATION));
        // Where and how the edge fetches. nginx keeps the upstream headers of this answer
        // across the internal redirect and never forwards them to the client.
        h.insert("x-walgit-store-url", hv(&target.url)?);
        if let Some(auth) = &target.authorization {
            h.insert("x-walgit-store-authorization", hv(auth)?);
        }
        // The edge's cache key: the object, not the (possibly presigned, changing) URL.
        h.insert(
            "x-walgit-store-key",
            hv(&walgit_store::util::encode_path(key))?,
        );
        h.insert("x-walgit-accel", HeaderValue::from_static(store.backend()));
        // nginx keeps only Content-Type/Disposition, Accept-Ranges, Cache-Control and Expires
        // of this answer across the internal redirect and would otherwise hand the client
        // the bucket's ETag (md5/crc form) — different from the version ETag our HEAD/304 use,
        // so `If-Range` would fail and a resumed download get the whole object. The edge
        // re-emits this header as the response ETag and hides the bucket's.
        h.insert("x-walgit-etag", etag_of(&meta.version));
        return Ok(resp);
    }

    // HEAD and Range both need the size before deciding what to fetch. For
    // a plain GET we want exactly one store round trip, so we pass the
    // client's If-None-Match straight through as a conditional read.
    if head || range.is_some() {
        let meta = match store.head(key).await {
            Ok(Some(m)) => m,
            Ok(None) => return Err(ApiError::NotFound(format!("{key} not found"))),
            Err(e) => return Err(e.into()),
        };
        if if_none_match_hit(headers, &meta.version) {
            return Ok(not_modified(&meta.version, &opts));
        }
        if head {
            let mut resp = StatusCode::OK.into_response();
            base_headers(&mut resp, &meta, &opts);
            resp.headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from(meta.size));
            return Ok(resp);
        }
        let spec = range.unwrap();
        if if_range_allows(headers, &meta.version) {
            let Some(r) = spec.resolve(meta.size) else {
                return Ok(range_not_satisfiable(&meta, &opts));
            };
            // Pin the generation we described to the client: a concurrent
            // rewrite (never expected for immutable keys) must not splice
            // bytes of two versions.
            let get = GetOptions {
                if_match: Some(meta.version.clone()),
                range: Some(r.clone()),
                ..Default::default()
            };
            return match store.get(key, get).await {
                Ok(GetResult::Object { meta: m, body }) => {
                    let total = m.size.max(meta.size);
                    let mut resp =
                        (StatusCode::PARTIAL_CONTENT, Body::from_stream(body)).into_response();
                    base_headers(&mut resp, &meta, &opts);
                    let h = resp.headers_mut();
                    h.insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!(
                            "bytes {}-{}/{}",
                            r.start,
                            r.end - 1,
                            total
                        ))
                        .unwrap(),
                    );
                    h.insert(header::CONTENT_LENGTH, HeaderValue::from(r.end - r.start));
                    Ok(resp)
                }
                Ok(GetResult::NotModified { .. }) => {
                    unreachable!("no if_none_match on a range read")
                }
                Err(StoreError::PreconditionFailed { .. }) => {
                    Err(ApiError::Conflict("object changed during read".into()))
                }
                Err(e) => Err(e.into()),
            };
        }
        // If-Range mismatch: fall through to a full 200.
    }

    // One tag maps onto the store's conditional read (GCS: metadata-only RPC,
    // no body). Lists / `*` are compared after the read; the body is dropped.
    let tags = etags(headers, header::IF_NONE_MATCH);
    let get = GetOptions {
        if_none_match: match tags.as_slice() {
            [one] if one != "*" => Some(Version::new(one.as_str())),
            _ => None,
        },
        ..Default::default()
    };
    match store.get(key, get).await {
        Ok(GetResult::NotModified { version }) => Ok(not_modified(&version, &opts)),
        Ok(GetResult::Object { meta, .. }) if if_none_match_hit(headers, &meta.version) => {
            Ok(not_modified(&meta.version, &opts))
        }
        Ok(GetResult::Object { meta, body }) => {
            let mut resp = (StatusCode::OK, Body::from_stream(body)).into_response();
            base_headers(&mut resp, &meta, &opts);
            resp.headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from(meta.size));
            Ok(resp)
        }
        Err(StoreError::NotFound { .. }) => Err(ApiError::NotFound(format!("{key} not found"))),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accel_capability() {
        let mut h = HeaderMap::new();
        assert!(!accel_requested(&h));
        h.insert(
            CAPABILITIES_HEADER,
            HeaderValue::from_static("foo, Accel-Redirect"),
        );
        assert!(accel_requested(&h));
    }

    #[test]
    fn parses_ranges() {
        assert_eq!(
            parse_range(Some("bytes=0-99")),
            Some(RangeSpec::From {
                start: 0,
                end: Some(99)
            })
        );
        assert_eq!(
            parse_range(Some("bytes=100-")),
            Some(RangeSpec::From {
                start: 100,
                end: None
            })
        );
        assert_eq!(
            parse_range(Some("bytes=-500")),
            Some(RangeSpec::Suffix(500))
        );
        assert_eq!(parse_range(Some("bytes=5-3")), None);
        assert_eq!(parse_range(Some("bytes=0-1,3-4")), None);
        assert_eq!(parse_range(Some("items=0-1")), None);
        assert_eq!(parse_range(None), None);
    }

    #[test]
    fn resolves_ranges() {
        assert_eq!(
            RangeSpec::From {
                start: 0,
                end: Some(99)
            }
            .resolve(50),
            Some(0..50)
        );
        assert_eq!(
            RangeSpec::From {
                start: 10,
                end: None
            }
            .resolve(50),
            Some(10..50)
        );
        assert_eq!(
            RangeSpec::From {
                start: 50,
                end: None
            }
            .resolve(50),
            None
        );
        assert_eq!(RangeSpec::Suffix(10).resolve(50), Some(40..50));
        assert_eq!(RangeSpec::Suffix(100).resolve(50), Some(0..50));
        assert_eq!(RangeSpec::Suffix(1).resolve(0), None);
    }

    #[test]
    fn etag_matching() {
        let mut h = HeaderMap::new();
        h.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("W/\"abc\", \"123\""),
        );
        assert!(if_none_match_hit(&h, &Version::new("123")));
        assert!(if_none_match_hit(&h, &Version::new("\"abc\"")));
        assert!(!if_none_match_hit(&h, &Version::new("999")));
        h.insert(header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        assert!(if_none_match_hit(&h, &Version::new("999")));
        let mut h = HeaderMap::new();
        h.insert(header::IF_RANGE, HeaderValue::from_static("\"123\""));
        assert!(if_range_allows(&h, &Version::new("123")));
        assert!(!if_range_allows(&h, &Version::new("124")));
        h.insert(
            header::IF_RANGE,
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
        assert!(!if_range_allows(&h, &Version::new("123")));
        assert_eq!(etag_of(&Version::new("12345")), "\"12345\"");
        assert_eq!(etag_of(&Version::new("\"e\"")), "\"e\"");
    }
}
