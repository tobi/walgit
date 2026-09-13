//! Exercise the real credential chain with synthetic STS/S3 servers. Each case
//! runs in a child process with a clean environment: no global env mutation,
//! developer credentials, AWS network access or dependency on an AWS account.
#![cfg(feature = "s3")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use aws_smithy_types::DateTime;
use aws_smithy_types::date_time::Format;
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{any, post};
use tokio::process::Command;
use walgit_config::{S3Config, StoreConfig};
use walgit_store::{GetOptions, ObjectStore, s3::S3Store};

#[derive(Default)]
struct Requests {
    tokens: Vec<String>,
    signed: Vec<String>,
    session_tokens: Vec<String>,
}

type Recorded = Arc<Mutex<Requests>>;

async fn sts(State(recorded): State<Recorded>, body: String) -> (HeaderMap, String) {
    assert!(body.contains("Action=AssumeRoleWithWebIdentity"));
    let sequence = {
        let mut requests = recorded.lock().unwrap();
        requests.tokens.push(body);
        requests.tokens.len()
    };
    // The startup probe takes the first identity; the second, which signs the
    // first request, is within the SDK's refresh window. The rest are long-lived.
    let lifetime = if sequence <= 2 { 1 } else { 3600 };
    let expires = DateTime::from(SystemTime::now() + Duration::from_secs(lifetime))
        .fmt(Format::DateTime)
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "text/xml".parse().unwrap());
    (
        headers,
        format!(
            "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
         <AssumeRoleWithWebIdentityResult><Credentials>\
         <AccessKeyId>TESTIRSAKEY{sequence}</AccessKeyId><SecretAccessKey>synthetic-secret</SecretAccessKey>\
         <SessionToken>synthetic-session-{sequence}</SessionToken><Expiration>{expires}</Expiration>\
         </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
        ),
    )
}

async fn s3(State(recorded): State<Recorded>, request: Request) -> (HeaderMap, Bytes) {
    let mut requests = recorded.lock().unwrap();
    if let Some(auth) = request.headers().get("authorization") {
        requests.signed.push(auth.to_str().unwrap().to_owned());
        requests.session_tokens.push(
            request
                .headers()
                .get("x-amz-security-token")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
        );
    } else {
        let url = reqwest::Url::parse(&format!("http://localhost{}", request.uri())).unwrap();
        let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
        requests
            .signed
            .push(query.get("X-Amz-Credential").unwrap().to_string());
        requests
            .session_tokens
            .push(query.get("X-Amz-Security-Token").unwrap().to_string());
    }
    let mut headers = HeaderMap::new();
    headers.insert("etag", "\"test-etag\"".parse().unwrap());
    headers.insert("content-length", "7".parse().unwrap());
    (headers, Bytes::from_static(b"payload"))
}

async fn run_case(case: &str, variables: &[(&str, &str)]) -> Recorded {
    let home = tempfile::tempdir().unwrap();
    let token_file = home.path().join("token");
    tokio::fs::write(&token_file, "projected-token-1")
        .await
        .unwrap();
    let recorded = Recorded::default();
    let app = axum::Router::new()
        .route("/", post(sts))
        .route("/{*path}", any(s3))
        .fallback(|| async { StatusCode::NOT_FOUND })
        .with_state(recorded.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", "credential_child", "--nocapture"])
        .env_clear()
        .env("HOME", home.path())
        .env("AWS_CONFIG_FILE", home.path().join("config"))
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            home.path().join("credentials"),
        )
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role")
        .env("AWS_ROLE_SESSION_NAME", "walgit-test")
        .env("AWS_WEB_IDENTITY_TOKEN_FILE", &token_file)
        .env("AWS_ENDPOINT_URL_STS", &endpoint)
        .env("WALGIT_CREDENTIAL_TEST", case)
        .env("WALGIT_CREDENTIAL_ENDPOINT", &endpoint)
        .envs(variables.iter().copied())
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(30), child.output())
        .await
        .expect("credential test timed out")
        .expect("run child");
    server.abort();
    assert!(
        output.status.success(),
        "credential child failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    recorded
}

#[tokio::test]
async fn web_identity_is_used_and_refreshed_for_sdk_and_presigned_requests() {
    let recorded = run_case("irsa", &[]).await;
    let requests = recorded.lock().unwrap();
    assert_eq!(
        requests.tokens.len(),
        3,
        "the startup probe, the first request's exchange and one refresh"
    );
    assert!(
        requests
            .tokens
            .first()
            .unwrap()
            .contains("WebIdentityToken=projected-token-1")
    );
    assert!(
        requests
            .tokens
            .last()
            .unwrap()
            .contains("WebIdentityToken=projected-token-2")
    );
    assert_eq!(
        requests.signed.len(),
        3,
        "HEAD, GET and a cached HEAD, no extra bucket probes"
    );
    assert!(requests.signed.first().unwrap().contains("TESTIRSAKEY2/"));
    assert!(
        requests
            .signed
            .iter()
            .skip(1)
            .all(|s| s.contains("TESTIRSAKEY3/"))
    );
    assert_eq!(
        requests.session_tokens,
        [
            "synthetic-session-2",
            "synthetic-session-3",
            "synthetic-session-3"
        ]
    );
}

#[tokio::test]
async fn custom_static_pair_overrides_default_chain_and_preserves_session_token() {
    let recorded = run_case(
        "static",
        &[
            ("WALGIT_TEST_ACCESS_KEY", "CUSTOMKEY"),
            ("WALGIT_TEST_SECRET_KEY", "custom-secret"),
            ("AWS_ACCESS_KEY_ID", "IGNOREDKEY"),
            ("AWS_SECRET_ACCESS_KEY", "ignored-secret"),
            ("AWS_SESSION_TOKEN", "custom-session"),
        ],
    )
    .await;
    let requests = recorded.lock().unwrap();
    assert!(
        requests.tokens.is_empty(),
        "explicit credentials must not call STS"
    );
    assert_eq!(requests.signed.len(), 1);
    assert!(requests.signed.first().unwrap().contains("CUSTOMKEY/"));
    assert_eq!(requests.session_tokens, ["custom-session"]);
}

#[tokio::test]
async fn partial_or_empty_explicit_keys_fail_without_falling_back() {
    for variables in [
        vec![("WALGIT_TEST_ACCESS_KEY", "partial")],
        vec![("WALGIT_TEST_SECRET_KEY", "partial")],
        vec![
            ("WALGIT_TEST_ACCESS_KEY", ""),
            ("WALGIT_TEST_SECRET_KEY", "secret"),
        ],
        vec![
            ("WALGIT_TEST_ACCESS_KEY", "access"),
            ("WALGIT_TEST_SECRET_KEY", ""),
        ],
    ] {
        let recorded = run_case("invalid", &variables).await;
        let requests = recorded.lock().unwrap();
        assert!(requests.tokens.is_empty());
        assert!(requests.signed.is_empty());
    }
}

#[tokio::test]
async fn a_host_with_no_identity_fails_at_startup_not_on_the_first_request() {
    let recorded = run_case("no-identity", &[]).await;
    let requests = recorded.lock().unwrap();
    assert!(requests.tokens.is_empty());
    assert!(requests.signed.is_empty());
}

// Re-entered only by run_case; never changes this test runner's environment.
#[tokio::test]
async fn credential_child() {
    let Ok(case) = std::env::var("WALGIT_CREDENTIAL_TEST") else {
        return;
    };
    let cfg = StoreConfig {
        bucket: "test-bucket".into(),
        s3: S3Config {
            endpoint: std::env::var("WALGIT_CREDENTIAL_ENDPOINT").unwrap(),
            force_path_style: true,
            access_key_env: "WALGIT_TEST_ACCESS_KEY".into(),
            secret_key_env: "WALGIT_TEST_SECRET_KEY".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    if case == "no-identity" {
        // No web identity, no profile, no IMDS: the chain has nothing to offer.
        std::fs::remove_file(std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").unwrap()).unwrap();
        let error = S3Store::new(&cfg)
            .await
            .err()
            .expect("an unresolvable chain must fail construction");
        assert!(error.to_string().contains("resolved nothing"));
        return;
    }
    let result = S3Store::new(&cfg).await;
    if case == "invalid" {
        let error = result
            .err()
            .expect("invalid credentials must fail construction");
        assert!(error.to_string().contains("leave both unset"));
        return;
    }
    let store = result.expect("construct store");
    assert_eq!(store.head("key").await.unwrap().unwrap().size, 7);
    if case == "irsa" {
        tokio::fs::write(
            std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").unwrap(),
            "projected-token-2",
        )
        .await
        .unwrap();
        // Expire even if the SDK changes its refresh-window jitter policy.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let (_, bytes) = store
            .get("key", GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes[..], b"payload");
        assert_eq!(store.head("key").await.unwrap().unwrap().size, 7);
    }
}
