mod harness;

use axum::http::StatusCode;

use harness::Server;

type TestResult = anyhow::Result<()>;

#[tokio::test]
async fn metrics_config_controls_scrape_endpoint() -> TestResult {
    let disabled = Server::start_with_tweak(|cfg| cfg.telemetry.metrics = false).await?;
    assert!(disabled.state.metrics_handle.is_none());
    let response = reqwest::get(format!("{}/metrics", disabled.base_url)).await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let enabled = Server::start_with_tweak(|cfg| cfg.telemetry.metrics = true).await?;
    assert!(enabled.state.metrics_handle.is_some());
    let response = reqwest::get(format!("{}/metrics", enabled.base_url)).await?;
    assert_eq!(response.status(), StatusCode::OK);

    Ok(())
}
