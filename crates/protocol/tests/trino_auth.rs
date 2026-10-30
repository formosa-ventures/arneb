//! End-to-end tests for HTTP Basic auth on the Trino listener
//! (`[auth] type = "password"`). Plain HTTP keeps the tests focused on auth;
//! the server binary refuses password auth without `[tls]`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use serde_json::Value;
use tokio::net::TcpListener;

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{CredentialSource, ScramVerifier, TrinoConfig, TrinoServer, UserCredentials};

mod token_source;
use token_source::TokenSource;

fn creds() -> Arc<dyn CredentialSource> {
    Arc::new(
        UserCredentials::new(vec![
            (
                "alice".to_owned(),
                ScramVerifier::generate("alice-pw").unwrap(),
            ),
            ("bob".to_owned(), ScramVerifier::generate("bob-pw").unwrap()),
        ])
        .unwrap(),
    )
}

/// Start a server; returns its base URL and the verification counter.
async fn start(auth: Option<Arc<dyn CredentialSource>>) -> (String, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = TrinoServer::new(
        TrinoConfig {
            bind_address: format!("127.0.0.1:{port}"),
            ..TrinoConfig::default()
        },
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
    .with_password_auth(auth)
    .unwrap();
    let counter = server.password_verifications();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    (format!("http://127.0.0.1:{port}"), counter)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

async fn submit(
    base: &str,
    auth: Option<(&str, &str)>,
    headers: &[(&str, &str)],
) -> reqwest::Response {
    let mut req = client()
        .post(format!("{base}/v1/statement"))
        .body("SELECT 1");
    if let Some((u, p)) = auth {
        req = req.basic_auth(u, Some(p));
    }
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    req.send().await.unwrap()
}

/// Follow `nextUri` as `user` until the query finishes.
async fn drain(mut doc: Value, user: (&str, &str)) -> Value {
    while let Some(next) = doc.get("nextUri").and_then(Value::as_str) {
        let resp = client()
            .get(next)
            .basic_auth(user.0, Some(user.1))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        doc = resp.json().await.unwrap();
    }
    doc
}

#[tokio::test]
async fn credentials_decide_the_status() {
    let (base, _) = start(Some(creds())).await;

    let ok = submit(&base, Some(("alice", "alice-pw")), &[]).await;
    assert_eq!(ok.status(), StatusCode::OK);
    let doc = drain(ok.json().await.unwrap(), ("alice", "alice-pw")).await;
    assert_eq!(doc["stats"]["state"], "FINISHED");

    for (auth, what) in [
        (Some(("alice", "wrong")), "wrong password"),
        (Some(("mallory", "alice-pw")), "unknown user"),
        (None, "missing header"),
    ] {
        let resp = submit(&base, auth, &[]).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{what}");
        assert_eq!(
            resp.headers()["WWW-Authenticate"],
            "Basic realm=\"arneb\"",
            "{what}"
        );
    }
}

#[tokio::test]
async fn health_endpoints_need_no_credentials() {
    let (base, _) = start(Some(creds())).await;
    for path in ["/v1/info", "/v1/info/state", "/v1/status"] {
        let resp = client().get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path}");
    }
    let resp = client()
        .get(format!("{base}/v1/query"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authenticated_user_is_the_session_user() {
    let (base, _) = start(Some(creds())).await;

    let resp = submit(
        &base,
        Some(("alice", "alice-pw")),
        &[("X-Trino-User", "bob")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // No X-Trino-User: the query runs as alice.
    let resp = submit(&base, Some(("alice", "alice-pw")), &[]).await;
    let doc: Value = resp.json().await.unwrap();
    let id = doc["id"].as_str().unwrap().to_owned();
    drain(doc, ("alice", "alice-pw")).await;
    let info: Value = client()
        .get(format!("{base}/v1/query/{id}"))
        .basic_auth("alice", Some("alice-pw"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(info["session"]["user"], "alice");
}

#[tokio::test]
async fn queries_are_private_to_their_owner() {
    let (base, _) = start(Some(creds())).await;
    let doc: Value = submit(&base, Some(("alice", "alice-pw")), &[])
        .await
        .json()
        .await
        .unwrap();
    let id = doc["id"].as_str().unwrap().to_owned();
    let next = doc["nextUri"].as_str().unwrap().to_owned();

    let as_bob = |req: reqwest::RequestBuilder| req.basic_auth("bob", Some("bob-pw"));
    assert_eq!(
        as_bob(client().get(&next)).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        as_bob(client().delete(&next))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        as_bob(client().get(format!("{base}/v1/query/{id}")))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        as_bob(client().delete(format!("{base}/v1/query/{id}")))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let listed: Value = as_bob(client().get(format!("{base}/v1/query")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 0);

    // The owner still drains it.
    let doc = drain(
        client()
            .get(&next)
            .basic_auth("alice", Some("alice-pw"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap(),
        ("alice", "alice-pw"),
    )
    .await;
    assert_eq!(doc["stats"]["state"], "FINISHED");
}

#[tokio::test]
async fn repeated_requests_verify_once() {
    let (base, counter) = start(Some(creds())).await;
    for _ in 0..100 {
        let resp = client()
            .get(format!("{base}/v1/query"))
            .basic_auth("alice", Some("alice-pw"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // Failures are not cached: each wrong attempt is verified again.
    for _ in 0..3 {
        let resp = client()
            .get(format!("{base}/v1/query"))
            .basic_auth("alice", Some("nope"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
    assert_eq!(counter.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn none_mode_is_unchanged() {
    let (base, counter) = start(None).await;
    let resp = submit(&base, None, &[("X-Trino-User", "anyone")]).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let doc = resp.json().await.unwrap();
    let doc = drain(doc, ("ignored", "ignored")).await;
    assert_eq!(doc["stats"]["state"], "FINISHED");
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

/// Password auth over plain HTTP is refused on any non-loopback listener,
/// however the server was built (the binary also refuses it in config).
#[tokio::test]
async fn password_auth_without_tls_refuses_non_loopback() {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let server = TrinoServer::new(
        TrinoConfig::default(),
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
    .with_password_auth(Some(creds()))
    .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(5), server.serve(listener))
        .await
        .expect("serve must refuse, not run")
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("TLS"), "{err}");

    // Without password auth the same listener serves.
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let server = TrinoServer::new(
        TrinoConfig::default(),
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), server.serve(listener))
            .await
            .is_err(),
        "an unauthenticated listener keeps serving"
    );
}

/// A blank `X-Trino-User` must not hide a matching `X-Presto-User` from the
/// impersonation check and then leave the session user blank: the
/// authenticated user owns the query.
#[tokio::test]
async fn blank_trino_user_header_does_not_change_the_owner() {
    let (base, _) = start(Some(creds())).await;
    let resp = submit(
        &base,
        Some(("alice", "alice-pw")),
        &[("X-Trino-User", " "), ("X-Presto-User", "alice")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let doc: Value = resp.json().await.unwrap();
    let id = doc["id"].as_str().unwrap().to_owned();
    let doc = drain(doc, ("alice", "alice-pw")).await;
    assert_eq!(doc["stats"]["state"], "FINISHED");
    let info: Value = client()
        .get(format!("{base}/v1/query/{id}"))
        .basic_auth("alice", Some("alice-pw"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(info["session"]["user"], "alice");

    // Any user header naming someone else is impersonation, whichever
    // family it uses.
    let resp = submit(
        &base,
        Some(("alice", "alice-pw")),
        &[("X-Trino-User", "alice"), ("X-Presto-User", "bob")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

async fn list_queries(base: &str, user: &str, password: &str) -> StatusCode {
    client()
        .get(format!("{base}/v1/query"))
        .basic_auth(user, Some(password))
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn trino_basic_accepts_an_api_token_as_the_password() {
    let source = Arc::new(TokenSource::new(&[
        ("alice", "alice-pw"),
        ("bob", "bob-pw"),
    ]));
    let (base, counter) = start(Some(source.clone())).await;
    let token = source.issue("alice");

    let ok = submit(&base, Some(("alice", &token)), &[]).await;
    assert_eq!(ok.status(), StatusCode::OK);
    let doc = drain(ok.json().await.unwrap(), ("alice", &token)).await;
    assert_eq!(doc["stats"]["state"], "FINISHED");

    // The token authenticates only its owner.
    for user in ["bob", "mallory"] {
        let resp = submit(&base, Some((user, &token)), &[]).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{user}");
    }
    // Tokens take the fast path: no PBKDF2 verification ran.
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    // The password still works alongside the token.
    assert_eq!(
        list_queries(&base, "alice", "alice-pw").await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn revoked_token_is_refused_on_trino_basic_without_waiting_for_the_cache() {
    let source = Arc::new(TokenSource::new(&[("alice", "alice-pw")]));
    let (base, counter) = start(Some(source.clone())).await;
    let token = source.issue("alice");
    for _ in 0..3 {
        assert_eq!(list_queries(&base, "alice", &token).await, StatusCode::OK);
    }
    source.revoke(&token);
    assert_eq!(
        list_queries(&base, "alice", &token).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}
