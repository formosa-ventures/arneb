//! #245: the coordinator's liveness probe. A worker's Flight server
//! answers `ping`; a peer that accepts the connection but never answers
//! (a stopped process) and a closed port both fail within the timeout.

use std::time::{Duration, Instant};

use arneb_rpc::FlightState;

#[tokio::test]
async fn a_flight_server_answers_ping() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(arneb_rpc::__serve_flight_for_test(
        listener,
        FlightState::new(),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    arneb_rpc::ping(&addr, Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_peer_that_never_answers_times_out() {
    // Accepts TCP (as the kernel does for a SIGSTOPped worker) but never
    // speaks HTTP/2.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    let started = Instant::now();
    let err = arneb_rpc::ping(&addr, Duration::from_millis(300))
        .await
        .unwrap_err();
    assert!(err.contains("no answer within 300 ms"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn a_closed_port_fails() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    drop(listener);
    let err = arneb_rpc::ping(&addr, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(err.contains("failed to connect"), "{err}");
}
