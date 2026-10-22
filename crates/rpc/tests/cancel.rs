//! admission-control D4: the `cancel_query` Flight action reaches the
//! worker's callback with the right query id and is idempotent.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arneb_common::QueryId;
use arneb_rpc::{cancel_query, FlightState};

#[tokio::test]
async fn cancel_query_reaches_the_callback_and_is_idempotent() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut state = FlightState::new();
    let s = seen.clone();
    state.set_cancel_query_callback(Arc::new(move |q| {
        let mut seen = s.lock().unwrap();
        // First call for a query cancels "2 tasks", repeats cancel none.
        let n = if seen.contains(&q) { 0 } else { 2 };
        seen.push(q);
        n
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(arneb_rpc::__serve_flight_for_test(listener, state));
    tokio::time::sleep(Duration::from_millis(200)).await;

    let q = QueryId::new();
    cancel_query(&addr, q).await.unwrap();
    cancel_query(&addr, q).await.unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![q, q]);
}

#[tokio::test]
async fn cancel_query_without_a_callback_is_acked() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(arneb_rpc::__serve_flight_for_test(
        listener,
        FlightState::new(),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel_query(&addr, QueryId::new()).await.unwrap();
}
