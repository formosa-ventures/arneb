//! Integration tests for the Arrow Flight server and exchange client.

use std::sync::Arc;

use arneb_rpc::{BroadcastOutputBuffer, BufferKind, ExchangeClient, FlightState, OutputBuffer};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn test_batch(schema: &Arc<Schema>, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
}

/// Start a Flight server on a random port and return the address.
async fn start_test_flight_server(state: FlightState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let service = arneb_rpc::__flight_service_for_test(state);
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Give server time to start.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    addr_str
}

#[tokio::test]
async fn flight_server_do_get_roundtrip() {
    let schema = test_schema();
    let state = FlightState::new();

    // Create buffer, write data, then register.
    let buf = OutputBuffer::single(32, schema.clone());
    let batch = test_batch(&schema, vec![1, 2, 3, 4, 5]);
    buf.write_batch(0, batch).await.unwrap();
    state.register_partitioned_buffer("task-42".into(), buf);

    let addr = start_test_flight_server(state.clone()).await;

    // Spawn a task to close the buffer senders after a short delay (signals EOF).
    let state2 = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if let Some(BufferKind::Partitioned(buf)) = state2.get_buffer("task-42") {
            buf.lock().await.close();
        }
    });

    // Use ExchangeClient to fetch the data.
    let client = ExchangeClient::new(&addr);

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.fetch_partition("task-42", 0),
    )
    .await
    .expect("fetch timed out")
    .expect("fetch failed");

    let batches = arneb_common::stream::collect_stream(result).await.unwrap();

    assert!(!batches.is_empty(), "should receive at least one batch");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "should receive all 5 rows");
}

#[tokio::test]
async fn flight_server_heartbeat_roundtrip() {
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_clone = received.clone();

    let state = FlightState::with_heartbeat_callback(Arc::new(move |msg| {
        received_clone.lock().unwrap().push(msg);
    }));

    let addr = start_test_flight_server(state).await;

    // Send a heartbeat.
    let msg = arneb_rpc::HeartbeatMessage {
        worker_id: "test-worker".into(),
        flight_address: "http://localhost:9091".into(),
        max_splits: 128,
    };

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        arneb_rpc::send_heartbeat(&addr, &msg),
    )
    .await
    .expect("heartbeat timed out");

    result.expect("heartbeat failed");

    // Verify callback was invoked.
    let messages = received.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].worker_id, "test-worker");
    assert_eq!(messages[0].max_splits, 128);
}

// A2.1.4 (2026-05-28): forced-broadcast end-to-end tests. The fragmenter
// (A2.2, deferred) does not yet produce broadcast fragments, but the
// producer-side `register_broadcast_buffer` + server-side `do_get`
// Broadcast arm + `async_stream_broadcast` are wired in A2.1.2 — these
// tests exercise the full path with a hand-registered
// `BroadcastOutputBuffer`. Regression guard for the BufferKind enum
// dispatch and the per-consumer `subscribe()` semantics.

#[tokio::test]
async fn flight_server_broadcast_two_consumers_each_get_full_set() {
    let schema = test_schema();
    let state = FlightState::new();

    // Producer pre-writes batches before any consumer connects. The
    // historical-replay semantics of `BroadcastStream::next()` ensures
    // late subscribers still see them.
    let bbuf = Arc::new(BroadcastOutputBuffer::new(schema.clone()));
    bbuf.write_batch(test_batch(&schema, vec![1, 2, 3]));
    bbuf.write_batch(test_batch(&schema, vec![4, 5]));
    bbuf.write_batch(test_batch(&schema, vec![6]));
    bbuf.finish();
    state.register_broadcast_buffer("task-bcast".into(), Arc::clone(&bbuf));

    let addr = start_test_flight_server(state.clone()).await;

    // Two independent clients fetch concurrently; both must drain the
    // full 6-row payload. `partition_id` is informational on the
    // broadcast path — server ignores it.
    let client_a = ExchangeClient::new(&addr);
    let client_b = ExchangeClient::new(&addr);

    let fetch_a = tokio::spawn(async move {
        let stream = client_a
            .fetch_partition("task-bcast", 0)
            .await
            .expect("fetch_a failed");
        arneb_common::stream::collect_stream(stream).await.unwrap()
    });
    let fetch_b = tokio::spawn(async move {
        let stream = client_b
            .fetch_partition("task-bcast", 0)
            .await
            .expect("fetch_b failed");
        arneb_common::stream::collect_stream(stream).await.unwrap()
    });

    let batches_a = tokio::time::timeout(std::time::Duration::from_secs(5), fetch_a)
        .await
        .expect("fetch_a timed out")
        .expect("fetch_a panicked");
    let batches_b = tokio::time::timeout(std::time::Duration::from_secs(5), fetch_b)
        .await
        .expect("fetch_b timed out")
        .expect("fetch_b panicked");

    let rows_a: usize = batches_a.iter().map(|b| b.num_rows()).sum();
    let rows_b: usize = batches_b.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows_a, 6, "consumer A should receive all 6 rows");
    assert_eq!(rows_b, 6, "consumer B should receive all 6 rows");
}

#[tokio::test]
async fn flight_server_broadcast_late_subscriber_replays_then_tails() {
    let schema = test_schema();
    let state = FlightState::new();

    let bbuf = Arc::new(BroadcastOutputBuffer::new(schema.clone()));
    // Pre-write historical batches, then register + start server,
    // then write more, then finish. A subscriber arriving anywhere in
    // this lifecycle must end up with the full set.
    bbuf.write_batch(test_batch(&schema, vec![10, 20]));
    state.register_broadcast_buffer("task-late".into(), Arc::clone(&bbuf));

    let addr = start_test_flight_server(state.clone()).await;

    // Background producer: write 2 more batches over time, then finish.
    let bbuf_for_producer = Arc::clone(&bbuf);
    let schema_for_producer = schema.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        bbuf_for_producer.write_batch(test_batch(&schema_for_producer, vec![30, 40, 50]));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        bbuf_for_producer.write_batch(test_batch(&schema_for_producer, vec![60]));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        bbuf_for_producer.finish();
    });

    let client = ExchangeClient::new(&addr);
    let stream = client
        .fetch_partition("task-late", 0)
        .await
        .expect("fetch failed");

    let batches = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        arneb_common::stream::collect_stream(stream),
    )
    .await
    .expect("collect timed out")
    .expect("collect failed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 6,
        "late subscriber should replay historical batches and tail new ones"
    );
}
