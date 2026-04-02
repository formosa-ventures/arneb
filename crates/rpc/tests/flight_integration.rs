//! Integration tests for the Arrow Flight server and exchange client.

use std::sync::Arc;

use arneb_rpc::{ExchangeClient, FlightState, OutputBuffer};
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
    state.register_buffer("task-42".into(), buf);

    let addr = start_test_flight_server(state.clone()).await;

    // Spawn a task to close the buffer senders after a short delay (signals EOF).
    let state2 = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if let Some(buf) = state2.get_buffer("task-42") {
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
