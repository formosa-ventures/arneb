//! admission-control D3: a worker task's memory-limit failure reaches the
//! consumer as `ExecutionError::ResourceExhausted`, on both failure paths:
//! the task failed before registering its buffer (task-status callback), and
//! the producer failed mid-stream (buffer failure flag).

use std::sync::Arc;
use std::time::Duration;

use arneb_common::error::{ArnebError, ExecutionError};
use arneb_rpc::{
    tag_resource_exhausted, BufferKind, ExchangeClient, FlightState, OutputBuffer,
    TaskStatusResponse, RESOURCE_EXHAUSTED_TAG,
};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

async fn start(state: FlightState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(arneb_rpc::__flight_service_for_test(state))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    addr
}

fn assert_resource_exhausted(err: &ExecutionError) {
    match err {
        ExecutionError::ResourceExhausted(msg) => {
            assert!(msg.contains("query memory limit exceeded"), "{msg}");
            assert!(!msg.contains(RESOURCE_EXHAUSTED_TAG), "tag leaked: {msg}");
        }
        other => panic!("expected ResourceExhausted, got {other:?}"),
    }
}

#[tokio::test]
async fn failed_task_status_keeps_the_class() {
    let mut state = FlightState::new();
    state.set_task_status_callback(Arc::new(|_| {
        Some(TaskStatusResponse::Failed(tag_resource_exhausted(
            "query memory limit exceeded: HashAggregateExec",
        )))
    }));
    let addr = start(state).await;
    let err = match ExchangeClient::new(&addr).fetch_partition("q.t", 0).await {
        Ok(_) => panic!("fetch should fail"),
        Err(e) => e,
    };
    assert_resource_exhausted(&err);
}

#[tokio::test]
async fn mid_stream_failure_keeps_the_class() {
    let state = FlightState::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let buf = OutputBuffer::single(4, schema);
    let failure = buf.failure_handle();
    state.register_partitioned_buffer("q.t".into(), buf);
    *failure.lock().unwrap() = Some(tag_resource_exhausted(
        "stream error during execution: query memory limit exceeded",
    ));
    if let Some(BufferKind::Partitioned(buf)) = state.get_buffer("q.t") {
        buf.lock().await.close();
    }
    let addr = start(state).await;

    let err = match ExchangeClient::new(&addr).fetch_partition("q.t", 0).await {
        Err(e) => e,
        Ok(mut stream) => match stream.next().await {
            Some(Err(ArnebError::Execution(e))) => e,
            other => panic!("expected an execution error, got {other:?}"),
        },
    };
    assert_resource_exhausted(&err);
}
