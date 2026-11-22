//! #244: Flight served on its own control runtime keeps answering while
//! every execution thread is busy, and the work behind a request still
//! runs on the execution runtime.

use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use arneb_rpc::{BroadcastOutputBuffer, ExchangeClient, FlightState};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

const EXEC_THREADS: usize = 2;

fn runtime(name: &str, threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name(name)
        .enable_all()
        .build()
        .unwrap()
}

/// Serve `state` on `on` and return its address once it answers.
fn serve(
    on: &tokio::runtime::Runtime,
    client: &tokio::runtime::Runtime,
    state: FlightState,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    on.spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        arneb_rpc::__serve_flight_for_test(listener, state).await;
    });
    client.block_on(async {
        for _ in 0..50 {
            if arneb_rpc::ping(&addr, Duration::from_millis(200))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("Flight server never answered");
    });
    addr
}

/// Block every thread of `exec` for `hold`, as CPU-bound operators that
/// never yield do. Returns once all of them are held.
fn saturate(exec: &tokio::runtime::Runtime, hold: Duration) {
    let all_held = Arc::new(Barrier::new(EXEC_THREADS + 1));
    for _ in 0..EXEC_THREADS {
        let all_held = all_held.clone();
        exec.spawn(async move {
            all_held.wait();
            std::thread::sleep(hold);
        });
    }
    all_held.wait();
}

fn client() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn ping_answers_while_every_execution_thread_is_busy() {
    let exec = runtime("exec-test", EXEC_THREADS);
    let control = runtime("control-test", 2);
    let client = client();
    let mut state = FlightState::new();
    state.set_execution_runtime(exec.handle().clone());
    let addr = serve(&control, &client, state);

    saturate(&exec, Duration::from_secs(3));
    let started = Instant::now();
    client
        .block_on(arneb_rpc::ping(&addr, Duration::from_secs(1)))
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    exec.shutdown_background();
}

/// The failure #244 fixes: Flight on the runtime whose threads are all
/// held does not answer until they free up.
#[test]
fn ping_waits_when_flight_shares_the_busy_runtime() {
    let exec = runtime("exec-test", EXEC_THREADS);
    let client = client();
    let addr = serve(&exec, &client, FlightState::new());

    saturate(&exec, Duration::from_secs(3));
    let err = client
        .block_on(arneb_rpc::ping(&addr, Duration::from_millis(500)))
        .unwrap_err();
    assert!(err.contains("no answer within 500 ms"), "{err}");
    exec.shutdown_background();
}

#[test]
fn work_a_request_spawns_runs_on_the_execution_runtime() {
    let exec = runtime("exec-test", EXEC_THREADS);
    let control = runtime("control-test", 2);
    let client = client();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let tx = Mutex::new(tx);
    let mut state = FlightState::new();
    state.set_cancel_query_callback(Arc::new(move |_| {
        let tx = tx.lock().unwrap().clone();
        tokio::spawn(async move {
            let name = std::thread::current().name().unwrap_or("").to_string();
            let _ = tx.send(name);
        });
        1
    }));
    state.set_execution_runtime(exec.handle().clone());
    let addr = serve(&control, &client, state);

    client
        .block_on(arneb_rpc::cancel_query(&addr, arneb_common::QueryId::new()))
        .unwrap();
    let thread = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(thread.starts_with("exec-test"), "spawned on {thread}");
}

#[test]
fn do_get_output_reaches_the_consumer_through_the_control_runtime() {
    let exec = runtime("exec-test", EXEC_THREADS);
    let control = runtime("control-test", 2);
    let client = client();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let buffer = Arc::new(BroadcastOutputBuffer::new(schema.clone()));
    for start in [0i64, 100, 200] {
        let values: Vec<i64> = (start..start + 100).collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        buffer.write_batch(batch);
    }
    buffer.finish();
    let mut state = FlightState::new();
    state.register_broadcast_buffer("q.t".into(), buffer);
    state.set_execution_runtime(exec.handle().clone());
    let addr = serve(&control, &client, state);

    let sum: i64 = client.block_on(async {
        let mut stream = ExchangeClient::new(addr)
            .fetch_partition("q.t", 0)
            .await
            .unwrap();
        let mut sum = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            sum += col.values().iter().sum::<i64>();
        }
        sum
    });
    assert_eq!(sum, (0..300).sum::<i64>());
}
