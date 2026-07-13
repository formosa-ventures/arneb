//! A1.3 integration test: end-to-end Flight roundtrip for the two
//! cross-fragment dynamic-filter messages.
//!
//! - `submit_task` carries `pending_dynamic_filters` (coord → worker)
//! - `report_dynamic_filters` action carries a partition Domain (worker → coord)
//! - `notify_dynamic_filter` action carries a resolved Domain (coord → worker)
//!
//! Verifies wire encoding + callback dispatch on a real Flight server,
//! not the deeper routing through `DynamicFilterServiceRegistry` /
//! `DynamicFilterCollector` (those have their own unit tests in the
//! `scheduler` and `execution` crates). A1.5 will exercise the full
//! producer → registry → notify → collector chain.

use std::sync::Arc;
use std::sync::Mutex;

use arneb_common::types::ScalarValue;
use arneb_common::{Domain, DynamicFilterId, QueryId, StageId, TaskId};
use arneb_rpc::{
    notify_dynamic_filter, report_dynamic_filters, submit_task, FlightState,
    NotifyDynamicFilterRequest, ReportDynamicFilterRequest, SourceExchange, TaskDescriptor,
};

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

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    addr_str
}

fn sample_domain() -> Domain {
    Domain::DistinctValues(vec![ScalarValue::Int64(11), ScalarValue::Int64(22)])
}

#[tokio::test]
async fn submit_task_carries_pending_dynamic_filters() {
    // Worker captures the descriptor it receives.
    let captured = Arc::new(Mutex::new(None::<TaskDescriptor>));
    let captured_for_cb = captured.clone();

    let mut state = FlightState::new();
    state.set_task_callback(Arc::new(move |descriptor: TaskDescriptor| {
        *captured_for_cb.lock().unwrap() = Some(descriptor);
    }));

    let worker_addr = start_test_flight_server(state).await;

    // Coord builds a descriptor with two pre-resolved DFs.
    let descriptor = TaskDescriptor {
        task_id: TaskId {
            stage_id: StageId(7),
            partition_id: 0,
        },
        stage_id: StageId(7),
        query_id: QueryId::new(),
        plan_json: "{}".into(),
        output_partitions: 1,
        output_hash_columns: Vec::new(),
        broadcast: false,
        source_exchanges: Vec::<SourceExchange>::new(),
        pending_dynamic_filters: vec![
            (DynamicFilterId(0), sample_domain()),
            (DynamicFilterId(1), Domain::All),
        ],
        scan_task_count: 1,
        must_drain: false,
    };

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        submit_task(&worker_addr, &descriptor),
    )
    .await
    .expect("submit_task timed out")
    .expect("submit_task failed");

    // Give the callback a moment to land.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let received = captured.lock().unwrap().clone().expect("no descriptor");
    assert_eq!(received.pending_dynamic_filters.len(), 2);
    assert_eq!(received.pending_dynamic_filters[0].0, DynamicFilterId(0));
    assert_eq!(received.pending_dynamic_filters[0].1, sample_domain());
    assert!(matches!(received.pending_dynamic_filters[1].1, Domain::All));
}

#[tokio::test]
async fn report_dynamic_filters_action_roundtrip() {
    // Coord captures the report.
    let captured = Arc::new(Mutex::new(None::<ReportDynamicFilterRequest>));
    let captured_for_cb = captured.clone();

    let mut state = FlightState::new();
    state.set_df_report_callback(Arc::new(move |req: ReportDynamicFilterRequest| {
        *captured_for_cb.lock().unwrap() = Some(req);
    }));

    let coord_addr = start_test_flight_server(state).await;

    let req = ReportDynamicFilterRequest {
        query_id: QueryId::new(),
        task_id: TaskId {
            stage_id: StageId(3),
            partition_id: 2,
        },
        df_id: DynamicFilterId(42),
        partition_idx: 2,
        domain: sample_domain(),
    };

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        report_dynamic_filters(&coord_addr, &req),
    )
    .await
    .expect("report timed out")
    .expect("report failed");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let received = captured.lock().unwrap().clone().expect("no report");
    assert_eq!(received.df_id, DynamicFilterId(42));
    assert_eq!(received.partition_idx, 2);
    assert_eq!(received.domain, sample_domain());
}

#[tokio::test]
async fn notify_dynamic_filter_action_roundtrip() {
    // Worker captures the notify.
    let captured = Arc::new(Mutex::new(None::<NotifyDynamicFilterRequest>));
    let captured_for_cb = captured.clone();

    let mut state = FlightState::new();
    state.set_df_notify_callback(Arc::new(move |req: NotifyDynamicFilterRequest| {
        *captured_for_cb.lock().unwrap() = Some(req);
    }));

    let worker_addr = start_test_flight_server(state).await;

    let req = NotifyDynamicFilterRequest {
        query_id: QueryId::new(),
        task_id: TaskId {
            stage_id: StageId(9),
            partition_id: 1,
        },
        df_id: DynamicFilterId(7),
        domain: sample_domain(),
    };

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        notify_dynamic_filter(&worker_addr, &req),
    )
    .await
    .expect("notify timed out")
    .expect("notify failed");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let received = captured.lock().unwrap().clone().expect("no notify");
    assert_eq!(received.df_id, DynamicFilterId(7));
    assert_eq!(received.domain, sample_domain());
}

#[tokio::test]
async fn report_with_no_callback_is_silently_acked() {
    // FlightState has no callback registered — server still acks
    // the report cleanly so the worker doesn't see a transport error.
    let state = FlightState::new();
    let coord_addr = start_test_flight_server(state).await;

    let req = ReportDynamicFilterRequest {
        query_id: QueryId::new(),
        task_id: TaskId {
            stage_id: StageId(0),
            partition_id: 0,
        },
        df_id: DynamicFilterId(0),
        partition_idx: 0,
        domain: Domain::All,
    };

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        report_dynamic_filters(&coord_addr, &req),
    )
    .await
    .expect("report timed out");

    // Quiet ack, not error.
    assert!(result.is_ok(), "expected ok, got {result:?}");
}
