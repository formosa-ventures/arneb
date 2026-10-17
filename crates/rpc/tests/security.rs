//! Cluster RPC security: bearer secret and TLS on real Flight servers.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arneb_common::{QueryId, StageId, TaskId};
use arneb_rpc::{
    cancel_query_with, send_heartbeat_with, serve_flight, submit_task_with, BufferKind,
    ClusterSecret, ClusterSecurity, ClusterTls, ExchangeClient, FlightState, HeartbeatError,
    HeartbeatMessage, OutputBuffer, SourceExchange, TaskDescriptor,
};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

fn secret(c: char) -> ClusterSecret {
    ClusterSecret::new(c.to_string().repeat(40)).unwrap()
}

fn with_secret(c: char) -> ClusterSecurity {
    ClusterSecurity {
        secret: Some(secret(c)),
        tls: None,
    }
}

/// A CA and a `localhost` node certificate it signed.
fn cluster_tls() -> ClusterTls {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .signed_by(&key, &issuer)
        .unwrap();
    ClusterTls {
        cert_pem: cert.pem().into_bytes(),
        key_pem: key.serialize_pem().into_bytes(),
        ca_pem: ca.pem().into_bytes(),
    }
}

/// Serve `state` with `security` on an ephemeral port; returns
/// `http://localhost:<port>` (upgraded to https by the client under TLS).
async fn start(state: FlightState, security: ClusterSecurity) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        serve_flight(listener, state, &security).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    format!("http://localhost:{port}")
}

fn heartbeat() -> HeartbeatMessage {
    HeartbeatMessage {
        worker_id: "w1".into(),
        flight_address: "http://localhost:1".into(),
        max_splits: 8,
    }
}

fn descriptor() -> TaskDescriptor {
    TaskDescriptor {
        task_id: TaskId {
            stage_id: StageId(1),
            partition_id: 0,
        },
        stage_id: StageId(1),
        query_id: QueryId::new(),
        plan_json: "{}".into(),
        output_partitions: 1,
        output_hash_columns: Vec::new(),
        broadcast: false,
        source_exchanges: Vec::<SourceExchange>::new(),
        pending_dynamic_filters: Vec::new(),
        scan_task_count: 1,
        must_drain: false,
    }
}

/// A Flight state that records heartbeats and task submissions, with a
/// one-batch buffer registered as `task-1`.
async fn recording_state() -> (FlightState, Arc<Mutex<usize>>, Arc<Mutex<usize>>) {
    let beats = Arc::new(Mutex::new(0));
    let tasks = Arc::new(Mutex::new(0));
    let b = beats.clone();
    let mut state = FlightState::with_heartbeat_callback(Arc::new(move |_| {
        *b.lock().unwrap() += 1;
    }));
    let t = tasks.clone();
    state.set_task_callback(Arc::new(move |_| {
        *t.lock().unwrap() += 1;
    }));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let buf = OutputBuffer::single(4, schema.clone());
    buf.write_batch(
        0,
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap(),
    )
    .await
    .unwrap();
    state.register_partitioned_buffer("task-1".into(), buf);
    if let Some(BufferKind::Partitioned(buf)) = state.get_buffer("task-1") {
        buf.lock().await.close();
    }
    (state, beats, tasks)
}

/// Count `cancel_query` actions that reach the worker.
fn count_cancels(state: &mut FlightState) -> Arc<Mutex<usize>> {
    let cancels = Arc::new(Mutex::new(0));
    let c = cancels.clone();
    state.set_cancel_query_callback(Arc::new(move |_| {
        *c.lock().unwrap() += 1;
        0
    }));
    cancels
}

async fn fetch_rows(addr: &str, security: ClusterSecurity) -> Result<usize, String> {
    let client = ExchangeClient::new(addr).with_security(security);
    let stream = tokio::time::timeout(Duration::from_secs(5), client.fetch_partition("task-1", 0))
        .await
        .map_err(|_| "timed out".to_owned())?
        .map_err(|e| e.to_string())?;
    let batches = arneb_common::stream::collect_stream(stream)
        .await
        .map_err(|e| e.to_string())?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

#[tokio::test]
async fn matching_secret_allows_every_call() {
    let (mut state, beats, tasks) = recording_state().await;
    let cancels = count_cancels(&mut state);
    let addr = start(state, with_secret('a')).await;
    let sec = with_secret('a');
    send_heartbeat_with(&addr, &heartbeat(), &sec)
        .await
        .unwrap();
    submit_task_with(&addr, &descriptor(), &sec).await.unwrap();
    cancel_query_with(&addr, QueryId::new(), &sec)
        .await
        .unwrap();
    assert_eq!(*cancels.lock().unwrap(), 1);
    assert_eq!(fetch_rows(&addr, sec).await.unwrap(), 3);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(*beats.lock().unwrap(), 1);
    assert_eq!(*tasks.lock().unwrap(), 1);
}

#[tokio::test]
async fn wrong_or_missing_secret_is_unauthenticated() {
    let (mut state, beats, tasks) = recording_state().await;
    let cancels = count_cancels(&mut state);
    let addr = start(state, with_secret('a')).await;
    for sec in [with_secret('b'), ClusterSecurity::default()] {
        let err = send_heartbeat_with(&addr, &heartbeat(), &sec)
            .await
            .unwrap_err();
        assert!(matches!(err, HeartbeatError::Unauthenticated(_)), "{err}");
        assert!(err.to_string().contains("[cluster] secret"), "{err}");
        let err = submit_task_with(&addr, &descriptor(), &sec)
            .await
            .unwrap_err();
        assert!(err.contains("invalid cluster secret"), "{err}");
        let err = cancel_query_with(&addr, QueryId::new(), &sec)
            .await
            .unwrap_err();
        assert!(err.contains("invalid cluster secret"), "{err}");
        let err = fetch_rows(&addr, sec).await.unwrap_err();
        assert!(err.contains("invalid cluster secret"), "{err}");
    }
    assert_eq!(*cancels.lock().unwrap(), 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The worker was never registered and no task ran.
    assert_eq!(*beats.lock().unwrap(), 0);
    assert_eq!(*tasks.lock().unwrap(), 0);
}

#[tokio::test]
async fn no_secret_anywhere_is_unchanged() {
    let (state, beats, _) = recording_state().await;
    let addr = start(state, ClusterSecurity::default()).await;
    let sec = ClusterSecurity::default();
    send_heartbeat_with(&addr, &heartbeat(), &sec)
        .await
        .unwrap();
    assert_eq!(fetch_rows(&addr, sec).await.unwrap(), 3);
    assert_eq!(*beats.lock().unwrap(), 1);
}

#[tokio::test]
async fn tls_with_the_cluster_ca() {
    let tls = cluster_tls();
    let (state, _, _) = recording_state().await;
    let server = ClusterSecurity {
        secret: Some(secret('a')),
        tls: Some(tls.clone()),
    };
    let addr = start(state, server.clone()).await;
    assert_eq!(fetch_rows(&addr, server.clone()).await.unwrap(), 3);

    // A client trusting a different CA cannot connect.
    let other = cluster_tls();
    let stranger = ClusterSecurity {
        secret: Some(secret('a')),
        tls: Some(ClusterTls {
            ca_pem: other.ca_pem,
            ..tls.clone()
        }),
    };
    assert!(fetch_rows(&addr, stranger).await.is_err());

    // Neither can a plaintext client.
    assert!(fetch_rows(&addr, with_secret('a')).await.is_err());
}
