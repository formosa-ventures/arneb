//! pgwire CancelRequest end to end: the cancel a client sends on a second
//! connection (psql's Ctrl-C, JDBC's `Statement.cancel`) stops the statement
//! running on the connection whose backend key it carries, and only that one.
//!
//! A stub distributed executor sleeps "forever" and records when its future
//! is dropped, standing in for a long distributed query: dropping it is what
//! cancels the query on the workers (the `statement_timeout` path).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::error::ArnebError;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::ExecutionContext;
use arneb_planner::LogicalPlan;
use arneb_protocol::{DistributedExecutor, ProtocolConfig, ProtocolServer};

/// Sleeps for 60 s; counts the futures dropped before they finished.
#[derive(Default)]
struct Slow {
    dropped: Arc<AtomicUsize>,
}

struct CountOnDrop(Arc<AtomicUsize>);
impl Drop for CountOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl DistributedExecutor for Slow {
    async fn execute(
        &self,
        _plan: LogicalPlan,
        _ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, ArnebError> {
        let _guard = CountOnDrop(Arc::clone(&self.dropped));
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(Vec::new())
    }

    fn has_workers(&self) -> bool {
        true
    }
}

/// A server over `slow`; returns its port.
async fn serve(slow: Arc<Slow>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
    .with_distributed_executor(slow);
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

async fn connect(port: u16) -> Arc<tokio_postgres::Client> {
    let (client, conn) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=t"), NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);
    Arc::new(client)
}

/// Start a slow statement on `client` in the background.
fn run_slow(
    client: &Arc<tokio_postgres::Client>,
) -> tokio::task::JoinHandle<Result<Vec<SimpleQueryMessage>, tokio_postgres::Error>> {
    let client = Arc::clone(client);
    tokio::spawn(async move { client.simple_query("SELECT 1").await })
}

async fn show_timeout(client: &tokio_postgres::Client) -> String {
    client
        .simple_query("SHOW statement_timeout")
        .await
        .unwrap()
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap()
}

fn assert_user_cancelled(result: Result<Vec<SimpleQueryMessage>, tokio_postgres::Error>) {
    let err = result.expect_err("the cancelled statement must fail");
    let db = err.as_db_error().expect("server error");
    assert_eq!(db.code().code(), "57014", "{}", db.message());
    assert!(db.message().contains("user request"), "{}", db.message());
}

#[tokio::test]
async fn cancel_request_stops_the_running_statement() {
    let slow = Arc::new(Slow::default());
    let dropped = Arc::clone(&slow.dropped);
    let port = serve(slow).await;
    let client = connect(port).await;

    let running = run_slow(&client);
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.cancel_token().cancel_query(NoTls).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("the cancel must end the statement promptly")
        .unwrap();
    assert_user_cancelled(result);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the query future was dropped"
    );
    // Same connection, still usable.
    assert_eq!(show_timeout(&client).await, "0");
}

#[tokio::test]
async fn cancel_request_stops_an_extended_protocol_statement() {
    // JDBC (and so Metabase) runs statements over the extended protocol.
    let slow = Arc::new(Slow::default());
    let dropped = Arc::clone(&slow.dropped);
    let port = serve(slow).await;
    let client = connect(port).await;

    let running = {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.query("SELECT 1", &[]).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.cancel_token().cancel_query(NoTls).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("the cancel must end the statement promptly")
        .unwrap()
        .expect_err("the cancelled statement must fail");
    assert_eq!(err.as_db_error().unwrap().code().code(), "57014", "{err:?}");
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the query future was dropped"
    );
    assert_eq!(show_timeout(&client).await, "0");
}

#[tokio::test]
async fn cancel_request_only_reaches_its_own_connection() {
    let slow = Arc::new(Slow::default());
    let dropped = Arc::clone(&slow.dropped);
    let port = serve(slow).await;
    let a = connect(port).await;
    let b = connect(port).await;

    let running_a = run_slow(&a);
    let running_b = run_slow(&b);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B's backend key must not cancel A's statement.
    b.cancel_token().cancel_query(NoTls).await.unwrap();
    let result_b = tokio::time::timeout(Duration::from_secs(5), running_b)
        .await
        .expect("B's cancel must end B's statement")
        .unwrap();
    assert_user_cancelled(result_b);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!running_a.is_finished(), "A's statement must keep running");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    a.cancel_token().cancel_query(NoTls).await.unwrap();
    let result_a = tokio::time::timeout(Duration::from_secs(5), running_a)
        .await
        .expect("A's cancel must end A's statement")
        .unwrap();
    assert_user_cancelled(result_a);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancel_request_while_idle_does_not_affect_the_next_statement() {
    let slow = Arc::new(Slow::default());
    let port = serve(slow).await;
    let client = connect(port).await;

    assert_eq!(show_timeout(&client).await, "0");
    client.cancel_token().cancel_query(NoTls).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The stale cancel hit no statement; the next one runs to completion.
    assert_eq!(show_timeout(&client).await, "0");
    client
        .simple_query("SET statement_timeout = '300ms'")
        .await
        .unwrap();
    let err = client.simple_query("SELECT 1").await.unwrap_err();
    assert!(
        err.as_db_error()
            .unwrap()
            .message()
            .contains("statement timeout"),
        "an idle cancel must not cancel a later statement: {err:?}"
    );
}
