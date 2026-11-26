//! Root-stage placement end to end: a coordinator and two workers run as
//! separate `arneb` processes, once with the root stage on a worker (the
//! default) and once with `ARNEB_INCLUDE_COORDINATOR=true`. A query whose
//! top is a sort, a window, an aggregate or a join returns the same rows
//! both ways, and its root work really moves (one more fragment); a plan
//! that scans in its root stays on the coordinator.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// A port that was free a moment ago.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A pgwire port whose Web UI port (`+ 1000`) is free too.
fn free_pg_port() -> u16 {
    loop {
        let port = free_port();
        if port < 60_000 && TcpListener::bind(("127.0.0.1", port + 1000)).is_ok() {
            return port;
        }
    }
}

/// The node processes; killed when dropped so a failed test leaves none.
struct Cluster {
    nodes: Vec<Child>,
    pg_port: u16,
    dir: tempfile::TempDir,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            let _ = node.kill();
            let _ = node.wait();
        }
    }
}

impl Cluster {
    fn logs(&self) -> String {
        let mut out = String::new();
        for name in ["coordinator", "worker-1", "worker-2"] {
            let log = std::fs::read_to_string(self.dir.path().join(format!("{name}.log")))
                .unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(20).collect();
            out.push_str(&format!("--- {name}\n"));
            for line in tail.into_iter().rev() {
                out.push_str(line);
                out.push('\n');
            }
        }
        out
    }
}

const TABLES: &[(&str, &str, &str)] = &[
    (
        "t",
        "k,g,v\n",
        r#"[{ name = "k", type = "int32" }, { name = "g", type = "int32" }, { name = "v", type = "int64" }]"#,
    ),
    (
        "d",
        "g,name\n",
        r#"[{ name = "g", type = "int32" }, { name = "name", type = "utf8" }]"#,
    ),
];

fn write_data(dir: &Path) {
    let mut t = String::from(TABLES[0].1);
    for k in 0..2_000 {
        t.push_str(&format!("{k},{},{}\n", k % 7, (k * 37) % 1_009));
    }
    std::fs::write(dir.join("t.csv"), t).unwrap();
    let mut d = String::from(TABLES[1].1);
    for g in 0..7 {
        d.push_str(&format!("{g},group-{g}\n"));
    }
    std::fs::write(dir.join("d.csv"), d).unwrap();
}

fn tables_toml(dir: &Path) -> String {
    TABLES
        .iter()
        .map(|(name, _, schema)| {
            format!(
                "[[tables]]\nname = \"{name}\"\npath = \"{}\"\nformat = \"csv\"\nschema = {schema}\n\n",
                dir.join(format!("{name}.csv")).display()
            )
        })
        .collect()
}

fn spawn(dir: &Path, name: &str, role: &str, include_coordinator: bool) -> Child {
    let log = std::fs::File::create(dir.join(format!("{name}.log"))).unwrap();
    Command::new(env!("CARGO_BIN_EXE_arneb"))
        .args(["--config"])
        .arg(dir.join(format!("{name}.toml")))
        .args(["--role", role, "--no-trino"])
        .env(
            "ARNEB_INCLUDE_COORDINATOR",
            if include_coordinator { "true" } else { "false" },
        )
        .env("RUST_LOG", "info")
        // The server logs to stdout.
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .expect("start arneb")
}

/// Active, alive workers the coordinator's Web UI reports.
fn active_workers(web_port: u16) -> usize {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", web_port)) else {
        return 0;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let request = "GET /api/v1/cluster/workers HTTP/1.0\r\nHost: localhost\r\n\r\n";
    if stream.write_all(request.as_bytes()).is_err() {
        return 0;
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    let Some((_, body)) = response.split_once("\r\n\r\n") else {
        return 0;
    };
    serde_json::from_str::<Vec<serde_json::Value>>(body)
        .map(|workers| {
            workers
                .iter()
                .filter(|w| w["state"] == "active" && w["alive"] == true)
                .count()
        })
        .unwrap_or(0)
}

fn start_cluster(include_coordinator: bool) -> Cluster {
    let dir = tempfile::tempdir().unwrap();
    write_data(dir.path());
    let tables = tables_toml(dir.path());
    let pg_port = free_pg_port();
    let coord_rpc = free_port();
    std::fs::write(
        dir.path().join("coordinator.toml"),
        format!(
            "bind_address = \"127.0.0.1\"\nport = {pg_port}\n\n[cluster]\nrole = \"coordinator\"\n\
             rpc_port = {coord_rpc}\n\n{tables}"
        ),
    )
    .unwrap();
    for n in 1..=2 {
        std::fs::write(
            dir.path().join(format!("worker-{n}.toml")),
            format!(
                "bind_address = \"127.0.0.1\"\n\n[cluster]\nrole = \"worker\"\nrpc_port = {}\n\
                 coordinator_address = \"127.0.0.1:{coord_rpc}\"\nworker_id = \"worker-{n}\"\n\n{tables}",
                free_port()
            ),
        )
        .unwrap();
    }
    let mut cluster = Cluster {
        nodes: Vec::new(),
        pg_port,
        dir,
    };
    let path = cluster.dir.path().to_path_buf();
    cluster.nodes.push(spawn(
        &path,
        "coordinator",
        "coordinator",
        include_coordinator,
    ));
    for n in 1..=2 {
        cluster.nodes.push(spawn(
            &path,
            &format!("worker-{n}"),
            "worker",
            include_coordinator,
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while active_workers(pg_port + 1000) < 2 {
        assert!(
            Instant::now() < deadline,
            "workers did not register\n{}",
            cluster.logs()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    cluster
}

/// Each query's rows, every value as text, in the order the server sent.
async fn run(cluster: &Cluster, queries: &[&str]) -> Vec<Vec<Vec<Option<String>>>> {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=arneb dbname=arneb",
            cluster.pg_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap_or_else(|e| panic!("connect: {e}\n{}", cluster.logs()));
    tokio::spawn(connection);
    let mut out = Vec::new();
    for sql in queries {
        let messages = client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}\n{}", cluster.logs()));
        let rows = messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .map(|i| row.get(i).map(str::to_string))
                        .collect(),
                ),
                _ => None,
            })
            .collect();
        out.push(rows);
    }
    out
}

/// The fragment count of each distributed query the coordinator started,
/// in order, from its "starting distributed execution" log lines.
fn fragment_counts(cluster: &Cluster) -> Vec<usize> {
    let log = std::fs::read_to_string(cluster.dir.path().join("coordinator.log")).unwrap();
    log.lines()
        .filter(|line| line.contains("starting distributed execution"))
        .filter_map(|line| {
            let plain = strip_ansi(line);
            let (_, rest) = plain.split_once("fragments=")?;
            rest.split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        })
        .collect()
}

fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // CSI: ESC [ ... final byte in @..~
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) && c != '[' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The queries, and whether the root stage moves to a worker when allowed.
const QUERIES: &[(&str, bool)] = &[
    // Sort root.
    ("SELECT k, v FROM t ORDER BY v DESC, k LIMIT 50", true),
    // Window root.
    (
        "SELECT k, g, row_number() OVER (PARTITION BY g ORDER BY v, k) AS rn \
         FROM t ORDER BY g, rn LIMIT 60",
        true,
    ),
    // Aggregate root.
    (
        "SELECT g, count(*), sum(v), min(v), max(v) FROM t GROUP BY g ORDER BY g",
        true,
    ),
    // Join root.
    (
        "SELECT d.name, count(*) FROM t JOIN d ON t.g = d.g GROUP BY d.name ORDER BY d.name",
        true,
    ),
    // A plan that scans in its root fragment stays on the coordinator:
    // set operations over these small tables plan as one fragment.
    (
        "SELECT g FROM t WHERE v < 100 UNION SELECT g FROM d WHERE g > 3 ORDER BY g",
        false,
    ),
];

#[tokio::test(flavor = "multi_thread")]
async fn the_root_stage_returns_the_same_rows_on_a_worker_and_on_the_coordinator() {
    let sql: Vec<&str> = QUERIES.iter().map(|(sql, _)| *sql).collect();
    let offloaded = start_cluster(false);
    let on_worker = run(&offloaded, &sql).await;
    let offloaded_fragments = fragment_counts(&offloaded);
    drop(offloaded);

    let kept = start_cluster(true);
    let on_coordinator = run(&kept, &sql).await;
    let kept_fragments = fragment_counts(&kept);
    drop(kept);

    for ((sql, a), b) in sql.iter().zip(&on_worker).zip(&on_coordinator) {
        assert!(!a.is_empty(), "{sql}: no rows");
        assert_eq!(a, b, "{sql}");
    }
    // The coordinator started every query both ways. A root stage that
    // moves to a worker adds one fragment: the single-task stage that does
    // the root's work, under the coordinator's bare exchange.
    assert_eq!(
        offloaded_fragments.len(),
        QUERIES.len(),
        "{offloaded_fragments:?}"
    );
    assert_eq!(kept_fragments.len(), QUERIES.len(), "{kept_fragments:?}");
    for (((sql, moves), with), without) in QUERIES
        .iter()
        .zip(&offloaded_fragments)
        .zip(&kept_fragments)
    {
        let expected = if *moves { without + 1 } else { *without };
        assert_eq!(
            *with, expected,
            "{sql}: {offloaded_fragments:?} with the root on a worker, \
             {kept_fragments:?} on the coordinator"
        );
    }
}
