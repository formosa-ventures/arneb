//! Confirms that custom `target: "arneb::xxx"` events emitted from
//! `arneb_execution` reach a subscriber configured with the same default
//! filter the server uses (`EnvFilter::new("info")`).
//!
//! Background: a prior session observed that
//! `tracing::info!(target: "arneb::mem", ...)` events from this crate
//! never appeared in worker logs. This test isolates the library-level
//! behaviour from the runtime/docker environment.

use std::io;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn custom_target_events_reach_default_env_filter_subscriber() {
    let capture = Capture::default();
    let capture_for_writer = capture.clone();
    let make_writer = move || capture_for_writer.clone();

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .with_writer(make_writer)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "arneb::mem", marker = 1, "custom-target event");
        tracing::info!(marker = 2, "default-target event");
    });

    let buf = capture.0.lock().unwrap();
    let log = String::from_utf8_lossy(&buf);
    assert!(
        log.contains("arneb::mem"),
        "custom-target event missing from output: {log:?}"
    );
    assert!(
        log.contains("custom-target event"),
        "custom-target message missing from output: {log:?}"
    );
    assert!(
        log.contains("default-target event"),
        "default-target message missing from output: {log:?}"
    );
}
