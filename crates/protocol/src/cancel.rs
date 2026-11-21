//! pgwire CancelRequest support.
//!
//! Each connection gets its own backend key (process ID + random secret) at
//! startup, sent to the client in `BackendKeyData` and registered in a
//! shared [`ConnectionManager`]. A client cancels a statement by opening a
//! second connection that carries only that key (psql's Ctrl-C, JDBC's
//! `Statement.cancel`); pgwire hands it to [`DefaultCancelHandler`], which
//! fires the connection's [`ConnectionHandle`]. pgwire's simple and
//! extended query paths race each statement against that handle (it finds
//! it in the session extensions) and, when it fires, drop the statement's
//! future, which cancels the query on its workers, exactly as
//! `statement_timeout` does, and answer `ERROR 57014`.
//!
//! Only pgwire may arm the handle (`ConnectionHandle::start_query`): arming
//! it again from Arneb's handler would replace pgwire's signal, and pgwire
//! treats a replaced signal as a cancel.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use pgwire::api::cancel::DefaultCancelHandler;
use pgwire::api::{ClientInfo, ConnectionHandle, ConnectionManager};
use pgwire::messages::startup::SecretKey;

/// First process ID handed out, as pgwire's own generator does.
const FIRST_PID: i32 = 100_000;

/// Backend keys for every pgwire connection of one server.
#[derive(Debug)]
pub struct BackendKeys {
    next_pid: AtomicI32,
    connections: Arc<ConnectionManager>,
}

impl Default for BackendKeys {
    fn default() -> Self {
        Self {
            next_pid: AtomicI32::new(FIRST_PID),
            connections: Arc::default(),
        }
    }
}

impl BackendKeys {
    /// Give `client` a fresh backend key and register it for cancellation.
    /// Call before `finish_authentication`, which sends the key to the
    /// client. The registration lives in the connection's session
    /// extensions and ends with the connection.
    pub(crate) fn assign<C: ClientInfo>(&self, client: &mut C) {
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed);
        // A 4-byte secret: the only size protocol 3.0 allows, and valid in
        // 3.2. Arneb does not negotiate the protocol version, so pgwire's
        // generator, which reads the client's (default 3.2) version, would
        // send a 32-byte key that 3.0 clients reject at connect.
        let mut secret = [0u8; 4];
        aws_lc_rs::rand::fill(&mut secret).expect("system random number generator failed");
        let secret_key = SecretKey::I32(i32::from_be_bytes(secret));
        client.set_pid_and_secret_key(pid, secret_key.clone());
        let (handle, guard) = self.connections.register(pid, secret_key);
        let extensions = client.session_extensions();
        extensions.insert::<Arc<ConnectionHandle>>(handle);
        extensions.insert(guard);
    }

    /// The handler pgwire calls with each CancelRequest.
    pub(crate) fn cancel_handler(&self) -> DefaultCancelHandler {
        DefaultCancelHandler::new(Arc::clone(&self.connections))
    }
}
