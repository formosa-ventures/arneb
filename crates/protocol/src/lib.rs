mod encoding;
mod error;
mod handler;
mod server;
mod session;

pub use server::{ProtocolConfig, ProtocolServer};

// Re-export for integration testing
#[doc(hidden)]
pub mod __private {
    pub use crate::handler::HandlerFactory;
}
