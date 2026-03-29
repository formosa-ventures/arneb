mod encoding;
mod error;
mod handler;
mod metadata;
mod server;
mod session;

pub use server::{ProtocolConfig, ProtocolServer};

// Re-export the distributed executor trait for server crate to implement
pub use handler::DistributedExecutor;

// Re-export for integration testing
#[doc(hidden)]
pub mod __private {
    pub use crate::handler::HandlerFactory;
}
