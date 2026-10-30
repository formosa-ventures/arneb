//! The agent query API service: an HTTP query API for AI agents on the
//! coordinator's Web port. Filled in by later tasks; for now it hosts the
//! configurable [`AgentLimits`].

pub mod limits;

pub use limits::AgentLimits;
