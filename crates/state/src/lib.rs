//! Durable coordinator state (users, API tokens) kept in one atomic,
//! versioned JSON file. Only the coordinator / standalone server opens it.

mod file;
mod users;

pub use file::{StateError, StateStore};
pub use users::SeedReport;
