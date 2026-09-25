//! Scalar function registry and built-in function implementations.

mod args;
mod conditional;
mod date;
mod math;
mod regexp;
mod registry;
mod string;

pub use registry::{default_registry, FunctionRegistry, ScalarFunction};
