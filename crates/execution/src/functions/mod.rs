//! Scalar function registry and built-in function implementations.

mod date;
mod math;
mod registry;
mod string;

pub use registry::{default_registry, FunctionRegistry, ScalarFunction};
