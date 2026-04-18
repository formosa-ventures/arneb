//! Thrift code generator for the `hive-metastore` crate.
//!
//! This binary runs `volo-build` to regenerate `crates/hive-metastore/src/hms.rs`
//! from the Thrift IDL files in `crates/hive-metastore/thrift_idl/`.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p hive-metastore-thrift-build
//! ```
//!
//! The generated file is committed to the repo — `cargo build` does not
//! invoke this generator automatically.

use std::fs;
use std::path::PathBuf;

/// Prepended to the generated file so `cargo fmt` leaves it alone. Without
/// this marker, rustfmt rewrites volo-build's output on every run and
/// produces a ~3k-line diff that drowns real changes.
///
/// Uses the outer `#[rustfmt::skip]` attribute — the inner form
/// (`#![rustfmt::skip]`) requires the unstable `custom_inner_attributes`
/// feature (rust-lang/rust#54726). volo-build emits the whole file as one
/// top-level `pub mod hms { ... }`, so attaching the outer attribute to
/// that module is equivalent to skipping the whole file.
const RUSTFMT_SKIP_HEADER: &str = "#[rustfmt::skip]\n";

fn main() {
    // `CARGO_MANIFEST_DIR` points to `crates/hive-metastore/thrift_build/`.
    // Compute absolute paths to the IDL dir and the sibling `src/` output dir.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let crate_root = manifest_dir
        .parent()
        .expect("thrift_build must live under crates/hive-metastore/");
    let idl_dir = crate_root.join("thrift_idl");
    let out_dir = crate_root.join("src");
    let thrift_file = idl_dir.join("hive_metastore.thrift");
    let output_path = out_dir.join("hms.rs");

    volo_build::Builder::thrift()
        .filename("hms.rs".into())
        .add_service(&thrift_file)
        .include_dirs(vec![idl_dir.clone()])
        .out_dir(out_dir.clone())
        .write()
        .expect("volo-build failed to generate Thrift bindings");

    // Make the generated file idempotent under `cargo fmt` by prepending an
    // inner rustfmt::skip attribute. Re-run is a no-op if the header is
    // already present.
    let body = fs::read_to_string(&output_path).expect("failed to read freshly generated hms.rs");
    if !body.starts_with(RUSTFMT_SKIP_HEADER) {
        let mut new_body = String::with_capacity(body.len() + RUSTFMT_SKIP_HEADER.len());
        new_body.push_str(RUSTFMT_SKIP_HEADER);
        new_body.push_str(&body);
        fs::write(&output_path, new_body).expect("failed to rewrite hms.rs with header");
    }

    println!("Generated {}", output_path.display());
}
