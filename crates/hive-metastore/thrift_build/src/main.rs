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

use std::path::PathBuf;

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

    volo_build::Builder::thrift()
        .filename("hms.rs".into())
        .add_service(&thrift_file)
        .include_dirs(vec![idl_dir.clone()])
        .out_dir(out_dir.clone())
        .write()
        .expect("volo-build failed to generate Thrift bindings");

    println!("Generated {}/hms.rs", out_dir.display());
}
