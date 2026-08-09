#![cfg_attr(feature = "in-rust-tree", feature(rustc_private))]
// `tests/lib_parity.rs` includes the same generated root module and *is* the
// `cfg(test)` build of this crate: the upstream `#[cfg(test)]` modules and our
// own unit tests (`shared_analyzer::tests`) compile and run there. A lib test
// target would be a second `cfg(test)` compilation of the identical tree, so it
// would only duplicate that suite. Keep the lib itself out of `cfg(test)` (see
// `test = false` in Cargo.toml) so there is exactly one of them.
#![cfg(not(test))]
#![allow(macro_expanded_macro_exports_accessed_by_absolute_paths)]
#![allow(unfulfilled_lint_expectations)]

extern crate self as ra_ap_rust_analyzer;

include!(concat!(
    env!("OUT_DIR"),
    "/ra_ap_rust_analyzer_bridge/src/root.rs"
));
