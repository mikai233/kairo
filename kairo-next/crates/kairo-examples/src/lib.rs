//! Runnable example targets for the Kairo rewrite.
//!
//! The example implementations live under `examples/` so they can be run with
//! `cargo run -p kairo-examples --example <name>`.

pub mod cluster_tools_tcp;
pub mod counter;
pub mod ddata_tcp;
pub mod patterns;
pub mod reply;
pub mod sharding_local;
