//! Generated gRPC bindings for the `monitor.v1` wire protocol.
//!
//! Wraps the code emitted by `build.rs` (both the `Monitor` server trait the
//! headless service implements and the client stub the UI dials with). As with
//! `crate::pb`, generated code does not satisfy this crate's strict lint policy,
//! so all warnings are silenced for this module only.
#![allow(warnings)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]
#![allow(clippy::nursery)]

tonic::include_proto!("monitor.v1");
