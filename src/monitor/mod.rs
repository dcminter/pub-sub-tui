//! The monitor↔UI boundary.
//!
//! The headless service [`serve`]s observed state over the `monitor.v1.Monitor`
//! gRPC service; a UI [`stream`]s it back into a local [`watch`](tokio::sync::watch)
//! channel. [`convert`] mirrors [`AppState`](crate::observe::AppState) to and from
//! the wire types, encoding publisher liveness in a clock-independent way.

mod client;
mod convert;
pub mod proto;
mod server;

pub use client::stream;
pub use convert::{from_wire, to_wire};
pub use server::serve;
