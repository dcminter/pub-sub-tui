//! Build script: compiles the vendored `google.pubsub.v1` protos into both server
//! traits (for the interception proxy) and client stubs (for forwarding upstream).
//!
//! `protoc` is not assumed to be installed on the host; the `protoc-bin-vendored`
//! crate supplies a pinned binary and the bundled well-known-type includes.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build scripts are single-threaded; nothing else reads the env here.
    unsafe {
        std::env::set_var("PROTOC", &protoc);
    }

    let well_known = protoc_bin_vendored::include_path()?;
    let includes: Vec<PathBuf> = vec![PathBuf::from("proto"), well_known];

    let protos: Vec<PathBuf> = vec![
        PathBuf::from("proto/google/pubsub/v1/pubsub.proto"),
        // The monitor↔UI wire protocol (see src/monitor/).
        PathBuf::from("proto/monitor/v1/monitor.proto"),
    ];

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes(".")
        .compile_protos(&protos, &includes)?;

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    println!("cargo:rerun-if-changed=proto");

    Ok(())
}
