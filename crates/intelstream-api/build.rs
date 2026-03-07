fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protobuf definitions when protoc is available.
    // Skip gracefully if protoc is not installed (uses stub types in grpc/mod.rs).
    if std::env::var("SKIP_PROTO_BUILD").is_ok() {
        println!("cargo:warning=Skipping protobuf compilation (SKIP_PROTO_BUILD set)");
        return Ok(());
    }

    match tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/grpc")
        .compile_protos(&["../../proto/intelstream.proto"], &["../../proto/"])
    {
        Ok(()) => {}
        Err(e) => {
            println!(
                "cargo:warning=Protobuf compilation skipped: {}. Using stub types.",
                e
            );
        }
    }
    Ok(())
}
