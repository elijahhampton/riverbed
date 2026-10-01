//! Generates the gRPC service code from `proto/service.proto` when the `grpc` feature is enabled.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&["proto/service.proto"], &["proto"])?;
    Ok(())
}
