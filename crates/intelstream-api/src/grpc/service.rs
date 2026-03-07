//! gRPC service implementations.
//!
//! These are stub implementations that will be connected to the broker
//! once the protobuf codegen pipeline is fully wired up.

/// The IntelStream gRPC service implementation.
/// TODO: Implement the tonic `IntelStream` trait once proto is compiled.
pub struct IntelStreamGrpcService {
    // TODO: hold an Arc<Broker> reference
}

impl IntelStreamGrpcService {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for IntelStreamGrpcService {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: Implement the generated tonic service trait:
//
// #[tonic::async_trait]
// impl intelstream_server::IntelStream for IntelStreamGrpcService {
//     async fn produce(
//         &self,
//         request: tonic::Request<ProduceRequest>,
//     ) -> Result<tonic::Response<ProduceResponse>, tonic::Status> {
//         ...
//     }
//
//     async fn fetch(
//         &self,
//         request: tonic::Request<FetchRequest>,
//     ) -> Result<tonic::Response<FetchResponse>, tonic::Status> {
//         ...
//     }
//
//     type ConsumeStream = ...;
//     async fn consume(
//         &self,
//         request: tonic::Request<ConsumeRequest>,
//     ) -> Result<tonic::Response<Self::ConsumeStream>, tonic::Status> {
//         // server-streaming for real-time consumption
//         ...
//     }
// }
