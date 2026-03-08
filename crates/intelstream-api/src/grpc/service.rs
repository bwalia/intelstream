//! gRPC service implementations backed by the core broker.

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use intelstream_core::error::IntelStreamError;
use intelstream_core::message::Message as CoreMessage;
use intelstream_core::topic::TopicConfig;

use super::proto;
use super::{ClusterService, IntelStream, TopicService};
use crate::AppState;

/// Maps an IntelStreamError to a tonic Status.
fn to_status(err: IntelStreamError) -> Status {
    match &err {
        IntelStreamError::TopicNotFound(_) | IntelStreamError::PartitionNotFound { .. } => {
            Status::not_found(err.to_string())
        }
        IntelStreamError::TopicAlreadyExists(_) => Status::already_exists(err.to_string()),
        IntelStreamError::MessageTooLarge { .. }
        | IntelStreamError::InvalidPartitionCount(_)
        | IntelStreamError::InvalidMessage(_)
        | IntelStreamError::OffsetOutOfRange { .. } => Status::invalid_argument(err.to_string()),
        IntelStreamError::NotLeader(_) | IntelStreamError::ElectionInProgress => {
            Status::unavailable(err.to_string())
        }
        _ => Status::internal(err.to_string()),
    }
}

/// The IntelStream gRPC service implementation (produce/fetch/consume/commit).
pub struct IntelStreamGrpcService {
    state: Arc<AppState>,
}

impl IntelStreamGrpcService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl IntelStream for IntelStreamGrpcService {
    async fn produce(
        &self,
        request: Request<proto::ProduceRequest>,
    ) -> Result<Response<proto::ProduceResponse>, Status> {
        let req = request.into_inner();

        let key = if req.key.is_empty() {
            None
        } else {
            Some(Bytes::from(req.key))
        };
        let value = Bytes::from(req.value);
        let mut message = CoreMessage::new(key, value);

        for h in &req.headers {
            message.headers.insert(h.key.clone(), h.value.clone());
        }

        let offset = self
            .state
            .broker
            .produce(&req.topic, req.partition as u32, message)
            .map_err(to_status)?;

        Ok(Response::new(proto::ProduceResponse {
            topic: req.topic,
            partition: req.partition,
            offset,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }))
    }

    async fn fetch(
        &self,
        request: Request<proto::FetchRequest>,
    ) -> Result<Response<proto::FetchResponse>, Status> {
        let req = request.into_inner();
        let max_messages = if req.max_messages <= 0 {
            100
        } else {
            req.max_messages as u32
        };

        let records = self
            .state
            .broker
            .consume(
                &req.topic,
                req.partition as u32,
                Some(req.offset),
                max_messages,
                None,
            )
            .map_err(to_status)?;

        let messages: Vec<proto::Message> = records
            .into_iter()
            .map(|(header, msg)| proto::Message {
                offset: header.offset,
                key: msg.key.map(|k| k.to_vec()).unwrap_or_default(),
                value: msg.value.to_vec(),
                timestamp: header.append_timestamp.timestamp_millis(),
                headers: msg
                    .headers
                    .iter()
                    .map(|(k, v)| proto::MessageHeader {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect(),
                schema_id: msg.schema_id.unwrap_or(0),
            })
            .collect();

        Ok(Response::new(proto::FetchResponse {
            topic: req.topic,
            partition: req.partition,
            high_watermark: 0,
            log_end_offset: 0,
            messages,
        }))
    }

    type ConsumeStream =
        Pin<Box<dyn Stream<Item = Result<proto::ConsumeResponse, Status>> + Send + 'static>>;

    async fn consume(
        &self,
        request: Request<proto::ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let req = request.into_inner();
        let state = self.state.clone();

        // One-shot fetch wrapped as a stream (real streaming will be added later)
        let stream = async_stream::try_stream! {
            let group_id = if req.group_id.is_empty() {
                None
            } else {
                Some(req.group_id.as_str())
            };

            let records = state
                .broker
                .consume(
                    &req.topic,
                    req.partition as u32,
                    Some(req.start_offset),
                    100,
                    group_id,
                )
                .map_err(to_status)?;

            for (header, msg) in records {
                yield proto::ConsumeResponse {
                    message: Some(proto::Message {
                        offset: header.offset,
                        key: msg.key.map(|k| k.to_vec()).unwrap_or_default(),
                        value: msg.value.to_vec(),
                        timestamp: header.append_timestamp.timestamp_millis(),
                        headers: msg
                            .headers
                            .iter()
                            .map(|(k, v)| proto::MessageHeader {
                                key: k.clone(),
                                value: v.clone(),
                            })
                            .collect(),
                        schema_id: msg.schema_id.unwrap_or(0),
                    }),
                };
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::ConsumeStream))
    }

    async fn commit_offset(
        &self,
        request: Request<proto::CommitOffsetRequest>,
    ) -> Result<Response<proto::CommitOffsetResponse>, Status> {
        let req = request.into_inner();

        self.state
            .broker
            .commit_offset(&req.topic, req.partition as u32, &req.group_id, req.offset)
            .map_err(to_status)?;

        Ok(Response::new(proto::CommitOffsetResponse { success: true }))
    }
}

/// Topic management gRPC service.
pub struct TopicGrpcService {
    state: Arc<AppState>,
}

impl TopicGrpcService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl TopicService for TopicGrpcService {
    async fn create_topic(
        &self,
        request: Request<proto::CreateTopicRequest>,
    ) -> Result<Response<proto::CreateTopicResponse>, Status> {
        let req = request.into_inner();
        let mut config = TopicConfig::new(
            &req.name,
            req.partition_count as u32,
            req.replication_factor as u32,
        );
        config.compact = req.compact;

        self.state
            .broker
            .create_topic(config)
            .await
            .map_err(to_status)?;

        Ok(Response::new(proto::CreateTopicResponse {
            name: req.name,
            success: true,
        }))
    }

    async fn delete_topic(
        &self,
        request: Request<proto::DeleteTopicRequest>,
    ) -> Result<Response<proto::DeleteTopicResponse>, Status> {
        let req = request.into_inner();
        self.state
            .broker
            .delete_topic(&req.name)
            .await
            .map_err(to_status)?;

        Ok(Response::new(proto::DeleteTopicResponse { success: true }))
    }

    async fn get_topic(
        &self,
        request: Request<proto::GetTopicRequest>,
    ) -> Result<Response<proto::GetTopicResponse>, Status> {
        let req = request.into_inner();
        let meta = self
            .state
            .broker
            .get_topic(&req.name)
            .await
            .map_err(to_status)?;

        Ok(Response::new(proto::GetTopicResponse {
            name: meta.config.name,
            partition_count: meta.config.partition_count as i32,
            replication_factor: meta.config.replication_factor as i32,
            retention_ms: meta.config.retention.max_age_ms.unwrap_or(0) as i64,
            compact: meta.config.compact,
            partitions: vec![],
        }))
    }

    async fn list_topics(
        &self,
        _request: Request<proto::ListTopicsRequest>,
    ) -> Result<Response<proto::ListTopicsResponse>, Status> {
        let topic_names = self.state.broker.list_topics().await;
        let mut topics = Vec::with_capacity(topic_names.len());

        for name in &topic_names {
            if let Ok(meta) = self.state.broker.get_topic(name).await {
                topics.push(proto::GetTopicResponse {
                    name: meta.config.name,
                    partition_count: meta.config.partition_count as i32,
                    replication_factor: meta.config.replication_factor as i32,
                    retention_ms: meta.config.retention.max_age_ms.unwrap_or(0) as i64,
                    compact: meta.config.compact,
                    partitions: vec![],
                });
            }
        }

        Ok(Response::new(proto::ListTopicsResponse { topics }))
    }
}

/// Cluster management gRPC service.
pub struct ClusterGrpcService {
    state: Arc<AppState>,
}

impl ClusterGrpcService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl ClusterService for ClusterGrpcService {
    async fn get_cluster_status(
        &self,
        _request: Request<proto::GetClusterStatusRequest>,
    ) -> Result<Response<proto::GetClusterStatusResponse>, Status> {
        let broker_status = self.state.broker.status().await;
        let topic_names = self.state.broker.list_topics().await;
        let partition_count = self.state.broker.partition_count() as i32;
        let config = self.state.broker.config();

        let broker_info = proto::BrokerInfo {
            id: config.id as i32,
            host: config.host.clone(),
            port: config.port as i32,
            status: format!("{:?}", broker_status),
            partitions_led: partition_count,
            partitions_followed: 0,
        };

        Ok(Response::new(proto::GetClusterStatusResponse {
            cluster_name: "intelstream-default".to_string(),
            controller_id: config.id as i32,
            brokers: vec![broker_info],
            total_topics: topic_names.len() as i32,
            total_partitions: partition_count,
        }))
    }

    async fn get_broker_info(
        &self,
        _request: Request<proto::GetBrokerInfoRequest>,
    ) -> Result<Response<proto::GetBrokerInfoResponse>, Status> {
        let broker_status = self.state.broker.status().await;
        let partition_count = self.state.broker.partition_count() as i32;
        let config = self.state.broker.config();

        Ok(Response::new(proto::GetBrokerInfoResponse {
            broker: Some(proto::BrokerInfo {
                id: config.id as i32,
                host: config.host.clone(),
                port: config.port as i32,
                status: format!("{:?}", broker_status),
                partitions_led: partition_count,
                partitions_followed: 0,
            }),
        }))
    }
}
