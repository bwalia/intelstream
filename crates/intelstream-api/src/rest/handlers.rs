//! REST API request handlers.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use bytes::Bytes;
use tracing::info;

use intelstream_core::error::IntelStreamError;
use intelstream_core::message::Message;
use intelstream_core::topic::TopicConfig;

use super::models::*;
use crate::{ApiResponse, AppState};

/// Map an IntelStreamError to an HTTP status code.
fn error_status(err: &IntelStreamError) -> StatusCode {
    match err {
        IntelStreamError::TopicNotFound(_) | IntelStreamError::PartitionNotFound { .. } => {
            StatusCode::NOT_FOUND
        }
        IntelStreamError::TopicAlreadyExists(_) => StatusCode::CONFLICT,
        IntelStreamError::MessageTooLarge { .. }
        | IntelStreamError::InvalidPartitionCount(_)
        | IntelStreamError::InvalidMessage(_)
        | IntelStreamError::OffsetOutOfRange { .. } => StatusCode::BAD_REQUEST,
        IntelStreamError::NotLeader(_) | IntelStreamError::ElectionInProgress => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Map an IntelStreamError to an error code string.
fn error_code(err: &IntelStreamError) -> &'static str {
    match err {
        IntelStreamError::TopicNotFound(_) => "TOPIC_NOT_FOUND",
        IntelStreamError::TopicAlreadyExists(_) => "TOPIC_ALREADY_EXISTS",
        IntelStreamError::PartitionNotFound { .. } => "PARTITION_NOT_FOUND",
        IntelStreamError::MessageTooLarge { .. } => "MESSAGE_TOO_LARGE",
        IntelStreamError::InvalidPartitionCount(_) => "INVALID_PARTITION_COUNT",
        IntelStreamError::InvalidMessage(_) => "INVALID_MESSAGE",
        IntelStreamError::OffsetOutOfRange { .. } => "OFFSET_OUT_OF_RANGE",
        IntelStreamError::NotLeader(_) => "NOT_LEADER",
        IntelStreamError::ElectionInProgress => "ELECTION_IN_PROGRESS",
        _ => "INTERNAL_ERROR",
    }
}

/// Health check endpoint.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthResponse)
    ),
    tag = "health"
)]
pub async fn health_check(State(state): State<Arc<AppState>>) -> Json<ApiResponse<HealthResponse>> {
    let uptime = state.started_at.elapsed().as_secs();
    Json(ApiResponse::ok(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        broker_id: state.broker.id(),
        uptime_secs: uptime,
    }))
}

/// List all topics.
#[utoipa::path(
    get,
    path = "/api/v1/topics",
    responses(
        (status = 200, description = "List of topics", body = Vec<TopicResponse>)
    ),
    tag = "topics"
)]
pub async fn list_topics(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<TopicResponse>>> {
    let topic_names = state.broker.list_topics().await;
    let mut topics = Vec::with_capacity(topic_names.len());

    for name in &topic_names {
        if let Ok(meta) = state.broker.get_topic(name).await {
            topics.push(TopicResponse {
                name: meta.config.name.clone(),
                partition_count: meta.config.partition_count,
                replication_factor: meta.config.replication_factor,
                retention_hours: meta
                    .config
                    .retention
                    .max_age_ms
                    .map(|ms| ms / 3_600_000)
                    .unwrap_or(168),
                compact: meta.config.compact,
                created_at: meta.created_at.to_rfc3339(),
            });
        }
    }

    Json(ApiResponse::ok(topics))
}

/// Create a new topic.
#[utoipa::path(
    post,
    path = "/api/v1/topics",
    request_body = TopicCreateRequest,
    responses(
        (status = 201, description = "Topic created", body = TopicResponse),
        (status = 409, description = "Topic already exists")
    ),
    tag = "topics"
)]
pub async fn create_topic(
    State(state): State<Arc<AppState>>,
    Json(request): Json<TopicCreateRequest>,
) -> (StatusCode, Json<ApiResponse<TopicResponse>>) {
    info!(topic = %request.name, partitions = request.partition_count, "Creating topic");

    let mut config = TopicConfig::new(
        &request.name,
        request.partition_count,
        request.replication_factor,
    );
    config.compact = request.compact;

    match state.broker.create_topic(config).await {
        Ok(meta) => {
            let response = TopicResponse {
                name: meta.config.name.clone(),
                partition_count: meta.config.partition_count,
                replication_factor: meta.config.replication_factor,
                retention_hours: meta
                    .config
                    .retention
                    .max_age_ms
                    .map(|ms| ms / 3_600_000)
                    .unwrap_or(168),
                compact: meta.config.compact,
                created_at: meta.created_at.to_rfc3339(),
            };
            (StatusCode::CREATED, Json(ApiResponse::ok(response)))
        }
        Err(e) => (
            error_status(&e),
            Json(ApiResponse::err(error_code(&e), e.to_string())),
        ),
    }
}

/// Get topic details.
#[utoipa::path(
    get,
    path = "/api/v1/topics/{name}",
    params(
        ("name" = String, Path, description = "Topic name")
    ),
    responses(
        (status = 200, description = "Topic details", body = TopicResponse),
        (status = 404, description = "Topic not found")
    ),
    tag = "topics"
)]
pub async fn get_topic(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> (StatusCode, Json<ApiResponse<TopicResponse>>) {
    match state.broker.get_topic(&name).await {
        Ok(meta) => {
            let response = TopicResponse {
                name: meta.config.name.clone(),
                partition_count: meta.config.partition_count,
                replication_factor: meta.config.replication_factor,
                retention_hours: meta
                    .config
                    .retention
                    .max_age_ms
                    .map(|ms| ms / 3_600_000)
                    .unwrap_or(168),
                compact: meta.config.compact,
                created_at: meta.created_at.to_rfc3339(),
            };
            (StatusCode::OK, Json(ApiResponse::ok(response)))
        }
        Err(e) => (
            error_status(&e),
            Json(ApiResponse::err(error_code(&e), e.to_string())),
        ),
    }
}

/// Delete a topic.
#[utoipa::path(
    delete,
    path = "/api/v1/topics/{name}",
    params(
        ("name" = String, Path, description = "Topic name")
    ),
    responses(
        (status = 204, description = "Topic deleted"),
        (status = 404, description = "Topic not found")
    ),
    tag = "topics"
)]
pub async fn delete_topic(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> StatusCode {
    info!(topic = %name, "Deleting topic");
    match state.broker.delete_topic(&name).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(e) => error_status(&e),
    }
}

/// Produce a message to a topic-partition.
#[utoipa::path(
    post,
    path = "/api/v1/topics/{name}/partitions/{partition}/messages",
    params(
        ("name" = String, Path, description = "Topic name"),
        ("partition" = u32, Path, description = "Partition ID")
    ),
    request_body = ProduceRequest,
    responses(
        (status = 200, description = "Message produced", body = ProduceResponse),
        (status = 404, description = "Topic or partition not found")
    ),
    tag = "messages"
)]
pub async fn produce_message(
    State(state): State<Arc<AppState>>,
    Path((name, partition)): Path<(String, u32)>,
    Json(request): Json<ProduceRequest>,
) -> (StatusCode, Json<ApiResponse<ProduceResponse>>) {
    info!(topic = %name, partition, "Producing message");

    let key = request.key.map(Bytes::from);
    let value = Bytes::from(request.value);
    let mut message = Message::new(key, value);

    // Copy request headers into the message
    for (k, v) in request.headers {
        message.headers.insert(k, v);
    }

    match state.broker.produce(&name, partition, message) {
        Ok(offset) => {
            let response = ProduceResponse {
                topic: name,
                partition,
                offset,
                timestamp: chrono::Utc::now().to_rfc3339(),
            };
            (StatusCode::OK, Json(ApiResponse::ok(response)))
        }
        Err(e) => (
            error_status(&e),
            Json(ApiResponse::err(error_code(&e), e.to_string())),
        ),
    }
}

/// Consume messages from a topic-partition.
#[utoipa::path(
    get,
    path = "/api/v1/topics/{name}/partitions/{partition}/messages",
    params(
        ("name" = String, Path, description = "Topic name"),
        ("partition" = u32, Path, description = "Partition ID"),
        ("offset" = Option<u64>, Query, description = "Start offset"),
        ("max_messages" = Option<u32>, Query, description = "Max messages to return"),
        ("group_id" = Option<String>, Query, description = "Consumer group ID")
    ),
    responses(
        (status = 200, description = "Consumed messages", body = ConsumeResponse)
    ),
    tag = "messages"
)]
pub async fn consume_messages(
    State(state): State<Arc<AppState>>,
    Path((name, partition)): Path<(String, u32)>,
    Query(params): Query<ConsumeRequest>,
) -> (StatusCode, Json<ApiResponse<ConsumeResponse>>) {
    info!(topic = %name, partition, offset = ?params.offset, "Consuming messages");

    let max_messages = if params.max_messages == 0 {
        100
    } else {
        params.max_messages
    };
    let group_id_ref = params.group_id.as_deref();

    match state
        .broker
        .consume(&name, partition, params.offset, max_messages, group_id_ref)
    {
        Ok(records) => {
            let next_offset = records
                .last()
                .map(|(h, _)| h.offset + 1)
                .unwrap_or(params.offset.unwrap_or(0));

            let messages: Vec<MessageResponse> = records
                .into_iter()
                .map(|(header, msg)| MessageResponse {
                    offset: header.offset,
                    key: msg
                        .key
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).to_string()),
                    value: String::from_utf8_lossy(&msg.value).to_string(),
                    headers: msg.headers.clone(),
                    timestamp: header.append_timestamp.to_rfc3339(),
                })
                .collect();

            let response = ConsumeResponse {
                topic: name,
                partition,
                messages,
                next_offset,
            };
            (StatusCode::OK, Json(ApiResponse::ok(response)))
        }
        Err(e) => (
            error_status(&e),
            Json(ApiResponse::err(error_code(&e), e.to_string())),
        ),
    }
}

/// Get cluster status.
#[utoipa::path(
    get,
    path = "/api/v1/cluster/status",
    responses(
        (status = 200, description = "Cluster status", body = ClusterStatusResponse)
    ),
    tag = "cluster"
)]
pub async fn get_cluster_status(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<ClusterStatusResponse>> {
    let broker_status = state.broker.status().await;
    let topic_names = state.broker.list_topics().await;
    let partition_count = state.broker.partition_count() as u32;
    let config = state.broker.config();

    let broker_info = BrokerInfo {
        id: config.id,
        host: config.host.clone(),
        port: config.port,
        status: format!("{:?}", broker_status),
        partitions_led: partition_count,
        partitions_followed: 0,
    };

    Json(ApiResponse::ok(ClusterStatusResponse {
        cluster_name: "intelstream-default".to_string(),
        brokers: vec![broker_info],
        total_topics: topic_names.len() as u32,
        total_partitions: partition_count,
        controller_id: config.id,
    }))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use intelstream_core::broker::{Broker, BrokerConfig};
    use intelstream_core::storage::StorageConfig;

    use crate::rest::build_router;
    use crate::AppState;

    /// Create a test AppState with a real broker backed by a temp directory.
    fn test_app(dir: &Path) -> axum::Router {
        let config = BrokerConfig {
            id: 1,
            host: "127.0.0.1".to_string(),
            port: 9292,
            data_dir: dir.to_string_lossy().to_string(),
            storage: StorageConfig::default(),
        };
        let broker = Broker::new(config).unwrap();
        let state = Arc::new(AppState::new(Arc::new(broker)));
        build_router(state)
    }

    use std::sync::Arc;

    #[tokio::test]
    async fn test_health_check() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["success"].as_bool().unwrap());
        assert_eq!(json["data"]["broker_id"], 1);
    }

    #[tokio::test]
    async fn test_list_topics_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/topics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["data"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_topic_returns_201() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"test-topic","partition_count":3,"replication_factor":1}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["success"].as_bool().unwrap());
        assert_eq!(json["data"]["name"], "test-topic");
        assert_eq!(json["data"]["partition_count"], 3);
    }

    #[tokio::test]
    async fn test_create_duplicate_topic_returns_409() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = BrokerConfig {
            id: 1,
            host: "127.0.0.1".to_string(),
            port: 9292,
            data_dir: tmp.path().to_string_lossy().to_string(),
            storage: StorageConfig::default(),
        };
        let broker = Arc::new(Broker::new(config).unwrap());

        // Create topic directly on broker first
        let topic_config = intelstream_core::topic::TopicConfig::new("dup-topic", 1, 1);
        broker.create_topic(topic_config).await.unwrap();

        let state = Arc::new(AppState::new(broker));
        let app = build_router(state);

        // Try to create the same topic via REST
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"dup-topic","partition_count":1,"replication_factor":1}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_create_topic_invalid_json_returns_error() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics")
                    .header("content-type", "application/json")
                    .body(Body::from("NOT_JSON"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_client_error());
    }

    #[tokio::test]
    async fn test_get_topic_not_found() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/topics/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_topic_returns_204() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = BrokerConfig {
            id: 1,
            host: "127.0.0.1".to_string(),
            port: 9292,
            data_dir: tmp.path().to_string_lossy().to_string(),
            storage: StorageConfig::default(),
        };
        let broker = Arc::new(Broker::new(config).unwrap());

        let topic_config = intelstream_core::topic::TopicConfig::new("to-delete", 1, 1);
        broker.create_topic(topic_config).await.unwrap();

        let state = Arc::new(AppState::new(broker));
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/topics/to-delete")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_produce_and_consume_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = BrokerConfig {
            id: 1,
            host: "127.0.0.1".to_string(),
            port: 9292,
            data_dir: tmp.path().to_string_lossy().to_string(),
            storage: StorageConfig::default(),
        };
        let broker = Arc::new(Broker::new(config).unwrap());
        broker.start().await.unwrap();

        let topic_config = intelstream_core::topic::TopicConfig::new("test-rt", 1, 1);
        broker.create_topic(topic_config).await.unwrap();

        let state = Arc::new(AppState::new(broker));

        // Produce a message
        let app = build_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics/test-rt/partitions/0/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"key":"k1","value":"hello-world","headers":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"]["offset"], 0);

        // Consume the message back
        let app = build_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/topics/test-rt/partitions/0/messages?offset=0&max_messages=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let messages = json["data"]["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["value"], "hello-world");
        assert_eq!(messages[0]["offset"], 0);
    }

    #[tokio::test]
    async fn test_produce_to_nonexistent_partition() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics/no-topic/partitions/0/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"value":"test","headers":{}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_cluster_status_returns_200() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cluster/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["success"].as_bool().unwrap());
        assert_eq!(json["data"]["brokers"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_swagger_ui_accessible() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/swagger-ui/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success() || response.status().is_redirection());
    }

    #[tokio::test]
    async fn test_nonexistent_route_returns_404() {
        let tmp = tempfile::TempDir::new().unwrap();
        let app = test_app(tmp.path());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
