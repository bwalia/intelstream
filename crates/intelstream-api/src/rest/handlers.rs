//! REST API request handlers.

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Json,
};
use tracing::info;

use super::models::*;
use crate::ApiResponse;

/// Health check endpoint.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthResponse)
    ),
    tag = "health"
)]
pub async fn health_check() -> Json<ApiResponse<HealthResponse>> {
    Json(ApiResponse::ok(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        broker_id: 0, // TODO: inject from broker state
        uptime_secs: 0,
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
pub async fn list_topics() -> Json<ApiResponse<Vec<TopicResponse>>> {
    // TODO: query the broker's topic registry
    Json(ApiResponse::ok(vec![]))
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
    Json(request): Json<TopicCreateRequest>,
) -> (StatusCode, Json<ApiResponse<TopicResponse>>) {
    info!(topic = %request.name, partitions = request.partition_count, "Creating topic");

    // TODO: delegate to broker.create_topic()
    let response = TopicResponse {
        name: request.name,
        partition_count: request.partition_count,
        replication_factor: request.replication_factor,
        retention_hours: request.retention_hours,
        compact: request.compact,
        created_at: chrono::Utc::now().to_rfc3339(),
    };

    (StatusCode::CREATED, Json(ApiResponse::ok(response)))
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
    Path(_name): Path<String>,
) -> Result<Json<ApiResponse<TopicResponse>>, StatusCode> {
    // TODO: query broker for topic metadata
    Err(StatusCode::NOT_FOUND)
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
pub async fn delete_topic(Path(name): Path<String>) -> StatusCode {
    info!(topic = %name, "Deleting topic");
    // TODO: delegate to broker.delete_topic()
    StatusCode::NO_CONTENT
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
    Path((name, partition)): Path<(String, u32)>,
    Json(_request): Json<ProduceRequest>,
) -> Json<ApiResponse<ProduceResponse>> {
    info!(topic = %name, partition, "Producing message");

    // TODO: delegate to broker.produce()
    Json(ApiResponse::ok(ProduceResponse {
        topic: name,
        partition,
        offset: 0,
        timestamp: chrono::Utc::now().to_rfc3339(),
    }))
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
    Path((name, partition)): Path<(String, u32)>,
    Query(params): Query<ConsumeRequest>,
) -> Json<ApiResponse<ConsumeResponse>> {
    info!(topic = %name, partition, offset = ?params.offset, "Consuming messages");

    // TODO: delegate to broker for actual consumption
    Json(ApiResponse::ok(ConsumeResponse {
        topic: name,
        partition,
        messages: vec![],
        next_offset: params.offset.unwrap_or(0),
    }))
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
pub async fn get_cluster_status() -> Json<ApiResponse<ClusterStatusResponse>> {
    // TODO: aggregate cluster state from consensus module
    Json(ApiResponse::ok(ClusterStatusResponse {
        cluster_name: "intelstream-default".to_string(),
        brokers: vec![],
        total_topics: 0,
        total_partitions: 0,
        controller_id: 0,
    }))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::rest::build_router;

    #[tokio::test]
    async fn test_health_check() {
        let app = build_router();
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
    }

    #[tokio::test]
    async fn test_list_topics_returns_200() {
        let app = build_router();
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
    }

    #[tokio::test]
    async fn test_create_topic_returns_201() {
        let app = build_router();
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
    }

    #[tokio::test]
    async fn test_create_topic_invalid_json_returns_error() {
        let app = build_router();
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
    async fn test_delete_topic_returns_204() {
        let app = build_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/topics/test-topic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_produce_message_returns_200() {
        let app = build_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/topics/test-topic/partitions/0/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"key":"k1","value":"v1","headers":{}}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_consume_messages_returns_200() {
        let app = build_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/topics/test-topic/partitions/0/messages?offset=0&max_messages=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_cluster_status_returns_200() {
        let app = build_router();
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
    }

    #[tokio::test]
    async fn test_swagger_ui_accessible() {
        let app = build_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/swagger-ui/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Swagger UI may redirect or return 200
        assert!(response.status().is_success() || response.status().is_redirection());
    }

    #[tokio::test]
    async fn test_nonexistent_route_returns_404() {
        let app = build_router();
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
