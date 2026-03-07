//! # REST API
//!
//! Axum-based REST API with OpenAPI/Swagger documentation.

pub mod handlers;
pub mod models;

use axum::{
    routing::{delete, get, post},
    Router,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

/// OpenAPI documentation for the IntelStream REST API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "IntelStream API",
        version = "0.1.0",
        description = "Next-generation distributed streaming platform API",
        license(name = "Apache-2.0")
    ),
    paths(
        handlers::health_check,
        handlers::list_topics,
        handlers::create_topic,
        handlers::get_topic,
        handlers::delete_topic,
        handlers::produce_message,
        handlers::consume_messages,
        handlers::get_cluster_status,
    ),
    components(schemas(
        models::TopicCreateRequest,
        models::TopicResponse,
        models::ProduceRequest,
        models::ProduceResponse,
        models::ConsumeRequest,
        models::ConsumeResponse,
        models::MessageResponse,
        models::ClusterStatusResponse,
        models::BrokerInfo,
        models::HealthResponse,
    ))
)]
pub struct ApiDoc;

/// Build the REST API router with all routes.
pub fn build_router() -> Router {
    let api_routes = Router::new()
        // Health
        .route("/health", get(handlers::health_check))
        // Topics
        .route("/topics", get(handlers::list_topics))
        .route("/topics", post(handlers::create_topic))
        .route("/topics/:name", get(handlers::get_topic))
        .route("/topics/:name", delete(handlers::delete_topic))
        // Messages
        .route(
            "/topics/:name/partitions/:partition/messages",
            post(handlers::produce_message),
        )
        .route(
            "/topics/:name/partitions/:partition/messages",
            get(handlers::consume_messages),
        )
        // Cluster
        .route("/cluster/status", get(handlers::get_cluster_status));

    Router::new()
        .nest("/api/v1", api_routes)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
}
