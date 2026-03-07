//! # IntelStream API
//!
//! REST and gRPC API layer for producing, consuming, and managing topics.
//! The REST API auto-generates OpenAPI/Swagger documentation.

pub mod grpc;
pub mod rest;

use serde::{Deserialize, Serialize};

/// Common API response wrapper.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

/// API error details.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn err(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(ApiError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_response_ok() {
        let response = ApiResponse::ok("hello");
        assert!(response.success);
        assert_eq!(response.data.unwrap(), "hello");
        assert!(response.error.is_none());
    }

    #[test]
    fn test_api_response_err() {
        let response: ApiResponse<String> = ApiResponse::err("NOT_FOUND", "Topic not found");
        assert!(!response.success);
        assert!(response.data.is_none());
        let error = response.error.unwrap();
        assert_eq!(error.code, "NOT_FOUND");
        assert_eq!(error.message, "Topic not found");
    }

    #[test]
    fn test_api_response_serialization() {
        let response = ApiResponse::ok(42);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"data\":42"));
        assert!(!json.contains("error"));
    }
}
