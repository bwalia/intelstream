//! # IntelStream Server
//!
//! Main entry point for the IntelStream distributed streaming platform.
//! Boots the broker, API server, MCP automation server, and schema registry.

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod config;

use config::ServerConfig;

/// IntelStream — Next-Generation Distributed Streaming Platform
#[derive(Parser, Debug)]
#[command(name = "intelstream", version, about, long_about = None)]
struct Cli {
    /// Path to the server configuration file.
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,

    /// Override the broker ID.
    #[arg(long)]
    broker_id: Option<u32>,

    /// Override the listen address.
    #[arg(long)]
    host: Option<String>,

    /// Override the broker port.
    #[arg(long)]
    port: Option<u16>,

    /// Data directory override.
    #[arg(long)]
    data_dir: Option<String>,

    /// Log level override (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing / logging
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .json()
        .init();

    info!("Starting IntelStream server...");

    // Load configuration
    let config = ServerConfig::load(&cli.config)?;
    info!(?config, "Loaded configuration");

    // Resolve host override once, used by both broker and REST server
    let listen_host = cli.host.unwrap_or_else(|| config.broker.host.clone());

    // Initialize the core broker
    let broker_config = intelstream_core::broker::BrokerConfig {
        id: cli.broker_id.unwrap_or(config.broker.id),
        host: listen_host.clone(),
        port: cli.port.unwrap_or(config.broker.port),
        data_dir: cli.data_dir.unwrap_or(config.broker.data_dir.clone()),
        storage: intelstream_core::storage::StorageConfig {
            data_dir: config.broker.data_dir.clone(),
            segment_size_bytes: config.broker.log_segment_size_bytes,
            max_message_size_bytes: config.broker.max_message_size_bytes as usize,
            ..Default::default()
        },
    };

    let broker = intelstream_core::broker::Broker::new(broker_config)?;
    broker.start().await?;

    info!(
        "IntelStream broker {} is ready — listening on {}:{}",
        broker.id(),
        config.broker.host,
        config.broker.port,
    );

    // Start the REST API server
    let rest_addr = format!("{}:{}", listen_host, config.api.rest_port);
    let rest_router = intelstream_api::rest::build_router();
    let rest_listener = TcpListener::bind(&rest_addr).await?;
    info!("REST API listening on {}", rest_addr);

    tokio::spawn(async move {
        if let Err(e) = axum::serve(rest_listener, rest_router).await {
            tracing::error!("REST API server error: {}", e);
        }
    });

    // TODO: Start the gRPC server (intelstream-api)
    // TODO: Start the MCP server (intelstream-mcp)
    // TODO: Start the schema registry (intelstream-schema)
    // TODO: Initialize consensus and join cluster
    // TODO: Start replication engine

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal");
    broker.shutdown().await?;

    info!("IntelStream server stopped.");
    Ok(())
}
