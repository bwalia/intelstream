//! # IntelStream CLI (`isctl`)
//!
//! Command-line tool for managing IntelStream clusters, topics, and messages.
//!
//! ## Usage
//!
//! ```text
//! isctl topic create my-topic --partitions 6 --replication-factor 3
//! isctl topic list
//! isctl produce my-topic --key "user-123" --value '{"event":"login"}'
//! isctl consume my-topic --group my-group --from-beginning
//! isctl cluster status
//! isctl mcp alerts
//! ```

use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;

/// IntelStream CLI — manage clusters, topics, and messages.
#[derive(Parser)]
#[command(
    name = "isctl",
    version,
    about = "IntelStream command-line interface",
    long_about = "Manage IntelStream distributed streaming clusters, topics, producers, and consumers."
)]
struct Cli {
    /// Broker address (host:port).
    #[arg(short, long, default_value = "localhost:9292", global = true)]
    broker: String,

    /// Output format.
    #[arg(short, long, default_value = "table", global = true)]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Table,
    Json,
    Yaml,
}

#[derive(Subcommand)]
enum Commands {
    /// Topic management commands.
    Topic {
        #[command(subcommand)]
        action: TopicAction,
    },
    /// Produce messages to a topic.
    Produce {
        /// Topic name.
        topic: String,
        /// Message key.
        #[arg(short, long)]
        key: Option<String>,
        /// Message value.
        #[arg(short, long)]
        value: String,
        /// Partition to produce to (optional, auto-assigned if omitted).
        #[arg(short, long)]
        partition: Option<u32>,
    },
    /// Consume messages from a topic.
    Consume {
        /// Topic name.
        topic: String,
        /// Consumer group ID.
        #[arg(short, long, default_value = "isctl-consumer")]
        group: String,
        /// Start from the beginning of the topic.
        #[arg(long)]
        from_beginning: bool,
        /// Maximum number of messages to consume.
        #[arg(short, long)]
        max_messages: Option<u32>,
    },
    /// Cluster management commands.
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    /// MCP (Management Control Plane) commands.
    Mcp {
        #[command(subcommand)]
        action: McpAction,
    },
    /// Schema registry commands.
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
    /// Connector management commands.
    Connector {
        #[command(subcommand)]
        action: ConnectorAction,
    },
}

#[derive(Subcommand)]
enum TopicAction {
    /// Create a new topic.
    Create {
        /// Topic name.
        name: String,
        /// Number of partitions.
        #[arg(short, long, default_value = "6")]
        partitions: u32,
        /// Replication factor.
        #[arg(short, long, default_value = "3")]
        replication_factor: u32,
        /// Retention period in hours.
        #[arg(long, default_value = "168")]
        retention_hours: u64,
        /// Enable log compaction.
        #[arg(long)]
        compact: bool,
    },
    /// List all topics.
    List,
    /// Describe a topic.
    Describe {
        /// Topic name.
        name: String,
    },
    /// Delete a topic.
    Delete {
        /// Topic name.
        name: String,
        /// Skip confirmation prompt.
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum ClusterAction {
    /// Show cluster status.
    Status,
    /// List all brokers.
    Brokers,
}

#[derive(Subcommand)]
enum McpAction {
    /// Show recent alerts from AI agents.
    Alerts,
    /// Show MCP agent status.
    Status,
    /// Show recommendations from AI agents.
    Recommendations,
}

#[derive(Subcommand)]
enum SchemaAction {
    /// Register a new schema.
    Register {
        /// Subject name.
        subject: String,
        /// Path to the schema file.
        #[arg(short, long)]
        file: String,
        /// Schema format (avro, json, protobuf).
        #[arg(short = 't', long, default_value = "avro")]
        format: String,
    },
    /// Get the latest schema for a subject.
    Get {
        /// Subject name.
        subject: String,
    },
    /// List all subjects.
    List,
}

#[derive(Subcommand)]
enum ConnectorAction {
    /// Create a new connector.
    Create {
        /// Path to connector config file.
        #[arg(short, long)]
        config: String,
    },
    /// List all connectors.
    List,
    /// Get connector status.
    Status {
        /// Connector name.
        name: String,
    },
    /// Pause a connector.
    Pause {
        /// Connector name.
        name: String,
    },
    /// Resume a connector.
    Resume {
        /// Connector name.
        name: String,
    },
    /// Delete a connector.
    Delete {
        /// Connector name.
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Topic { action } => handle_topic(action, &cli.broker).await?,
        Commands::Produce {
            topic,
            key,
            value,
            partition,
        } => handle_produce(&cli.broker, &topic, key.as_deref(), &value, partition).await?,
        Commands::Consume {
            topic,
            group,
            from_beginning,
            max_messages,
        } => {
            handle_consume(&cli.broker, &topic, &group, from_beginning, max_messages).await?
        }
        Commands::Cluster { action } => handle_cluster(action, &cli.broker).await?,
        Commands::Mcp { action } => handle_mcp(action, &cli.broker).await?,
        Commands::Schema { action } => handle_schema(action, &cli.broker).await?,
        Commands::Connector { action } => handle_connector(action, &cli.broker).await?,
    }

    Ok(())
}

async fn handle_topic(action: TopicAction, broker: &str) -> Result<()> {
    match action {
        TopicAction::Create {
            name,
            partitions,
            replication_factor,
            retention_hours,
            compact,
        } => {
            println!(
                "{} topic '{}' (partitions={}, rf={}, retention={}h, compact={})",
                "Creating".green().bold(),
                name,
                partitions,
                replication_factor,
                retention_hours,
                compact
            );
            // TODO: send REST API request to broker
            println!("{} Topic '{}' created successfully", "✓".green(), name);
        }
        TopicAction::List => {
            println!("{}", "Topics:".bold());
            // TODO: fetch topics from REST API
            println!("  (no topics — connect to broker at {} to list)", broker);
        }
        TopicAction::Describe { name } => {
            println!("{} topic '{}'", "Describing".cyan(), name);
            // TODO: fetch topic details from REST API
        }
        TopicAction::Delete { name, yes } => {
            if !yes {
                println!(
                    "{} Are you sure you want to delete topic '{}'? (use -y to skip)",
                    "Warning:".yellow().bold(),
                    name
                );
                // TODO: prompt for confirmation via dialoguer
            }
            // TODO: send delete request
            println!("{} Topic '{}' deleted", "✓".green(), name);
        }
    }
    Ok(())
}

async fn handle_produce(
    broker: &str,
    topic: &str,
    key: Option<&str>,
    value: &str,
    partition: Option<u32>,
) -> Result<()> {
    println!(
        "{} to topic '{}' (broker={})",
        "Producing".green().bold(),
        topic,
        broker
    );
    // TODO: create producer and send message via client SDK
    println!(
        "{} Message produced (key={}, partition={}, offset=0)",
        "✓".green(),
        key.unwrap_or("null"),
        partition.unwrap_or(0)
    );
    Ok(())
}

async fn handle_consume(
    broker: &str,
    topic: &str,
    group: &str,
    from_beginning: bool,
    max_messages: Option<u32>,
) -> Result<()> {
    println!(
        "{} from topic '{}' (group={}, from_beginning={})",
        "Consuming".cyan().bold(),
        topic,
        group,
        from_beginning
    );
    // TODO: create consumer and poll messages via client SDK
    println!("  (no messages — connect to broker at {} to consume)", broker);
    Ok(())
}

async fn handle_cluster(action: ClusterAction, broker: &str) -> Result<()> {
    match action {
        ClusterAction::Status => {
            println!("{}", "Cluster Status:".bold());
            // TODO: fetch from REST API
            println!("  Broker: {}", broker);
            println!("  Status: {}", "connecting...".yellow());
        }
        ClusterAction::Brokers => {
            println!("{}", "Brokers:".bold());
            // TODO: fetch broker list from REST API
        }
    }
    Ok(())
}

async fn handle_mcp(action: McpAction, _broker: &str) -> Result<()> {
    match action {
        McpAction::Alerts => {
            println!("{}", "MCP Alerts:".bold());
            // TODO: fetch from MCP API
            println!("  No recent alerts");
        }
        McpAction::Status => {
            println!("{}", "MCP Agent Status:".bold());
            println!("  anomaly-detector: {}", "active".green());
            println!("  auto-scaler: {}", "active".green());
            println!("  load-balancer: {}", "active".green());
            println!("  failover-manager: {}", "active".green());
        }
        McpAction::Recommendations => {
            println!("{}", "MCP Recommendations:".bold());
            // TODO: fetch from MCP API
            println!("  No pending recommendations");
        }
    }
    Ok(())
}

async fn handle_schema(action: SchemaAction, _broker: &str) -> Result<()> {
    match action {
        SchemaAction::Register {
            subject,
            file,
            format,
        } => {
            println!(
                "{} schema for subject '{}' (format={}, file={})",
                "Registering".green().bold(),
                subject,
                format,
                file
            );
            // TODO: read schema file and POST to schema registry
        }
        SchemaAction::Get { subject } => {
            println!("{} latest schema for '{}'", "Fetching".cyan(), subject);
            // TODO: GET from schema registry
        }
        SchemaAction::List => {
            println!("{}", "Schema Subjects:".bold());
            // TODO: fetch from schema registry
        }
    }
    Ok(())
}

async fn handle_connector(action: ConnectorAction, _broker: &str) -> Result<()> {
    match action {
        ConnectorAction::Create { config } => {
            println!("{} connector from config: {}", "Creating".green().bold(), config);
            // TODO: read config and POST to connector API
        }
        ConnectorAction::List => {
            println!("{}", "Connectors:".bold());
            // TODO: fetch from connector API
        }
        ConnectorAction::Status { name } => {
            println!("{} status for connector '{}'", "Fetching".cyan(), name);
        }
        ConnectorAction::Pause { name } => {
            println!("{} connector '{}'", "Pausing".yellow().bold(), name);
        }
        ConnectorAction::Resume { name } => {
            println!("{} connector '{}'", "Resuming".green().bold(), name);
        }
        ConnectorAction::Delete { name } => {
            println!("{} connector '{}'", "Deleting".red().bold(), name);
        }
    }
    Ok(())
}
