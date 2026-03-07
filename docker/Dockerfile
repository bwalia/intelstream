# ============================================================================
# IntelStream Server — Multi-stage Docker Build
# ============================================================================

# --- Stage 1: Build ---
FROM rust:1-bookworm AS builder

# Install protoc for proto compilation
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependency compilation
COPY Cargo.toml Cargo.lock* ./
COPY crates/intelstream-core/Cargo.toml crates/intelstream-core/Cargo.toml
COPY crates/intelstream-server/Cargo.toml crates/intelstream-server/Cargo.toml
COPY crates/intelstream-api/Cargo.toml crates/intelstream-api/Cargo.toml
COPY crates/intelstream-client/Cargo.toml crates/intelstream-client/Cargo.toml
COPY crates/intelstream-mcp/Cargo.toml crates/intelstream-mcp/Cargo.toml
COPY crates/intelstream-stream/Cargo.toml crates/intelstream-stream/Cargo.toml
COPY crates/intelstream-schema/Cargo.toml crates/intelstream-schema/Cargo.toml
COPY crates/intelstream-connector/Cargo.toml crates/intelstream-connector/Cargo.toml
COPY crates/intelstream-auth/Cargo.toml crates/intelstream-auth/Cargo.toml
COPY crates/intelstream-cli/Cargo.toml crates/intelstream-cli/Cargo.toml

# Create stub lib.rs files for dependency caching
RUN for crate in core api client mcp stream schema connector auth; do \
      mkdir -p crates/intelstream-${crate}/src && \
      echo "// stub" > crates/intelstream-${crate}/src/lib.rs; \
    done && \
    mkdir -p crates/intelstream-server/src && \
    echo "fn main() {}" > crates/intelstream-server/src/main.rs && \
    mkdir -p crates/intelstream-cli/src && \
    echo "fn main() {}" > crates/intelstream-cli/src/main.rs

# Build dependencies only (cached layer)
RUN cargo build --release --bin intelstream 2>/dev/null || true

# Copy full source and build
COPY . .
RUN cargo build --release --bin intelstream && \
    cargo build --release --bin isctl || \
    (SKIP_PROTO_BUILD=1 cargo build --release --bin intelstream && \
     SKIP_PROTO_BUILD=1 cargo build --release --bin isctl)

# --- Stage 2: Runtime ---
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r intelstream && useradd -r -g intelstream -d /opt/intelstream intelstream

WORKDIR /opt/intelstream

# Copy binaries
COPY --from=builder /build/target/release/intelstream ./bin/
COPY --from=builder /build/target/release/isctl ./bin/
COPY --from=builder /build/config ./config/

# Create data and log directories
RUN mkdir -p data logs && chown -R intelstream:intelstream /opt/intelstream

USER intelstream

# Expose ports: broker, REST API, gRPC, schema registry, MCP, Prometheus
EXPOSE 9292 8080 9090 8081 8090 9191

ENV RUST_LOG=info
ENV INTELSTREAM_CONFIG=/opt/intelstream/config/default.toml

ENTRYPOINT ["./bin/intelstream"]
CMD ["--config", "/opt/intelstream/config/default.toml"]
