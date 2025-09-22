# Velostream SQL Server Docker Image with All Serialization Formats
# Supports JSON, Avro, and Protobuf serialization
FROM rust:1.85-bookworm as builder

# Configure Git and Cargo to handle SSL issues
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
ENV CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt

# Install system dependencies including protobuf compiler
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    libzstd-dev \
    liblz4-dev \
    librdkafka-dev \
    ca-certificates \
    cmake \
    build-essential \
    protobuf-compiler \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Configure Cargo registry with more robust settings
RUN mkdir -p ~/.cargo && \
    echo '[http]' >> ~/.cargo/config.toml && \
    echo 'check-revoke = false' >> ~/.cargo/config.toml && \
    echo 'timeout = 60' >> ~/.cargo/config.toml && \
    echo '[net]' >> ~/.cargo/config.toml && \
    echo 'retry = 3' >> ~/.cargo/config.toml && \
    echo 'git-fetch-with-cli = true' >> ~/.cargo/config.toml

# Copy manifests and build script
COPY Cargo.toml Cargo.lock build.rs ./

# Create dummy source file to build dependencies first
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN mkdir -p src/bin && echo "fn main() {}" > src/bin/sql_server.rs && echo "fn main() {}" > src/bin/multi_job_sql_server.rs

# Build dependencies with ALL serialization features enabled
RUN cargo build --release --features "avro,protobuf" --bin velo-sql --bin velo-sql-multi

# Copy source code
COPY src ./src
COPY examples ./examples

# Build the actual application with all serialization features
RUN touch src/main.rs src/bin/sql_server.rs src/bin/multi_job_sql_server.rs
RUN cargo build --release --features "avro,protobuf" --bin velo-sql --bin velo-sql-multi

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    libzstd1 \
    liblz4-1 \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Create velo user
RUN useradd -r -s /bin/false -d /app velo

# Create app directory
WORKDIR /app

# Copy binaries from builder stage
COPY --from=builder /app/target/release/velo-sql /usr/local/bin/
COPY --from=builder /app/target/release/velo-sql-multi /usr/local/bin/

# Copy configuration files
COPY configs/velo-default.yaml ./sql-config.yaml
COPY examples/*.sql ./examples/

# Create data and logs directories
RUN mkdir -p /app/data /app/logs && chown -R velo:velo /app

# Switch to non-root user
USER velo

# Expose default ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD [ "velo-sql", "--help" ] || exit 1

# Default command - run multi-job SQL server with all serialization support
CMD ["velo-sql-multi", "--host", "0.0.0.0", "--port", "8080"]

# Build metadata
LABEL org.opencontainers.image.title="Velostream"
LABEL org.opencontainers.image.description="High-performance streaming SQL engine with JSON, Avro, and Protobuf support"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.features="json,avro,protobuf,financial-precision"