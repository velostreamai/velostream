# FerrisStreams SQL Server Docker Image
FROM rust:1.85-bookworm as builder

# Configure Git and Cargo to handle SSL issues
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
ENV CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt

# Install system dependencies and update CA certificates
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    libzstd-dev \
    liblz4-dev \
    librdkafka-dev \
    ca-certificates \
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

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy source file to build dependencies first
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN mkdir -p src/bin && echo "fn main() {}" > src/bin/sql_server.rs && echo "fn main() {}" > src/bin/multi_job_sql_server.rs

# Build dependencies
RUN cargo build --release --bin ferris-sql --bin ferris-sql-multi

# Copy source code
COPY src ./src
COPY examples ./examples

# Build the actual application
RUN touch src/main.rs src/bin/sql_server.rs src/bin/multi_job_sql_server.rs
RUN cargo build --release --bin ferris-sql --bin ferris-sql-multi

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

# Create ferris user
RUN useradd -r -s /bin/false -d /app ferris

# Create app directory
WORKDIR /app

# Copy binaries from builder stage
COPY --from=builder /app/target/release/ferris-sql /usr/local/bin/
COPY --from=builder /app/target/release/ferris-sql-multi /usr/local/bin/

# Copy configuration files
COPY sql-config.yaml ./
COPY examples/*.sql ./examples/

# Create data and logs directories
RUN mkdir -p /app/data /app/logs && chown -R ferris:ferris /app

# Switch to non-root user
USER ferris

# Expose default ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD [ "ferris-sql", "--help" ] || exit 1

# Default command - run SQL server
CMD ["ferris-sql", "server", "--brokers", "kafka:9092", "--port", "8080"]