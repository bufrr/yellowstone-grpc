# Build stage
FROM rust:1.86-bookworm as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY examples/ ./examples/
COPY yellowstone-grpc-client/ ./yellowstone-grpc-client/
COPY yellowstone-grpc-geyser/ ./yellowstone-grpc-geyser/
COPY yellowstone-grpc-proto/ ./yellowstone-grpc-proto/

# Build the specific binary
RUN cargo build --release -p yellowstone-grpc-client-simple --bin dex-router-monitor

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/dex-router-monitor /usr/local/bin/dex-router-monitor

# Create logs directory
RUN mkdir -p /app/logs

# Set default environment variables
ENV RUST_LOG=info
ENV ENDPOINT=http://127.0.0.1:10000

# Expose no ports (client application)

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep dex-router-monitor > /dev/null || exit 1

# Default command
ENTRYPOINT ["dex-router-monitor"]
CMD ["--help"]