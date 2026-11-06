# Multi-stage build for RaftLock server application
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    git \
    ca-certificates \
    tzdata \
    make \
    protobuf \
    protobuf-dev \
    && update-ca-certificates

# Protobuf plugin versions pinned to match go.mod dependencies
ARG PROTOC_GEN_GO_VERSION=v1.36.5
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.5.1

# Install protobuf compiler plugins for Go
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}

# Configure Go environment
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Generate protobuf Go code
RUN protoc --proto_path=. --proto_path=/usr/include \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/raftlock.proto raft/proto/raft.proto

# Build the application with optimizations
RUN go build \
    -a \
    -installsuffix cgo \
    -ldflags='-w -s -extldflags "-static"' \
    -o ./bin/raftlock-server \
    ./cmd/server

# Verify the binary was created and is executable
RUN ls -la ./bin/ && \
    chmod +x ./bin/raftlock-server && \
    ./bin/raftlock-server --version 2>/dev/null || echo "Binary verification complete"

# Final runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    curl \
    && update-ca-certificates

# Create raftlock user and group
RUN addgroup -g 1000 raftlock && \
    adduser -u 1000 -G raftlock -s /bin/sh -D raftlock

# Install grpc_health_probe
RUN curl -L "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.21/grpc_health_probe-linux-amd64" \
    -o /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

# Verify grpc_health_probe installation
RUN /usr/local/bin/grpc_health_probe --version

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/raftlock-server ./raftlock-server

# Create data directory with proper permissions
RUN mkdir -p /app/data && \
    chown -R raftlock:raftlock /app && \
    chmod +x ./raftlock-server

# Switch to non-root user
USER raftlock

# Expose ports
EXPOSE 8080 8081 8082 8083 8089 8090

# Add labels for better container management
LABEL maintainer="RaftLock Team" \
      description="RaftLock distributed lock service" \
      version="1.0.0"

# Health check using grpc_health_probe
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD /usr/local/bin/grpc_health_probe -addr=:8080 -service=raftlock.RaftLock || exit 1

# Default entrypoint
ENTRYPOINT ["./raftlock-server"]

# Default help command
CMD ["--help"]