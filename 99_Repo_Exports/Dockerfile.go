# Dockerfile for Go news pipeline services
FROM golang:1.22-alpine AS builder

# Install git (needed for go mod download)
# Install git with retries
RUN for i in 1 2 3; do apk add --no-cache git && break || sleep 5; done

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go-news-services/go.mod go-news-services/go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY go-news-services/ ./

# Build the specified binary
ARG BINARY_NAME
ARG SOURCE_PATH
RUN go build -o /app/${BINARY_NAME} ./${SOURCE_PATH}

# Runtime stage — reuse golang:1.22-alpine (already cached, avoids docker.io pull)
FROM golang:1.22-alpine

# Install ca-certificates for HTTPS requests
RUN for i in 1 2 3; do apk --no-cache add ca-certificates && break || sleep 5; done

# Create non-root user
RUN adduser -D -s /bin/sh appuser

# Declare ARG in this stage to use it
ARG BINARY_NAME

# Copy binary from builder to a fixed location
COPY --from=builder /app/${BINARY_NAME} /usr/local/bin/app

# Change ownership
RUN chown appuser:appuser /usr/local/bin/app

# Switch to non-root user
USER appuser

# Default command
CMD ["/usr/local/bin/app"]
