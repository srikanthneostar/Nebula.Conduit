# Build stage
FROM golang:1.23.3-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev sqlite-dev

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o nebula-conduit ./cmd/server

# Runtime stage
FROM debian:trixie-slim

# Install runtime dependencies including Python and ODBC
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libsqlite3-0 \
    python3 \
    python3-pip \
    python3-venv \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/nebula-conduit .

# Copy commons directory (config, certs, migrations, algorithms, etc.)
COPY --from=builder /app/commons ./commons

# Create virtual environment and install Python dependencies
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir /app/commons/libraries/nebula_fabric-3.10.0-py3-none-any.whl

# Add venv to PATH so Python scripts use it
ENV PATH="/opt/venv/bin:$PATH"

# Set environment variable
ENV NEBULA_CONDUIT_HOME=/app/commons

# Expose port (adjust based on your config.yaml)
EXPOSE 8080

# Run the application
CMD ["./nebula-conduit"]
