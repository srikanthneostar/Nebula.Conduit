# Build stage — use Debian-based image so the CGO binary links against glibc,
# matching the Debian runtime stage below.
FROM golang:1.24-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -a -o nebula-conduit ./cmd/server

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

# Create app directory and set permissions
WORKDIR /app

# Create a non-root user (use a different UID since 65534 is taken)
RUN groupadd -r appuser && useradd -r -g appuser -u 1001 appuser

# Copy binary from builder
COPY --from=builder /app/nebula-conduit .

# Copy commons directory (config, certs, migrations, algorithms, etc.)
COPY --from=builder /app/commons ./commons

# Copy sample data into commons for pipeline usage
COPY --from=builder /app/docs/sample_data.csv ./commons/sample_data.csv

# Create virtual environment and install Python dependencies
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir /app/commons/libraries/nebula_fabric-3.10.0-py3-none-any.whl

# Change ownership of app directory to non-root user
RUN chown -R appuser:appuser /app /opt/venv && \
    mkdir -p /app/data && \
    chown appuser:appuser /app/data

# Switch to non-root user
USER appuser

# Add venv to PATH so Python scripts use it
ENV PATH="/opt/venv/bin:$PATH"

# Set environment variable
ENV NEBULA_CONDUIT_HOME=/app/commons

# Expose port (adjust based on your config.yaml)
EXPOSE 8080

# Run the application
CMD ["./nebula-conduit"]
