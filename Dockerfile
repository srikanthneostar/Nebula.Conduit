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
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates sqlite-libs

# Create app directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/nebula-conduit .

# Copy commons directory (config, certs, migrations, algorithms, etc.)
COPY --from=builder /app/commons ./commons

# Set environment variable
ENV NEBULA_CONDUIT_HOME=/app/commons

# Expose port (adjust based on your config.yaml)
EXPOSE 8080

# Run the application
CMD ["./nebula-conduit"]
