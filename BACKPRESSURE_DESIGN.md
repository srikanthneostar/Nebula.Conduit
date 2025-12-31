# Backpressure Design Implementation

This document describes the backpressure design patterns implemented in the Nebula Conduit API server to handle high load scenarios gracefully.

## Overview

The backpressure implementation includes multiple layers of protection to prevent system overload and ensure service reliability:

1. **Circuit Breaker Pattern** - Prevents cascade failures
2. **Rate Limiting** - Controls request frequency per user
3. **Task Queue Management** - Manages concurrent task execution
4. **Resource Monitoring** - Monitors system health
5. **Graceful Degradation** - Rejects requests when overloaded

## Components

### 1. Circuit Breaker (`CircuitBreaker`)

**Purpose**: Prevents cascade failures by temporarily blocking requests when the system is experiencing high failure rates.

**States**:
- `CircuitClosed`: Normal operation, requests pass through
- `CircuitOpen`: High failure rate detected, requests are blocked
- `CircuitHalfOpen`: Testing if system has recovered

**Configuration**:
- `maxFailures`: 5 consecutive failures trigger circuit opening
- `timeout`: 30 seconds before attempting recovery

### 2. Rate Limiter (`UserRateLimiter`)

**Purpose**: Controls the rate of requests per user to prevent abuse and ensure fair resource allocation.

**Features**:
- Per-user rate limiting using token bucket algorithm
- Configurable rate (10 requests/second) and burst size (20 requests)
- Automatic limiter creation for new users

### 3. Task Queue (`TaskQueue`)

**Purpose**: Manages concurrent task execution to prevent resource exhaustion.

**Features**:
- Maximum concurrent tasks: 10
- Queue size limit: 100 pending tasks
- Atomic counters for thread-safe operations
- Graceful task queuing when at capacity

### 4. Resource Monitor (`ResourceMonitor`)

**Purpose**: Monitors system resources and triggers backpressure when thresholds are exceeded.

**Metrics**:
- Memory usage monitoring (80% threshold)
- CPU usage monitoring (80% threshold) - placeholder implementation
- Periodic health checks every 5 seconds

### 5. Backpressure Middleware

**Purpose**: Coordinates all backpressure mechanisms and makes decisions about request handling.

**Flow**:
1. Check circuit breaker state
2. Verify resource usage levels
3. Allow or reject requests based on system health

## API Endpoints

### Enhanced Task Creation (`POST /tasks`)

The task creation endpoint now implements intelligent backpressure:

- **Immediate Processing**: When system capacity allows, tasks are processed immediately (HTTP 201)
- **Queuing**: When at capacity, tasks are queued for later processing (HTTP 202)
- **Rejection**: When queue is full, requests are rejected (HTTP 503)

### Metrics Endpoint (`GET /metrics`)

Provides real-time backpressure statistics:

```json
{
  "timestamp": "2024-01-15T10:30:45Z",
  "uptime": "2h15m30s",
  "backpressure": {
    "circuit_breaker_state": 0,
    "circuit_breaker_failures": 0,
    "task_queue_size": 5,
    "running_tasks": 3,
    "max_concurrent_tasks": 10,
    "max_queue_size": 100
  },
  "resources": {
    "memory_usage_percent": 45.2,
    "cpu_usage_percent": 0,
    "max_memory_percent": 80,
    "max_cpu_percent": 80
  },
  "rate_limiting": {
    "rate_per_second": 10,
    "burst_size": 20
  }
}
```

## Configuration

Default backpressure configuration:

```go
backpressureConfig := &BackpressureConfig{
    MaxConcurrentTasks:    10,
    MaxQueueSize:          100,
    RateLimitPerSecond:    10.0,
    RateLimitBurst:        20,
    CircuitBreakerTimeout: 30 * time.Second,
    MaxMemoryUsagePercent: 80.0,
    MaxCPUUsagePercent:    80.0,
}
```

## HTTP Status Codes

The API now returns appropriate HTTP status codes for backpressure scenarios:

- `200 OK`: Normal operation
- `201 Created`: Task created and processing immediately
- `202 Accepted`: Task queued due to backpressure
- `429 Too Many Requests`: Rate limit exceeded
- `503 Service Unavailable`: System overloaded or circuit breaker open

## Background Workers

### Task Queue Worker
- Processes queued tasks when capacity becomes available
- Implements retry logic with exponential backoff
- Records success/failure for circuit breaker

### Resource Monitor Worker
- Continuously monitors system resources
- Updates usage statistics every 5 seconds
- Logs warnings when thresholds are exceeded

## Benefits

1. **System Stability**: Prevents overload and maintains service availability
2. **Fair Resource Allocation**: Rate limiting ensures equitable access
3. **Graceful Degradation**: System remains responsive under high load
4. **Observability**: Comprehensive metrics for monitoring and alerting
5. **Automatic Recovery**: Circuit breaker allows automatic system recovery

## Future Enhancements

1. **Advanced CPU Monitoring**: Implement proper CPU usage calculation
2. **Adaptive Thresholds**: Dynamic adjustment based on system performance
3. **Priority Queuing**: Different priority levels for tasks
4. **Distributed Rate Limiting**: Coordination across multiple instances
5. **Custom Backpressure Policies**: User-configurable backpressure rules

## Monitoring and Alerting

Monitor these key metrics:

- Circuit breaker state changes
- Queue size approaching limits
- Resource usage exceeding thresholds
- Rate limit violations
- Task processing latency

Set up alerts for:
- Circuit breaker opening
- Queue size > 80% of capacity
- Memory/CPU usage > 90%
- High rate limit rejection rates