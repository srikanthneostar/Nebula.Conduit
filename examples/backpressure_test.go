package examples

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/internal/models"
)

// Example demonstrating backpressure behavior
func TestBackpressureExample(t *testing.T) {
	// This is a conceptual test showing how backpressure would work
	// In a real scenario, you would set up the server with test dependencies

	fmt.Println("=== Backpressure Design Pattern Demo ===")

	// Simulate task requests
	taskRequests := []models.TaskRequest{
		{Script: "test1.py", Args: []string{"arg1"}, Env: []string{"ENV=test"}},
		{Script: "test2.py", Args: []string{"arg2"}, Env: []string{"ENV=test"}},
		{Script: "test3.py", Args: []string{"arg3"}, Env: []string{"ENV=test"}},
	}

	fmt.Println("\n1. Normal Operation (Low Load):")
	fmt.Println("   - Circuit Breaker: CLOSED")
	fmt.Println("   - Queue Size: 0/100")
	fmt.Println("   - Running Tasks: 2/10")
	fmt.Println("   - Memory Usage: 45%")
	fmt.Println("   → Result: HTTP 201 Created (Immediate processing)")

	fmt.Println("\n2. High Load Scenario:")
	fmt.Println("   - Circuit Breaker: CLOSED")
	fmt.Println("   - Queue Size: 15/100")
	fmt.Println("   - Running Tasks: 10/10 (At capacity)")
	fmt.Println("   - Memory Usage: 65%")
	fmt.Println("   → Result: HTTP 202 Accepted (Task queued)")

	fmt.Println("\n3. System Overload:")
	fmt.Println("   - Circuit Breaker: CLOSED")
	fmt.Println("   - Queue Size: 100/100 (Full)")
	fmt.Println("   - Running Tasks: 10/10")
	fmt.Println("   - Memory Usage: 85% (Above threshold)")
	fmt.Println("   → Result: HTTP 503 Service Unavailable")

	fmt.Println("\n4. Circuit Breaker Open:")
	fmt.Println("   - Circuit Breaker: OPEN (5+ failures)")
	fmt.Println("   - Queue Size: 50/100")
	fmt.Println("   - Running Tasks: 8/10")
	fmt.Println("   - Memory Usage: 70%")
	fmt.Println("   → Result: HTTP 503 Service Unavailable")

	fmt.Println("\n5. Rate Limiting:")
	fmt.Println("   - User exceeded 10 requests/second")
	fmt.Println("   - Burst capacity (20) exhausted")
	fmt.Println("   → Result: HTTP 429 Too Many Requests")

	// Demonstrate the request/response flow
	for i, req := range taskRequests {
		fmt.Printf("\n--- Request %d ---\n", i+1)

		// Convert to JSON
		jsonData, _ := json.Marshal(req)
		fmt.Printf("Request: POST /tasks\n%s\n", string(jsonData))

		// Simulate different responses based on system state
		switch i {
		case 0:
			// Normal processing
			response := map[string]interface{}{
				"id":         "task-123",
				"script":     req.Script,
				"status":     "running",
				"created_by": 1,
			}
			jsonResp, _ := json.Marshal(response)
			fmt.Printf("Response: HTTP 201 Created\n%s\n", string(jsonResp))

		case 1:
			// Queued due to capacity
			response := map[string]string{
				"message": "Task queued for execution due to high load",
			}
			jsonResp, _ := json.Marshal(response)
			fmt.Printf("Response: HTTP 202 Accepted\n%s\n", string(jsonResp))

		case 2:
			// Rejected due to overload
			response := map[string]string{
				"error": "System overloaded",
			}
			jsonResp, _ := json.Marshal(response)
			fmt.Printf("Response: HTTP 503 Service Unavailable\n%s\n", string(jsonResp))
		}
	}

	fmt.Println("\n=== Metrics Endpoint Response ===")
	metricsResponse := map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"uptime":    "2h15m30s",
		"backpressure": map[string]interface{}{
			"circuit_breaker_state":    0, // CLOSED
			"circuit_breaker_failures": 2,
			"task_queue_size":          15,
			"running_tasks":            10,
			"max_concurrent_tasks":     10,
			"max_queue_size":           100,
		},
		"resources": map[string]interface{}{
			"memory_usage_percent": 75.5,
			"cpu_usage_percent":    0,
			"max_memory_percent":   80,
			"max_cpu_percent":      80,
		},
		"rate_limiting": map[string]interface{}{
			"rate_per_second": 10,
			"burst_size":      20,
		},
	}

	jsonMetrics, _ := json.MarshalIndent(metricsResponse, "", "  ")
	fmt.Printf("GET /metrics\n%s\n", string(jsonMetrics))

	fmt.Println("\n=== Key Benefits ===")
	fmt.Println("✓ Prevents system overload")
	fmt.Println("✓ Maintains service availability under high load")
	fmt.Println("✓ Fair resource allocation via rate limiting")
	fmt.Println("✓ Automatic failure recovery via circuit breaker")
	fmt.Println("✓ Comprehensive monitoring and observability")
	fmt.Println("✓ Graceful degradation with appropriate HTTP status codes")
}

// Helper function to create HTTP test requests
func createTaskRequest(task models.TaskRequest) *http.Request {
	jsonData, _ := json.Marshal(task)
	req := httptest.NewRequest("POST", "/tasks", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	return req
}

// Example of how to test rate limiting
func TestRateLimitingExample(t *testing.T) {
	fmt.Println("=== Rate Limiting Example ===")

	// Simulate rapid requests from the same user
	for i := 0; i < 25; i++ {
		status := "200 OK"
		if i >= 20 { // After burst capacity
			status = "429 Too Many Requests"
		}
		fmt.Printf("Request %d: %s\n", i+1, status)
	}

	fmt.Println("\nAfter 1 second (token bucket refill):")
	fmt.Println("Request 26: 200 OK (Rate limiter allows new requests)")
}

// Example of circuit breaker behavior
func TestCircuitBreakerExample(t *testing.T) {
	fmt.Println("=== Circuit Breaker Example ===")

	states := []string{
		"CLOSED - Normal operation",
		"CLOSED - 1 failure",
		"CLOSED - 2 failures",
		"CLOSED - 3 failures",
		"CLOSED - 4 failures",
		"OPEN - 5 failures (Circuit opens)",
		"OPEN - Request blocked",
		"OPEN - Request blocked",
		"HALF_OPEN - Testing recovery (after 30s)",
		"CLOSED - Recovery successful",
	}

	for i, state := range states {
		fmt.Printf("Step %d: %s\n", i+1, state)
	}
}
