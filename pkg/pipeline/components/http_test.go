package components

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

func TestNewHTTPGetComponent(t *testing.T) {
	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
	}{
		{
			name: "valid configuration",
			config: pipeline.ComponentConfig{
				ID:   "test-http-1",
				Type: pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
					"headers": map[string]interface{}{
						"Authorization": "Bearer token123",
					},
					"interval": "30s",
				},
				Timeout: 10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "missing url",
			config: pipeline.ComponentConfig{
				ID:         "test-http-2",
				Type:       pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{},
			},
			expectError: true,
		},
		{
			name: "invalid interval format",
			config: pipeline.ComponentConfig{
				ID:   "test-http-3",
				Type: pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url":      "https://api.example.com/data",
					"interval": "invalid",
				},
			},
			expectError: true,
		},
		{
			name: "valid configuration without interval",
			config: pipeline.ComponentConfig{
				ID:   "test-http-4",
				Type: pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := NewHTTPGetComponent(tt.config)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if component == nil {
					t.Errorf("expected component but got nil")
				}
			}
		})
	}
}

func TestHTTPGetComponent_Validate(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		expectError bool
	}{
		{
			name:        "valid URL",
			url:         "https://api.example.com/data",
			expectError: false,
		},
		{
			name:        "empty URL",
			url:         "",
			expectError: true,
		},
		{
			name:        "invalid URL",
			url:         "://invalid-url",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component := &HTTPGetComponent{
				url: tt.url,
				config: pipeline.ComponentConfig{
					ID: "test",
				},
			}

			err := component.Validate()
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestHTTPGetComponent_Execute(t *testing.T) {
	// Create a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify headers
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
			t.Errorf("expected Authorization header 'Bearer test-token', got '%s'", auth)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "success"}`))
	}))
	defer server.Close()

	config := pipeline.ComponentConfig{
		ID:   "test-http",
		Type: pipeline.ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": server.URL,
			"headers": map[string]interface{}{
				"Authorization": "Bearer test-token",
			},
		},
		Timeout: 5 * time.Second,
	}

	component, err := NewHTTPGetComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	output, err := component.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("failed to execute component: %v", err)
	}

	// Read from output channel
	select {
	case data := <-output:
		if data.Payload == nil {
			t.Errorf("expected payload but got nil")
		}

		body, ok := data.Payload.([]byte)
		if !ok {
			t.Errorf("expected payload to be []byte")
		}

		expectedBody := `{"message": "success"}`
		if string(body) != expectedBody {
			t.Errorf("expected body '%s', got '%s'", expectedBody, string(body))
		}

		// Verify metadata
		if data.Metadata["status_code"] != "200" {
			t.Errorf("expected status_code '200', got '%s'", data.Metadata["status_code"])
		}

		if data.Metadata["content_type"] != "application/json" {
			t.Errorf("expected content_type 'application/json', got '%s'", data.Metadata["content_type"])
		}

	case <-time.After(5 * time.Second):
		t.Errorf("timeout waiting for data")
	}
}

func TestHTTPGetComponent_ExecuteWithInterval(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"count": ` + string(rune(requestCount+'0')) + `}`))
	}))
	defer server.Close()

	config := pipeline.ComponentConfig{
		ID:   "test-http-interval",
		Type: pipeline.ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url":      server.URL,
			"interval": "100ms",
		},
		Timeout: 5 * time.Second,
	}

	component, err := NewHTTPGetComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	output, err := component.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("failed to execute component: %v", err)
	}

	// Should receive at least 2 requests (at 100ms and 200ms)
	receivedCount := 0
	timeout := time.After(400 * time.Millisecond)

	for receivedCount < 2 {
		select {
		case data, ok := <-output:
			if !ok {
				// Channel closed
				if receivedCount < 2 {
					t.Errorf("expected at least 2 requests, got %d", receivedCount)
				}
				return
			}
			if data.Payload != nil {
				receivedCount++
			}
		case <-timeout:
			t.Errorf("timeout waiting for requests, received %d", receivedCount)
			return
		}
	}
}

func TestHTTPGetComponent_ExecuteWithError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "internal server error"}`))
	}))
	defer server.Close()

	config := pipeline.ComponentConfig{
		ID:   "test-http-error",
		Type: pipeline.ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": server.URL,
		},
		Timeout:         5 * time.Second,
		ContinueOnError: false,
	}

	component, err := NewHTTPGetComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	output, err := component.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("failed to execute component: %v", err)
	}

	// Should not receive any data due to error
	select {
	case data, ok := <-output:
		if ok && data.Payload != nil {
			t.Errorf("expected no data due to error, but received data")
		}
	case <-time.After(2 * time.Second):
		// Expected - no data should be sent
	}
}

func TestHTTPGetComponent_ExecuteAsProcessorWaitsForDelayedInput(t *testing.T) {
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "templated"}`))
	}))
	defer server.Close()

	config := pipeline.ComponentConfig{
		ID:   "test-http-processor",
		Type: pipeline.ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": server.URL + "/customers/{{customer_id}}",
			"headers": map[string]interface{}{
				"X-Customer": "{{customer_id}}",
			},
		},
		Timeout: 5 * time.Second,
	}

	component, err := NewHTTPGetComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	input := make(chan pipeline.Data, 1)
	output, err := component.Execute(ctx, input)
	if err != nil {
		t.Fatalf("failed to execute component: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	select {
	case reqPath := <-requests:
		t.Fatalf("expected HTTP GET to wait for input before sending request, got early request to %s", reqPath)
	default:
	}

	input <- pipeline.Data{
		Metadata: map[string]string{
			"customer_id": "cust-123",
		},
		TraceID: "trace-123",
	}
	close(input)

	select {
	case reqPath := <-requests:
		if !strings.HasSuffix(reqPath, "/customers/cust-123") {
			t.Fatalf("expected templated request path to end with /customers/cust-123, got %s", reqPath)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for templated processor request")
	}

	select {
	case data, ok := <-output:
		if !ok {
			t.Fatal("expected output data but channel was closed")
		}
		if data.TraceID != "trace-123" {
			t.Fatalf("expected trace id trace-123, got %s", data.TraceID)
		}
		if data.Metadata["status_code"] != "200" {
			t.Fatalf("expected status_code 200, got %s", data.Metadata["status_code"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for processor output")
	}
}

func TestHTTPGetComponent_Type(t *testing.T) {
	component := &HTTPGetComponent{
		config: pipeline.ComponentConfig{
			ID:   "test",
			Type: pipeline.ComponentTypeHTTPGet,
		},
	}

	if component.Type() != pipeline.ComponentTypeHTTPGet {
		t.Errorf("expected type %s, got %s", pipeline.ComponentTypeHTTPGet, component.Type())
	}
}

func TestHTTPGetComponent_ID(t *testing.T) {
	expectedID := "test-component-123"
	component := &HTTPGetComponent{
		config: pipeline.ComponentConfig{
			ID: expectedID,
		},
	}

	if component.ID() != expectedID {
		t.Errorf("expected ID %s, got %s", expectedID, component.ID())
	}
}

func TestHTTPGetComponent_Config(t *testing.T) {
	config := pipeline.ComponentConfig{
		ID:   "test",
		Type: pipeline.ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": "https://api.example.com",
		},
	}

	component := &HTTPGetComponent{
		config: config,
	}

	returnedConfig := component.Config()
	if returnedConfig.ID != config.ID {
		t.Errorf("expected config ID %s, got %s", config.ID, returnedConfig.ID)
	}
	if returnedConfig.Type != config.Type {
		t.Errorf("expected config Type %s, got %s", config.Type, returnedConfig.Type)
	}
}

func TestNewHTTPPostComponent(t *testing.T) {
	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
	}{
		{
			name: "valid configuration",
			config: pipeline.ComponentConfig{
				ID:   "test-http-post-1",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url":          "https://api.example.com/data",
					"content_type": "application/json",
					"headers": map[string]interface{}{
						"Authorization": "Bearer token123",
					},
				},
				Timeout: 10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "missing url",
			config: pipeline.ComponentConfig{
				ID:   "test-http-post-2",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"content_type": "application/json",
				},
			},
			expectError: true,
		},
		{
			name: "missing content_type uses default",
			config: pipeline.ComponentConfig{
				ID:   "test-http-post-3",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
				},
			},
			expectError: false,
		},
		{
			name: "valid configuration without headers",
			config: pipeline.ComponentConfig{
				ID:   "test-http-post-4",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url":          "https://api.example.com/data",
					"content_type": "application/json",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := NewHTTPPostComponent(tt.config)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if component == nil {
					t.Errorf("expected component but got nil")
				}
				httpPost, ok := component.(*HTTPPostComponent)
				if !ok {
					t.Fatalf("expected *HTTPPostComponent, got %T", component)
				}
				if tt.name == "missing content_type uses default" && httpPost.contentType != "application/json" {
					t.Errorf("expected default content type application/json, got %q", httpPost.contentType)
				}
			}
		})
	}
}

func TestHTTPPostComponent_Validate(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		contentType string
		expectError bool
	}{
		{
			name:        "valid configuration",
			url:         "https://api.example.com/data",
			contentType: "application/json",
			expectError: false,
		},
		{
			name:        "empty URL",
			url:         "",
			contentType: "application/json",
			expectError: true,
		},
		{
			name:        "empty content type uses default",
			url:         "https://api.example.com/data",
			contentType: "",
			expectError: false,
		},
		{
			name:        "invalid URL",
			url:         "://invalid-url",
			contentType: "application/json",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component := &HTTPPostComponent{
				url:         tt.url,
				contentType: tt.contentType,
				config: pipeline.ComponentConfig{
					ID: "test",
				},
			}

			err := component.Validate()
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestHTTPPostComponent_Execute(t *testing.T) {
	// Create a test HTTP server
	receivedBody := ""
	receivedContentType := ""
	receivedAuth := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify method
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got %s", r.Method)
		}

		// Capture headers
		receivedContentType = r.Header.Get("Content-Type")
		receivedAuth = r.Header.Get("Authorization")

		// Read body
		body := make([]byte, r.ContentLength)
		r.Body.Read(body)
		receivedBody = string(body)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "received"}`))
	}))
	defer server.Close()

	config := pipeline.ComponentConfig{
		ID:   "test-http-post",
		Type: pipeline.ComponentTypeHTTPPost,
		Parameters: map[string]interface{}{
			"url":          server.URL,
			"content_type": "application/json",
			"headers": map[string]interface{}{
				"Authorization": "Bearer test-token",
			},
		},
		Timeout: 5 * time.Second,
	}

	component, err := NewHTTPPostComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create input channel and send test data
	input := make(chan pipeline.Data, 1)
	testPayload := []byte(`{"test": "data"}`)
	input <- pipeline.Data{
		Payload:   testPayload,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   "test-trace",
	}
	close(input)

	output, err := component.Execute(ctx, input)
	if err != nil {
		t.Fatalf("failed to execute component: %v", err)
	}

	// Wait for execution to complete
	time.Sleep(100 * time.Millisecond)

	// Verify the request was made correctly
	if receivedContentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got '%s'", receivedContentType)
	}

	if receivedAuth != "Bearer test-token" {
		t.Errorf("expected Authorization 'Bearer test-token', got '%s'", receivedAuth)
	}

	if receivedBody != string(testPayload) {
		t.Errorf("expected body '%s', got '%s'", string(testPayload), receivedBody)
	}

	// Verify the response is emitted downstream
	select {
	case data, ok := <-output:
		if !ok {
			t.Fatalf("expected response output but channel was closed")
		}
		responsePayload, ok := data.Payload.([]byte)
		if !ok {
			t.Fatalf("expected []byte response payload, got %T", data.Payload)
		}
		if string(responsePayload) != `{"status": "received"}` {
			t.Errorf("expected response payload %q, got %q", `{"status": "received"}`, string(responsePayload))
		}
		if data.TraceID != "test-trace" {
			t.Errorf("expected trace id %q, got %q", "test-trace", data.TraceID)
		}
		if data.Metadata["status_code"] != "200" {
			t.Errorf("expected status_code 200, got %s", data.Metadata["status_code"])
		}
		if data.Metadata["content_type"] == "" {
			t.Errorf("expected response content_type metadata to be populated")
		}
	case <-time.After(1 * time.Second):
		t.Errorf("timeout waiting for HTTP POST response output")
	}

	select {
	case _, ok := <-output:
		if ok {
			t.Errorf("expected output channel to be closed after single response")
		}
	case <-time.After(1 * time.Second):
		t.Errorf("timeout waiting for output channel to close")
	}
}

func TestHTTPPostComponent_ExecuteWithStringPayload(t *testing.T) {
	receivedBody := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		r.Body.Read(body)
		receivedBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := pipeline.ComponentConfig{
		ID:   "test-http-post-string",
		Type: pipeline.ComponentTypeHTTPPost,
		Parameters: map[string]interface{}{
			"url":          server.URL,
			"content_type": "text/plain",
		},
		Timeout: 5 * time.Second,
	}

	component, err := NewHTTPPostComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	input := make(chan pipeline.Data, 1)
	testPayload := "test string data"
	input <- pipeline.Data{
		Payload:   testPayload,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   "test-trace",
	}
	close(input)

	output, err := component.Execute(ctx, input)
	if err != nil {
		t.Fatalf("failed to execute component: %v", err)
	}

	select {
	case data, ok := <-output:
		if !ok {
			t.Fatal("expected response output but channel was closed")
		}
		if string(data.Payload.([]byte)) != "" {
			t.Errorf("expected empty response payload, got %q", string(data.Payload.([]byte)))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for HTTP POST response output")
	}

	if receivedBody != testPayload {
		t.Errorf("expected body '%s', got '%s'", testPayload, receivedBody)
	}
}

func TestHTTPPostComponent_ExecuteWithError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "internal server error"}`))
	}))
	defer server.Close()

	config := pipeline.ComponentConfig{
		ID:   "test-http-post-error",
		Type: pipeline.ComponentTypeHTTPPost,
		Parameters: map[string]interface{}{
			"url":          server.URL,
			"content_type": "application/json",
		},
		Timeout:         5 * time.Second,
		ContinueOnError: false,
	}

	component, err := NewHTTPPostComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	input := make(chan pipeline.Data, 1)
	input <- pipeline.Data{
		Payload:   []byte(`{"test": "data"}`),
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   "test-trace",
	}
	close(input)

	output, err := component.Execute(ctx, input)
	if err != nil {
		t.Fatalf("failed to execute component: %v", err)
	}

	// Output channel should close quickly due to error
	select {
	case _, ok := <-output:
		if ok {
			t.Errorf("expected output channel to be closed due to error")
		}
	case <-time.After(2 * time.Second):
		t.Errorf("timeout waiting for output channel to close")
	}
}

func TestHTTPPostComponent_Type(t *testing.T) {
	component := &HTTPPostComponent{
		config: pipeline.ComponentConfig{
			ID:   "test",
			Type: pipeline.ComponentTypeHTTPPost,
		},
	}

	if component.Type() != pipeline.ComponentTypeHTTPPost {
		t.Errorf("expected type %s, got %s", pipeline.ComponentTypeHTTPPost, component.Type())
	}
}

func TestHTTPPostComponent_ID(t *testing.T) {
	expectedID := "test-post-component-123"
	component := &HTTPPostComponent{
		config: pipeline.ComponentConfig{
			ID: expectedID,
		},
	}

	if component.ID() != expectedID {
		t.Errorf("expected ID %s, got %s", expectedID, component.ID())
	}
}

func TestHTTPPostComponent_Config(t *testing.T) {
	config := pipeline.ComponentConfig{
		ID:   "test",
		Type: pipeline.ComponentTypeHTTPPost,
		Parameters: map[string]interface{}{
			"url":          "https://api.example.com",
			"content_type": "application/json",
		},
	}

	component := &HTTPPostComponent{
		config: config,
	}

	returnedConfig := component.Config()
	if returnedConfig.ID != config.ID {
		t.Errorf("expected config ID %s, got %s", config.ID, returnedConfig.ID)
	}
	if returnedConfig.Type != config.Type {
		t.Errorf("expected config Type %s, got %s", config.Type, returnedConfig.Type)
	}
}
