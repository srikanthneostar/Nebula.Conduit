package components

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// HTTPGetComponent fetches data via HTTP GET requests
type HTTPGetComponent struct {
	config     pipeline.ComponentConfig
	url        string
	headers    map[string]string
	interval   time.Duration
	httpClient *http.Client
}

// NewHTTPGetComponent creates a new HTTP GET component
func NewHTTPGetComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract URL from parameters
	url, ok := config.Parameters["url"].(string)
	if !ok || url == "" {
		return nil, fmt.Errorf("url parameter is required and must be a string")
	}

	// Extract headers (optional)
	headers := make(map[string]string)
	if headersParam, ok := config.Parameters["headers"].(map[string]interface{}); ok {
		for k, v := range headersParam {
			if strVal, ok := v.(string); ok {
				headers[k] = strVal
			}
		}
	}

	// Extract interval (optional, defaults to 0 for one-time execution)
	var interval time.Duration
	if intervalParam, ok := config.Parameters["interval"].(string); ok {
		parsedInterval, err := time.ParseDuration(intervalParam)
		if err != nil {
			return nil, fmt.Errorf("invalid interval format: %w", err)
		}
		interval = parsedInterval
	}

	// Create HTTP client with timeout
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second // Default timeout
	}

	httpClient := &http.Client{
		Timeout: timeout,
	}

	return &HTTPGetComponent{
		config:     config,
		url:        url,
		headers:    headers,
		interval:   interval,
		httpClient: httpClient,
	}, nil
}

// Execute runs the HTTP GET component logic
func (h *HTTPGetComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		// Check if we have input data (processor mode with template variables)
		select {
		case data, ok := <-input:
			if ok {
				// Process with input data for template variables
				if err := h.fetchAndSendWithData(ctx, data, output); err != nil {
					if !h.config.ContinueOnError {
						return
					}
				}
				// Continue reading from input channel for more data
				for {
					select {
					case <-ctx.Done():
						return
					case data, ok := <-input:
						if !ok {
							return
						}
						if err := h.fetchAndSendWithData(ctx, data, output); err != nil {
							if !h.config.ContinueOnError {
								return
							}
						}
					}
				}
			}
		default:
			// No input, run as source component
		}

		// If interval is set, run periodically; otherwise run once
		if h.interval > 0 {
			ticker := time.NewTicker(h.interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := h.fetchAndSend(ctx, output); err != nil {
						// Log error but continue if continue_on_error is true
						if !h.config.ContinueOnError {
							return
						}
					}
				}
			}
		} else {
			// One-time execution
			_ = h.fetchAndSend(ctx, output)
		}
	}()

	return output, nil
}

// fetchAndSend performs the HTTP GET request and sends data to output channel
func (h *HTTPGetComponent) fetchAndSend(ctx context.Context, output chan<- pipeline.Data) error {
	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers (no template support for GET as it's a source component)
	for key, value := range h.headers {
		req.Header.Set(key, value)
	}

	// Execute request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Create data payload
	data := pipeline.Data{
		Payload:   body,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   h.config.ID,
	}

	// Add response metadata
	data.Metadata["status_code"] = fmt.Sprintf("%d", resp.StatusCode)
	data.Metadata["content_type"] = resp.Header.Get("Content-Type")
	data.Metadata["content_length"] = fmt.Sprintf("%d", len(body))

	// Send data to output channel
	select {
	case output <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fetchAndSendWithData performs HTTP GET with template variables from input data
func (h *HTTPGetComponent) fetchAndSendWithData(ctx context.Context, inputData pipeline.Data, output chan<- pipeline.Data) error {
	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers with template variable support
	for key, value := range h.headers {
		resolvedValue := replaceTemplateVars(value, inputData.Metadata)
		req.Header.Set(key, resolvedValue)
	}

	// Execute request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Create data payload
	data := pipeline.Data{
		Payload:   body,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   inputData.TraceID,
	}

	// Copy original metadata
	for k, v := range inputData.Metadata {
		data.Metadata[k] = v
	}

	// Add response metadata
	data.Metadata["status_code"] = fmt.Sprintf("%d", resp.StatusCode)
	data.Metadata["content_type"] = resp.Header.Get("Content-Type")
	data.Metadata["content_length"] = fmt.Sprintf("%d", len(body))

	// Send data to output channel
	select {
	case output <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Validate checks if the component configuration is valid
func (h *HTTPGetComponent) Validate() error {
	if h.url == "" {
		return fmt.Errorf("url is required")
	}

	// Validate URL format
	if _, err := http.NewRequest(http.MethodGet, h.url, nil); err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	return nil
}

// Type returns the component type identifier
func (h *HTTPGetComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeHTTPGet
}

// ID returns the unique component instance identifier
func (h *HTTPGetComponent) ID() string {
	return h.config.ID
}

// Config returns the component configuration
func (h *HTTPGetComponent) Config() pipeline.ComponentConfig {
	return h.config
}

// HTTPPostComponent sends data via HTTP POST requests
type HTTPPostComponent struct {
	config         pipeline.ComponentConfig
	url            string
	headers        map[string]string
	contentType    string
	body           string // Static body template
	httpClient     *http.Client
	extractJWT     bool   // Extract JWT from response
	jwtField       string // Field name to extract JWT from (default: "token")
	jwtMetadataKey string // Metadata key to store JWT (default: "jwt_token")
}

// NewHTTPPostComponent creates a new HTTP POST component
func NewHTTPPostComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract URL from parameters
	url, ok := config.Parameters["url"].(string)
	if !ok || url == "" {
		return nil, fmt.Errorf("url parameter is required and must be a string")
	}

	// Extract content type from parameters (optional, defaults to application/json)
	contentType := "application/json"
	if ct, ok := config.Parameters["content_type"].(string); ok && ct != "" {
		contentType = ct
	}

	// Extract static body template (optional)
	body := ""
	if bodyParam, ok := config.Parameters["body"].(string); ok {
		body = bodyParam
	}

	// Extract headers (optional)
	headers := make(map[string]string)
	if headersParam, ok := config.Parameters["headers"].(map[string]interface{}); ok {
		for k, v := range headersParam {
			if strVal, ok := v.(string); ok {
				headers[k] = strVal
			}
		}
	}

	// Extract JWT extraction flag (optional, defaults to false)
	extractJWT := false
	if extractParam, ok := config.Parameters["extract_jwt"].(bool); ok {
		extractJWT = extractParam
	}

	// Extract JWT field name (optional, defaults to "token")
	jwtField := "token"
	if fieldParam, ok := config.Parameters["jwt_field"].(string); ok && fieldParam != "" {
		jwtField = fieldParam
	}

	// Extract JWT metadata key (optional, defaults to "jwt_token")
	jwtMetadataKey := "jwt_token"
	if keyParam, ok := config.Parameters["jwt_metadata_key"].(string); ok && keyParam != "" {
		jwtMetadataKey = keyParam
	}

	// Create HTTP client with timeout
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second // Default timeout
	}

	httpClient := &http.Client{
		Timeout: timeout,
	}

	return &HTTPPostComponent{
		config:         config,
		url:            url,
		headers:        headers,
		contentType:    contentType,
		body:           body,
		httpClient:     httpClient,
		extractJWT:     extractJWT,
		jwtField:       jwtField,
		jwtMetadataKey: jwtMetadataKey,
	}, nil
}

// Execute runs the HTTP POST component logic
func (h *HTTPPostComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		// If body is set (static body), this is a source component
		if h.body != "" {
			// Execute once with static body
			if err := h.sendStaticBody(ctx, output); err != nil {
				if !h.config.ContinueOnError {
					return
				}
			}
			return
		}

		// Otherwise, process input data
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					return
				}

				if err := h.sendData(ctx, data, output); err != nil {
					if !h.config.ContinueOnError {
						return
					}
				}
			}
		}
	}()

	return output, nil
}

// sendStaticBody sends a POST request with static body (source component mode)
func (h *HTTPPostComponent) sendStaticBody(ctx context.Context, output chan<- pipeline.Data) error {
	// Create HTTP request with static body
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader([]byte(h.body)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add content type header
	req.Header.Set("Content-Type", h.contentType)

	// Add custom headers
	for key, value := range h.headers {
		req.Header.Set(key, value)
	}

	// Execute request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Create data payload
	data := pipeline.Data{
		Payload:   responseBody,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   h.config.ID,
	}

	// Add response metadata
	data.Metadata["status_code"] = fmt.Sprintf("%d", resp.StatusCode)
	data.Metadata["content_type"] = resp.Header.Get("Content-Type")

	// Extract JWT if configured
	if h.extractJWT {
		if err := h.extractJWTFromResponse(responseBody, &data); err != nil {
			return fmt.Errorf("failed to extract JWT: %w", err)
		}
	}

	// Send data to output channel
	select {
	case output <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendData performs the HTTP POST request with the given data
func (h *HTTPPostComponent) sendData(ctx context.Context, data pipeline.Data, output chan<- pipeline.Data) error {
	// Convert payload to bytes
	var body []byte
	switch v := data.Payload.(type) {
	case []byte:
		body = v
	case string:
		body = []byte(v)
	default:
		return fmt.Errorf("unsupported payload type: %T", data.Payload)
	}

	// Create HTTP request with body
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add content type header
	req.Header.Set("Content-Type", h.contentType)

	// Add custom headers (with template support from metadata)
	for key, value := range h.headers {
		// Replace template variables from metadata
		resolvedValue := replaceTemplateVars(value, data.Metadata)
		req.Header.Set(key, resolvedValue)
	}

	// Execute request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Create output data with response
	outputData := pipeline.Data{
		Payload:   responseBody,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   data.TraceID,
	}

	// Copy original metadata
	for k, v := range data.Metadata {
		outputData.Metadata[k] = v
	}

	// Add response metadata
	outputData.Metadata["status_code"] = fmt.Sprintf("%d", resp.StatusCode)
	outputData.Metadata["content_type"] = resp.Header.Get("Content-Type")

	// Send to output if channel exists
	if output != nil {
		select {
		case output <- outputData:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// extractJWTFromResponse extracts JWT token from JSON response
func (h *HTTPPostComponent) extractJWTFromResponse(responseBody []byte, data *pipeline.Data) error {
	// Parse JSON response
	var jsonResponse map[string]interface{}
	if err := json.Unmarshal(responseBody, &jsonResponse); err != nil {
		return fmt.Errorf("failed to parse JSON response: %w", err)
	}

	// Extract JWT token
	if token, ok := jsonResponse[h.jwtField].(string); ok && token != "" {
		data.Metadata[h.jwtMetadataKey] = token
		return nil
	}

	return fmt.Errorf("JWT field '%s' not found in response", h.jwtField)
}

// Validate checks if the component configuration is valid
func (h *HTTPPostComponent) Validate() error {
	if h.url == "" {
		return fmt.Errorf("url is required")
	}

	if h.contentType == "" {
		return fmt.Errorf("content_type is required")
	}

	// Validate URL format
	if _, err := http.NewRequest(http.MethodPost, h.url, nil); err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	return nil
}

// Type returns the component type identifier
func (h *HTTPPostComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeHTTPPost
}

// ID returns the unique component instance identifier
func (h *HTTPPostComponent) ID() string {
	return h.config.ID
}

// Config returns the component configuration
func (h *HTTPPostComponent) Config() pipeline.ComponentConfig {
	return h.config
}

// replaceTemplateVars replaces {{variable}} templates with values from metadata
func replaceTemplateVars(template string, metadata map[string]string) string {
	result := template
	for key, value := range metadata {
		placeholder := fmt.Sprintf("{{%s}}", key)
		result = strings.ReplaceAll(result, placeholder, value)
	}
	return result
}
