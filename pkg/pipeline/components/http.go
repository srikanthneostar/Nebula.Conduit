package components

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	stateStore pipeline.StateStore
	pipelineID string
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

		// Graph wiring determines mode: nil input means source mode, non-nil input
		// means processor mode and we must wait for upstream data deterministically.
		if input != nil {
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
			if err := h.fetchAndSend(ctx, output); err != nil {
				fmt.Printf("HTTPGet[%s]: fetch failed: %v\n", h.config.ID, err)
			}
		}
	}()

	return output, nil
}

// fetchAndSend performs the HTTP GET request and sends data to output channel
func (h *HTTPGetComponent) fetchAndSend(ctx context.Context, output chan<- pipeline.Data) error {
	// Load persisted metadata from previous runs to resolve template variables
	persistedMeta := h.loadPersistedMetadata(ctx)

	// Resolve template variables in URL; strip query params with unresolved placeholders
	resolvedURL, decodedURL := resolveURLWithTemplates(h.url, persistedMeta)

	fmt.Printf("HTTPGet[%s]: decoded URL  = %s\n", h.config.ID, decodedURL)
	fmt.Printf("HTTPGet[%s]: encoded URL  = %s\n", h.config.ID, resolvedURL)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolvedURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers with template variable support from persisted state
	for key, value := range h.headers {
		resolvedValue := replaceTemplateVars(value, persistedMeta)
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
		TraceID:   h.config.ID,
	}

	// Seed metadata with persisted state so downstream components inherit it
	for k, v := range persistedMeta {
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

// fetchAndSendWithData performs HTTP GET with template variables from input data
func (h *HTTPGetComponent) fetchAndSendWithData(ctx context.Context, inputData pipeline.Data, output chan<- pipeline.Data) error {
	// Resolve template variables in URL; strip query params with unresolved placeholders
	resolvedURL, decodedURL := resolveURLWithTemplates(h.url, inputData.Metadata)

	fmt.Printf("HTTPGet[%s]: decoded URL  = %s\n", h.config.ID, decodedURL)
	fmt.Printf("HTTPGet[%s]: encoded URL  = %s\n", h.config.ID, resolvedURL)

	// Create HTTP request with resolved URL
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolvedURL, nil)
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

	// Persist metadata state for next run
	h.saveMetadataState(ctx, data.Metadata)

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

// SetStateStore injects the StateStore dependency for persisting metadata across runs.
func (h *HTTPGetComponent) SetStateStore(store pipeline.StateStore) {
	h.stateStore = store
}

// SetPipelineID injects the pipeline ID for state scoping.
func (h *HTTPGetComponent) SetPipelineID(pipelineID string) {
	h.pipelineID = pipelineID
}

// loadPersistedMetadata loads all previously saved state for this pipeline
// from any component and returns it as a metadata map. Used in source mode to resolve template vars.
func (h *HTTPGetComponent) loadPersistedMetadata(ctx context.Context) map[string]string {
	if h.stateStore == nil || h.pipelineID == "" {
		return nil
	}
	// Load state from all components in this pipeline — the json_extractor
	// persists values like max_modified under its own component ID, but
	// http_get needs them to resolve URL templates.
	state, err := h.stateStore.LoadAllState(ctx, h.pipelineID, "")
	if err != nil {
		fmt.Printf("HTTPGet[%s]: failed to load persisted state: %v\n", h.config.ID, err)
		return nil
	}
	if len(state) > 0 {
		fmt.Printf("HTTPGet[%s]: loaded %d persisted state key(s)\n", h.config.ID, len(state))
		for k, v := range state {
			fmt.Printf("HTTPGet[%s]:   %s = %s\n", h.config.ID, k, v)
		}
	}
	return state
}

// saveMetadataState persists metadata keys that match template variables in the URL.
func (h *HTTPGetComponent) saveMetadataState(ctx context.Context, metadata map[string]string) {
	if h.stateStore == nil || h.pipelineID == "" || len(metadata) == 0 {
		return
	}
	for k, v := range metadata {
		// Only persist keys that are referenced as template vars in the URL
		if strings.Contains(h.url, "{{"+k+"}}") {
			if err := h.stateStore.SaveState(ctx, h.pipelineID, h.config.ID, k, v); err != nil {
				fmt.Printf("HTTPGet[%s]: failed to persist state key %q: %v\n", h.config.ID, k, err)
			} else {
				fmt.Printf("HTTPGet[%s]: persisted state %s = %s\n", h.config.ID, k, v)
			}
		}
	}
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

// resolveURLWithTemplates resolves template variables in a URL and strips any
// query parameters that still contain unresolved {{...}} placeholders.
// The URL may be percent-encoded, so we decode parameter values before checking
// for placeholders, resolve templates, then re-encode the result.
// Returns (encodedURL, decodedURL) — encoded for the HTTP request, decoded for logging.
func resolveURLWithTemplates(rawURL string, metadata map[string]string) (string, string) {
	// Split URL into base and query string
	parts := strings.SplitN(rawURL, "?", 2)
	if len(parts) < 2 {
		// No query string — just replace in the base URL
		resolved := replaceTemplateVars(rawURL, metadata)
		if strings.Contains(resolved, "{{") {
			return rawURL, rawURL
		}
		return resolved, resolved
	}

	base := parts[0]
	queryStr := parts[1]

	// Parse query parameters properly (handles percent-encoding)
	params, err := url.ParseQuery(queryStr)
	if err != nil {
		// Fallback: plain string replacement if parsing fails
		fmt.Printf("HTTPGet: failed to parse query string, falling back to plain replacement: %v\n", err)
		fallback := replaceTemplateVars(rawURL, metadata)
		return fallback, fallback
	}

	// Build a new query string, resolving templates and stripping unresolved params
	result := url.Values{}
	for key, values := range params {
		keep := true
		resolved := make([]string, 0, len(values))
		for _, v := range values {
			// Replace template vars in the decoded value
			r := replaceTemplateVars(v, metadata)
			if strings.Contains(r, "{{") {
				fmt.Printf("HTTPGet: stripping query param %q — contains unresolved placeholder\n", key)
				keep = false
				break
			}
			resolved = append(resolved, r)
		}
		if keep {
			result[key] = resolved
		}
	}

	// Also resolve templates in the base URL (unlikely but safe)
	base = replaceTemplateVars(base, metadata)

	// Build the decoded version for logging (human-readable)
	var decodedParts []string
	for key, values := range result {
		for _, v := range values {
			decodedParts = append(decodedParts, key+"="+v)
		}
	}
	decoded := base
	if len(decodedParts) > 0 {
		decoded = base + "?" + strings.Join(decodedParts, "&")
	}

	// Build the encoded version for the actual HTTP request
	encoded := base
	if len(result) > 0 {
		encoded = base + "?" + result.Encode()
	}

	return encoded, decoded
}
