package pipeline

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// GraphQLSyncConfig holds configuration for the GraphQL pipeline sync
type GraphQLSyncConfig struct {
	Endpoint string
	Username string
	Password string
}

// graphqlRequest is the payload sent to the GraphQL endpoint
type graphqlRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// --- Login response types ---

type loginResponse struct {
	Data   loginData      `json:"data"`
	Errors []graphqlError `json:"errors,omitempty"`
}

type loginData struct {
	UserLogin loginResult `json:"userlogin"`
}

type loginResult struct {
	Status         string `json:"status"`
	MessageType    string `json:"messageType"`
	Message        string `json:"message"`
	AccessToken    string `json:"accessToken"`
	SessionTimeout int    `json:"sessionTimeout"`
	Data           any    `json:"data"`
}

// --- Pipeline query response types ---

type pipelineResponse struct {
	Data   pipelineData   `json:"data"`
	Errors []graphqlError `json:"errors,omitempty"`
}

type pipelineData struct {
	ActivePipelines []graphqlPipeline `json:"activePipelines"`
}

// --- Logout response types ---

type logoutResponse struct {
	Data   logoutData     `json:"data"`
	Errors []graphqlError `json:"errors,omitempty"`
}

type logoutData struct {
	Logout json.RawMessage `json:"logout"`
}

// --- Common types ---

type graphqlError struct {
	Message string `json:"message"`
}

type graphqlPipeline struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	ExecutionMode  string `json:"execution_mode"`
	CronExpression string `json:"cron_expression"`
	Status         string `json:"status"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// shared HTTP client — skips TLS verification for self-signed certificates
var graphqlHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	},
}

// ResolveGraphQLConfig builds the sync config from environment variables with
// fallback to the values provided in the config file / defaults.
func ResolveGraphQLConfig(endpoint, username, password string) GraphQLSyncConfig {
	if env := os.Getenv("GRAPHQL_ENDPOINT"); env != "" {
		endpoint = env
	}
	if endpoint == "" {
		endpoint = "https://192.168.1.230/api"
	}

	if env := os.Getenv("GRAPHQL_USERNAME"); env != "" {
		username = env
	}
	if username == "" {
		username = "integration"
	}

	if env := os.Getenv("GRAPHQL_PASSWORD"); env != "" {
		password = env
	}
	if password == "" {
		password = "Nebula=2020"
	}

	return GraphQLSyncConfig{
		Endpoint: endpoint,
		Username: username,
		Password: password,
	}
}

// SyncPipelinesFromGraphQL fetches active pipelines from the remote GraphQL
// endpoint and upserts them into the local SQLite repository. It is intended
// to be called once during service startup.
//
// Flow: login → fetch pipelines → logout
func SyncPipelinesFromGraphQL(ctx context.Context, cfg GraphQLSyncConfig, repo PipelineRepository, logger *zerolog.Logger) error {
	fmt.Printf("⚙ [GraphQL Sync] ▶ Starting pipeline sync — endpoint=%s username=%s\n", cfg.Endpoint, cfg.Username)
	logger.Info().
		Str("endpoint", cfg.Endpoint).
		Str("username", cfg.Username).
		Msg("[GraphQL Sync] ▶ Starting pipeline sync from remote GraphQL endpoint")

	// Step 1 — Login to obtain JWT token
	fmt.Println("⚙ [GraphQL Sync] Step 1/4 — Sending login mutation...")
	logger.Info().Str("endpoint", cfg.Endpoint).Msg("[GraphQL Sync] Step 1/4 — Sending login mutation")
	token, err := graphqlLogin(ctx, cfg, logger)
	if err != nil {
		fmt.Printf("⚙ [GraphQL Sync] ✖ Login failed: %v\n", err)
		logger.Error().Err(err).Msg("[GraphQL Sync] ✖ Login failed — aborting sync")
		return fmt.Errorf("graphql sync: login failed: %w", err)
	}
	fmt.Println("⚙ [GraphQL Sync] ✔ Login successful — JWT token obtained")
	logger.Info().Msg("[GraphQL Sync] ✔ Login successful — JWT token obtained")

	// Step 2 — Fetch active pipelines using the JWT token
	fmt.Println("⚙ [GraphQL Sync] Step 2/4 — Fetching active pipelines...")
	logger.Info().Msg("[GraphQL Sync] Step 2/4 — Fetching active pipelines")
	pipelines, err := fetchActivePipelines(ctx, cfg.Endpoint, token, logger)
	if err != nil {
		fmt.Printf("⚙ [GraphQL Sync] ✖ Failed to fetch pipelines: %v\n", err)
		logger.Error().Err(err).Msg("[GraphQL Sync] ✖ Failed to fetch pipelines — attempting logout before aborting")
		_ = graphqlLogout(ctx, cfg.Endpoint, token, cfg.Username, logger)
		return fmt.Errorf("graphql sync: failed to fetch pipelines: %w", err)
	}
	fmt.Printf("⚙ [GraphQL Sync] ✔ Fetched %d pipelines\n", len(pipelines))
	logger.Info().Int("count", len(pipelines)).Msg("[GraphQL Sync] ✔ Pipelines fetched successfully")

	// Step 3 — Logout immediately
	fmt.Println("⚙ [GraphQL Sync] Step 3/4 — Sending logout mutation...")
	logger.Info().Msg("[GraphQL Sync] Step 3/4 — Sending logout mutation")
	if err := graphqlLogout(ctx, cfg.Endpoint, token, cfg.Username, logger); err != nil {
		fmt.Printf("⚙ [GraphQL Sync] ⚠ Logout failed: %v\n", err)
		logger.Warn().Err(err).Msg("[GraphQL Sync] ⚠ Logout failed — session will expire on its own")
	} else {
		fmt.Println("⚙ [GraphQL Sync] ✔ Logout successful")
		logger.Info().Msg("[GraphQL Sync] ✔ Logout successful")
	}

	// Step 4 — Upsert pipelines into local SQLite
	fmt.Printf("⚙ [GraphQL Sync] Step 4/4 — Upserting %d pipelines into local SQLite...\n", len(pipelines))
	logger.Info().Int("count", len(pipelines)).Msg("[GraphQL Sync] Step 4/4 — Upserting pipelines into local SQLite")
	created, updated, skipped := 0, 0, 0
	for _, gp := range pipelines {
		logger.Debug().
			Str("pipeline_id", gp.ID).
			Str("name", gp.Name).
			Str("status", gp.Status).
			Str("execution_mode", gp.ExecutionMode).
			Msg("[GraphQL Sync] Processing pipeline")

		def, err := toDefinition(gp)
		if err != nil {
			skipped++
			logger.Warn().Err(err).Str("pipeline", gp.Name).Msg("[GraphQL Sync] ⚠ Skipping pipeline — conversion error")
			continue
		}

		existing, readErr := repo.Read(def.ID)
		if readErr == nil {
			def.Components = existing.Components
			def.Connections = existing.Connections
			if updateErr := repo.Update(def); updateErr != nil {
				skipped++
				logger.Error().Err(updateErr).Str("pipeline_id", def.ID).Str("name", def.Name).Msg("[GraphQL Sync] ✖ Failed to update pipeline in SQLite")
			} else {
				updated++
				logger.Info().Str("pipeline_id", def.ID).Str("name", def.Name).Msg("[GraphQL Sync] ✔ Updated existing pipeline")
			}
		} else {
			logger.Debug().Err(readErr).Str("pipeline_id", def.ID).Msg("[GraphQL Sync] Pipeline not found locally — creating new entry")
			if createErr := repo.Create(def); createErr != nil {
				skipped++
				logger.Error().Err(createErr).Str("pipeline_id", def.ID).Str("name", def.Name).Msg("[GraphQL Sync] ✖ Failed to create pipeline in SQLite")
			} else {
				created++
				logger.Info().Str("pipeline_id", def.ID).Str("name", def.Name).Msg("[GraphQL Sync] ✔ Created new pipeline")
			}
		}
	}

	logger.Info().
		Int("total", len(pipelines)).
		Int("created", created).
		Int("updated", updated).
		Int("skipped", skipped).
		Msg("[GraphQL Sync] ■ Pipeline sync completed")
	fmt.Printf("⚙ [GraphQL Sync] ■ Pipeline sync completed — total=%d created=%d updated=%d skipped=%d\n", len(pipelines), created, updated, skipped)
	return nil
}

// graphqlLogin authenticates against the GraphQL endpoint and returns the JWT
// access token.
func graphqlLogin(ctx context.Context, cfg GraphQLSyncConfig, logger *zerolog.Logger) (string, error) {
	fmt.Printf("⚙ [GraphQL Login] Connecting to %s as user '%s'\n", cfg.Endpoint, cfg.Username)
	query := `mutation UserLogin($username: String!, $password: String!) {
		userlogin(username: $username, password: $password) {
			status
			messageType
			message
			accessToken
			sessionTimeout
			data
		}
	}`

	payload := graphqlRequest{
		Query: query,
		Variables: map[string]interface{}{
			"username": cfg.Username,
			"password": cfg.Password,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Login] Failed to marshal login request body")
		return "", fmt.Errorf("failed to marshal login request: %w", err)
	}

	logger.Debug().Str("endpoint", cfg.Endpoint).Str("username", cfg.Username).Msg("[GraphQL Login] Sending POST request")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Login] Failed to create HTTP request")
		return "", fmt.Errorf("failed to create login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := graphqlHTTPClient.Do(req)
	if err != nil {
		fmt.Printf("⚙ [GraphQL Login] ✖ HTTP request failed: %v\n", err)
		logger.Error().Err(err).Str("endpoint", cfg.Endpoint).Msg("[GraphQL Login] HTTP request failed")
		return "", fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("⚙ [GraphQL Login] Response status: %d\n", resp.StatusCode)
	logger.Debug().Int("status_code", resp.StatusCode).Msg("[GraphQL Login] Received response")

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Login] Failed to read response body")
		return "", fmt.Errorf("failed to read login response: %w", err)
	}

	logger.Debug().Str("response_body", string(respBody)).Msg("[GraphQL Login] Raw response")

	if resp.StatusCode != http.StatusOK {
		logger.Error().Int("status_code", resp.StatusCode).Str("body", string(respBody)).Msg("[GraphQL Login] Non-200 status code")
		return "", fmt.Errorf("login endpoint returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var loginResp loginResponse
	if err := json.Unmarshal(respBody, &loginResp); err != nil {
		logger.Error().Err(err).Str("body", string(respBody)).Msg("[GraphQL Login] Failed to decode JSON response")
		return "", fmt.Errorf("failed to decode login response: %w", err)
	}

	if len(loginResp.Errors) > 0 {
		logger.Error().Str("graphql_error", loginResp.Errors[0].Message).Msg("[GraphQL Login] GraphQL returned errors")
		return "", fmt.Errorf("login graphql errors: %s", loginResp.Errors[0].Message)
	}

	if loginResp.Data.UserLogin.AccessToken == "" {
		logger.Error().
			Str("status", loginResp.Data.UserLogin.Status).
			Str("message", loginResp.Data.UserLogin.Message).
			Msg("[GraphQL Login] No access token in response")
		return "", fmt.Errorf("login succeeded but no access token returned (status: %s, message: %s)",
			loginResp.Data.UserLogin.Status, loginResp.Data.UserLogin.Message)
	}

	logger.Debug().
		Str("status", loginResp.Data.UserLogin.Status).
		Int("session_timeout", loginResp.Data.UserLogin.SessionTimeout).
		Msg("[GraphQL Login] Token received")

	return loginResp.Data.UserLogin.AccessToken, nil
}

// fetchActivePipelines calls the GetActivePipelines query using the provided
// JWT bearer token.
func fetchActivePipelines(ctx context.Context, endpoint, token string, logger *zerolog.Logger) ([]graphqlPipeline, error) {
	query := `query GetActivePipelines {
		activePipelines {
			id
			name
			description
			execution_mode
			cron_expression
			status
			created_at
			updated_at
		}
	}`

	body, err := json.Marshal(graphqlRequest{Query: query})
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Pipelines] Failed to marshal pipeline query request")
		return nil, fmt.Errorf("failed to marshal pipeline request: %w", err)
	}

	logger.Debug().Str("endpoint", endpoint).Msg("[GraphQL Pipelines] Sending POST request with Bearer token")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Pipelines] Failed to create HTTP request")
		return nil, fmt.Errorf("failed to create pipeline request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := graphqlHTTPClient.Do(req)
	if err != nil {
		logger.Error().Err(err).Str("endpoint", endpoint).Msg("[GraphQL Pipelines] HTTP request failed")
		return nil, fmt.Errorf("pipeline request failed: %w", err)
	}
	defer resp.Body.Close()

	logger.Debug().Int("status_code", resp.StatusCode).Msg("[GraphQL Pipelines] Received response")

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Pipelines] Failed to read response body")
		return nil, fmt.Errorf("failed to read pipeline response: %w", err)
	}

	fmt.Printf("⚙ [GraphQL Pipelines] Raw response body: %s\n", string(respBody))
	logger.Debug().Str("response_body", string(respBody)).Msg("[GraphQL Pipelines] Raw response")

	if resp.StatusCode != http.StatusOK {
		logger.Error().Int("status_code", resp.StatusCode).Str("body", string(respBody)).Msg("[GraphQL Pipelines] Non-200 status code")
		return nil, fmt.Errorf("pipeline endpoint returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var gqlResp pipelineResponse
	if err := json.Unmarshal(respBody, &gqlResp); err != nil {
		logger.Error().Err(err).Str("body", string(respBody)).Msg("[GraphQL Pipelines] Failed to decode JSON response")
		return nil, fmt.Errorf("failed to decode pipeline response: %w", err)
	}

	if len(gqlResp.Errors) > 0 {
		logger.Error().Str("graphql_error", gqlResp.Errors[0].Message).Msg("[GraphQL Pipelines] GraphQL returned errors")
		return nil, fmt.Errorf("pipeline graphql errors: %s", gqlResp.Errors[0].Message)
	}

	for i, p := range gqlResp.Data.ActivePipelines {
		logger.Debug().
			Int("index", i).
			Str("id", p.ID).
			Str("name", p.Name).
			Str("status", p.Status).
			Str("execution_mode", p.ExecutionMode).
			Str("cron", p.CronExpression).
			Msg("[GraphQL Pipelines] Pipeline received")
	}

	return gqlResp.Data.ActivePipelines, nil
}

// graphqlLogout calls the Logout mutation to invalidate the session.
func graphqlLogout(ctx context.Context, endpoint, token, username string, logger *zerolog.Logger) error {
	query := `mutation Logout($username: String!) {
		logout(username: $username)
	}`

	payload := graphqlRequest{
		Query: query,
		Variables: map[string]interface{}{
			"username": username,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Logout] Failed to marshal logout request body")
		return fmt.Errorf("failed to marshal logout request: %w", err)
	}

	logger.Debug().Str("endpoint", endpoint).Str("username", username).Msg("[GraphQL Logout] Sending POST request")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		logger.Error().Err(err).Msg("[GraphQL Logout] Failed to create HTTP request")
		return fmt.Errorf("failed to create logout request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := graphqlHTTPClient.Do(req)
	if err != nil {
		logger.Error().Err(err).Str("endpoint", endpoint).Msg("[GraphQL Logout] HTTP request failed")
		return fmt.Errorf("logout request failed: %w", err)
	}
	defer resp.Body.Close()

	logger.Debug().Int("status_code", resp.StatusCode).Msg("[GraphQL Logout] Received response")

	respBody, _ := io.ReadAll(resp.Body)
	logger.Debug().Str("response_body", string(respBody)).Msg("[GraphQL Logout] Raw response")

	if resp.StatusCode != http.StatusOK {
		logger.Error().Int("status_code", resp.StatusCode).Str("body", string(respBody)).Msg("[GraphQL Logout] Non-200 status code")
		return fmt.Errorf("logout endpoint returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// toDefinition converts a graphqlPipeline to a PipelineDefinition.
// Pipelines fetched from the remote GraphQL endpoint are treated as active.
// The execution_mode is normalised to a known value; defaults to "scheduled"
// when a cron_expression is present, otherwise "continuous".
func toDefinition(gp graphqlPipeline) (PipelineDefinition, error) {
	// Normalise execution mode from whatever casing the remote sends
	execMode := ExecutionMode(strings.ToLower(strings.TrimSpace(gp.ExecutionMode)))
	if execMode != ExecutionModeScheduled && execMode != ExecutionModeContinuous {
		// Infer from cron expression
		if strings.TrimSpace(gp.CronExpression) != "" {
			execMode = ExecutionModeScheduled
		} else {
			execMode = ExecutionModeContinuous
		}
	}

	// Always mark as active — these pipelines came from the active set
	status := PipelineStatusActive

	def := PipelineDefinition{
		ID:             gp.ID,
		Name:           gp.Name,
		Description:    gp.Description,
		ExecutionMode:  execMode,
		CronExpression: gp.CronExpression,
		Status:         status,
		Components:     []ComponentConfig{},
		Connections:    []Connection{},
	}

	if t, err := time.Parse(time.RFC3339, gp.CreatedAt); err == nil {
		def.CreatedAt = t
	} else {
		def.CreatedAt = time.Now()
	}

	if t, err := time.Parse(time.RFC3339, gp.UpdatedAt); err == nil {
		def.UpdatedAt = t
	} else {
		def.UpdatedAt = time.Now()
	}

	return def, nil
}
