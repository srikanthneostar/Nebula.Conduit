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
	Query string `json:"query"`
}

// graphqlResponse is the top-level response from the GraphQL endpoint
type graphqlResponse struct {
	Data   graphqlData    `json:"data"`
	Errors []graphqlError `json:"errors,omitempty"`
}

type graphqlData struct {
	ActivePipelines []graphqlPipeline `json:"activePipelines"`
}

type graphqlError struct {
	Message string `json:"message"`
}

type graphqlPipeline struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	ExecutionMode  string `json:"executionMode"`
	CronExpression string `json:"cronExpression"`
	Status         string `json:"status"`
	CreatedAt      string `json:"createdAt"`
	UpdatedAt      string `json:"updatedAt"`
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
func SyncPipelinesFromGraphQL(ctx context.Context, cfg GraphQLSyncConfig, repo PipelineRepository, logger *zerolog.Logger) error {
	logger.Info().Str("endpoint", cfg.Endpoint).Msg("Starting GraphQL pipeline sync")

	pipelines, err := fetchActivePipelines(ctx, cfg)
	if err != nil {
		return fmt.Errorf("graphql sync: failed to fetch pipelines: %w", err)
	}

	logger.Info().Int("count", len(pipelines)).Msg("Fetched pipelines from GraphQL endpoint")

	for _, gp := range pipelines {
		def, err := toDefinition(gp)
		if err != nil {
			logger.Warn().Err(err).Str("pipeline", gp.Name).Msg("Skipping pipeline due to conversion error")
			continue
		}

		// Try to read existing pipeline — update if found, create otherwise
		existing, readErr := repo.Read(def.ID)
		if readErr == nil {
			def.Components = existing.Components
			def.Connections = existing.Connections
			if updateErr := repo.Update(def); updateErr != nil {
				logger.Error().Err(updateErr).Str("pipeline_id", def.ID).Msg("Failed to update pipeline")
			} else {
				logger.Info().Str("pipeline_id", def.ID).Str("name", def.Name).Msg("Updated pipeline from GraphQL")
			}
		} else {
			if createErr := repo.Create(def); createErr != nil {
				logger.Error().Err(createErr).Str("pipeline_id", def.ID).Msg("Failed to create pipeline")
			} else {
				logger.Info().Str("pipeline_id", def.ID).Str("name", def.Name).Msg("Created pipeline from GraphQL")
			}
		}
	}

	logger.Info().Msg("GraphQL pipeline sync completed")
	return nil
}

// fetchActivePipelines calls the GraphQL endpoint and returns the list of
// active pipelines.
func fetchActivePipelines(ctx context.Context, cfg GraphQLSyncConfig) ([]graphqlPipeline, error) {
	query := `query GetActivePipelines {
		activePipelines {
			id
			name
			description
			executionMode
			cronExpression
			status
			createdAt
			updatedAt
		}
	}`

	body, err := json.Marshal(graphqlRequest{Query: query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(cfg.Username, cfg.Password)

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("graphql request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("graphql endpoint returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var gqlResp graphqlResponse
	if err := json.Unmarshal(respBody, &gqlResp); err != nil {
		return nil, fmt.Errorf("failed to decode graphql response: %w", err)
	}

	if len(gqlResp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %s", gqlResp.Errors[0].Message)
	}

	return gqlResp.Data.ActivePipelines, nil
}

// toDefinition converts a graphqlPipeline to a PipelineDefinition.
func toDefinition(gp graphqlPipeline) (PipelineDefinition, error) {
	def := PipelineDefinition{
		ID:             gp.ID,
		Name:           gp.Name,
		Description:    gp.Description,
		ExecutionMode:  ExecutionMode(gp.ExecutionMode),
		CronExpression: gp.CronExpression,
		Status:         PipelineStatus(gp.Status),
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
