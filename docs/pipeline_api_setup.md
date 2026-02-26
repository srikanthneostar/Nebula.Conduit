# Pipeline API Setup

## Important: Wiring Pipeline Routes

The pipeline API routes need to be registered in the server. Add the following to `internal/api/server.go` in the `NewServer` function.

### Step 1: Add imports

Add these imports to `internal/api/server.go`:

```go
"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
"github.com/Xecutables/Nebula.Conduit/pkg/pipeline/components"
```

### Step 2: Add pipeline engine initialization

Add this code in `NewServer()` after the existing service initialization (after `s.taskService = ...`):

```go
// Initialize pipeline engine
pipelineFactory := pipeline.NewComponentFactory()
components.RegisterComponents(pipelineFactory)
pipelineRepo := pipeline.NewSQLPipelineRepository(db)
pipelineMetrics := pipeline.NewDefaultMetricsCollector()
pipelineConfig := pipeline.PipelineEngineConfig{
    MaxConcurrentInstances:    10,
    MaxGoroutinesPerInstance:  50,
    ExecutionHistoryRetention: 30,
}
pipelineEngine := pipeline.NewPipelineEngine(db, pipelineRepo, pipelineFactory, nil, nil, pipelineMetrics, pipelineConfig)
```

### Step 3: Register pipeline routes

Add this inside the protected routes group (after `r.Get("/tasks", s.handleListTasks)`):

```go
pipeline.RegisterRoutes(r, pipelineEngine)
```

### Step 4: Initialize engine on startup

Add this after `server := api.NewServer(db, cfg)` in `cmd/server/main.go`:

```go
// Initialize pipeline engine is handled internally
```

## Testing with Postman

1. Import `Nebula_Conduit_Pipeline_API.postman_collection.json` into Postman
2. Disable SSL verification in Postman (Settings > SSL certificate verification > OFF) since the server uses self-signed certs
3. Run requests in order:
   - Login first to get auth token (auto-saved to collection variable)
   - Create Pipeline — saves pipeline_id
   - Trigger Pipeline — saves instance_id
   - Check status, history, etc.

## Testing with curl

```bash
# Create pipeline
curl -k -X POST https://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d @docs/sample_pipeline_csv_to_log.json

# Trigger pipeline (replace PIPELINE_ID)
curl -k -X POST https://localhost:8080/api/v1/pipelines/PIPELINE_ID/trigger \
  -H "Authorization: Bearer YOUR_TOKEN"

# List pipelines
curl -k https://localhost:8080/api/v1/pipelines \
  -H "Authorization: Bearer YOUR_TOKEN"
```
