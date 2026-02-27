# Pipeline API Setup

## Automatic Initialization

The pipeline engine is fully initialized inside `NewServer()` in `internal/api/server.go`. No manual setup steps are required.

On server startup, `NewServer()` automatically:
1. Creates the component factory and registers all built-in components
2. Creates the pipeline repository, metrics collector, and engine
3. Calls `pipelineEngine.Initialize(ctx)` which starts the scheduler and loads all active pipelines
4. Registers pipeline API routes under the authenticated route group

The pipeline engine is stored as a field on the `Server` struct (`s.pipelineEngine`) and passed to `pipeline.RegisterRoutes()`.

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

# Create pipeline with archive support
curl -k -X POST https://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d @docs/sample_pipeline_csv_with_archive.json

# Trigger pipeline (replace PIPELINE_ID)
curl -k -X POST https://localhost:8080/api/v1/pipelines/PIPELINE_ID/trigger \
  -H "Authorization: Bearer YOUR_TOKEN"

# List pipelines
curl -k https://localhost:8080/api/v1/pipelines \
  -H "Authorization: Bearer YOUR_TOKEN"

# Export all pipelines
curl -k https://localhost:8080/api/v1/pipelines/export \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -o pipelines-export.json

# Import pipelines
curl -k -X POST https://localhost:8080/api/v1/pipelines/import \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d @pipelines-export.json
```

## Sample Pipelines

Two sample pipeline JSON files are provided in the `docs/` folder:

| File | Description |
|------|-------------|
| `sample_pipeline_csv_to_log.json` | Basic CSV → attribute_update → log_sink pipeline |
| `sample_pipeline_csv_with_archive.json` | Same flow but with `archive_on_read` and `move_on_error` enabled on the CSV reader |

Both use `attribute_update` to set `{{variables}}` that `log_sink` resolves at runtime.