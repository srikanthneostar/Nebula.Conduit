-- Pipeline Logs table for print_log component
-- Stores log entries that can be queried via the API and displayed in the React frontend

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    execution_id TEXT,
    component_id TEXT NOT NULL,
    log_level TEXT NOT NULL DEFAULT 'info',
    message TEXT,
    payload TEXT,
    metadata TEXT,
    trace_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_pipeline_id ON pipeline_logs(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_execution_id ON pipeline_logs(execution_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_created_at ON pipeline_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_component_id ON pipeline_logs(component_id);
