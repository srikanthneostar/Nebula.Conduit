-- Pipeline Engine Schema Migration
-- Creates tables for pipeline definitions, components, connections, and execution history

-- Pipelines table
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    execution_mode TEXT NOT NULL CHECK(execution_mode IN ('scheduled', 'continuous')),
    cron_expression TEXT,
    status TEXT NOT NULL CHECK(status IN ('active', 'inactive')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline components table
CREATE TABLE IF NOT EXISTS pipeline_components (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    component_type TEXT NOT NULL,
    component_config TEXT NOT NULL, -- JSON
    position_in_graph INTEGER NOT NULL,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

-- Pipeline connections table
CREATE TABLE IF NOT EXISTS pipeline_connections (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    source_component_id TEXT NOT NULL,
    target_component_id TEXT NOT NULL,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
    FOREIGN KEY (source_component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE,
    FOREIGN KEY (target_component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE
);

-- Pipeline executions table
CREATE TABLE IF NOT EXISTS pipeline_executions (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'stopped')),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    error_message TEXT,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

-- Component executions table
CREATE TABLE IF NOT EXISTS component_executions (
    id TEXT PRIMARY KEY,
    pipeline_execution_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'stopped')),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    output_data_size INTEGER,
    error_message TEXT,
    FOREIGN KEY (pipeline_execution_id) REFERENCES pipeline_executions(id) ON DELETE CASCADE,
    FOREIGN KEY (component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE
);

-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_components_pipeline_id ON pipeline_components(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_connections_pipeline_id ON pipeline_connections(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_pipeline_id ON pipeline_executions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_started_at ON pipeline_executions(started_at);
CREATE INDEX IF NOT EXISTS idx_component_executions_pipeline_execution_id ON component_executions(pipeline_execution_id);
