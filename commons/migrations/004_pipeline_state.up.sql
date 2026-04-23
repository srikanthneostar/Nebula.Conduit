-- Pipeline component state table
-- Persists metadata (e.g. checkpoint cursors) between pipeline runs.
CREATE TABLE IF NOT EXISTS pipeline_component_state (
    pipeline_id  TEXT NOT NULL,
    component_id TEXT NOT NULL,
    state_key    TEXT NOT NULL,
    state_value  TEXT NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline_id, component_id, state_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_component_state_lookup
    ON pipeline_component_state(pipeline_id, component_id);
