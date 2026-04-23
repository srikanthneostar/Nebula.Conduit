package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// StateStore provides read/write access to persisted component state
// that survives across pipeline runs (e.g. checkpoint cursors).
type StateStore interface {
	// SaveState persists a key-value pair for a pipeline component.
	SaveState(ctx context.Context, pipelineID, componentID, key, value string) error

	// LoadState retrieves a previously persisted value. Returns "" if not found.
	LoadState(ctx context.Context, pipelineID, componentID, key string) (string, error)

	// LoadAllState retrieves all persisted key-value pairs for a pipeline component.
	LoadAllState(ctx context.Context, pipelineID, componentID string) (map[string]string, error)
}

// StateStoreInjectable is implemented by components that need persisted state
// across pipeline runs (e.g. http_get for checkpoint cursors).
type StateStoreInjectable interface {
	SetStateStore(store StateStore)
	SetPipelineID(pipelineID string)
}

type defaultStateStore struct {
	db *sql.DB
}

// NewStateStore creates a new StateStore backed by the given database.
func NewStateStore(db *sql.DB) StateStore {
	return &defaultStateStore{db: db}
}

func (s *defaultStateStore) SaveState(ctx context.Context, pipelineID, componentID, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pipeline_component_state (pipeline_id, component_id, state_key, state_value, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(pipeline_id, component_id, state_key)
		 DO UPDATE SET state_value = excluded.state_value, updated_at = excluded.updated_at`,
		pipelineID, componentID, key, value, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("failed to save component state: %w", err)
	}
	return nil
}

func (s *defaultStateStore) LoadState(ctx context.Context, pipelineID, componentID, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx,
		`SELECT state_value FROM pipeline_component_state
		 WHERE pipeline_id = ? AND component_id = ? AND state_key = ?`,
		pipelineID, componentID, key,
	).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to load component state: %w", err)
	}
	return value, nil
}

func (s *defaultStateStore) LoadAllState(ctx context.Context, pipelineID, componentID string) (map[string]string, error) {
	var rows *sql.Rows
	var err error

	if componentID == "" {
		// Load state from all components in the pipeline
		rows, err = s.db.QueryContext(ctx,
			`SELECT state_key, state_value FROM pipeline_component_state
			 WHERE pipeline_id = ?`,
			pipelineID,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT state_key, state_value FROM pipeline_component_state
			 WHERE pipeline_id = ? AND component_id = ?`,
			pipelineID, componentID,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load component state: %w", err)
	}
	defer rows.Close()

	state := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("failed to scan state row: %w", err)
		}
		state[k] = v
	}
	return state, nil
}
