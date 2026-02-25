package pipeline

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// PipelineRepository provides database operations for pipeline definitions
type PipelineRepository interface {
	// Create stores a new pipeline definition
	Create(def PipelineDefinition) error

	// Read retrieves a pipeline definition by ID
	Read(id string) (PipelineDefinition, error)

	// Update modifies an existing pipeline definition
	Update(def PipelineDefinition) error

	// Delete removes a pipeline definition
	Delete(id string) error

	// List retrieves all pipeline definitions
	List() ([]PipelineDefinition, error)

	// ListActive retrieves all active pipeline definitions
	ListActive() ([]PipelineDefinition, error)
}

// SQLPipelineRepository implements PipelineRepository using SQLite
type SQLPipelineRepository struct {
	db *sql.DB
}

// NewSQLPipelineRepository creates a new SQLPipelineRepository
func NewSQLPipelineRepository(db *sql.DB) *SQLPipelineRepository {
	return &SQLPipelineRepository{db: db}
}

// Create stores a new pipeline definition with all components and connections
func (r *SQLPipelineRepository) Create(def PipelineDefinition) error {
	// Validate pipeline definition
	if err := ValidatePipelineDefinition(def); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Begin transaction for atomic operation
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Generate ID if not provided
	if def.ID == "" {
		def.ID = uuid.New().String()
	}

	// Set timestamps
	now := time.Now()
	if def.CreatedAt.IsZero() {
		def.CreatedAt = now
	}
	def.UpdatedAt = now

	// Insert pipeline
	_, err = tx.Exec(`
		INSERT INTO pipelines (id, name, description, execution_mode, cron_expression, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, def.ID, def.Name, def.Description, def.ExecutionMode, def.CronExpression, def.Status, def.CreatedAt, def.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert pipeline: %w", err)
	}

	// Insert components
	for i, comp := range def.Components {
		configJSON, err := json.Marshal(comp)
		if err != nil {
			return fmt.Errorf("failed to marshal component config: %w", err)
		}

		_, err = tx.Exec(`
			INSERT INTO pipeline_components (id, pipeline_id, component_type, component_config, position_in_graph)
			VALUES (?, ?, ?, ?, ?)
		`, comp.ID, def.ID, comp.Type, string(configJSON), i)
		if err != nil {
			return fmt.Errorf("failed to insert component %s: %w", comp.ID, err)
		}
	}

	// Insert connections
	for _, conn := range def.Connections {
		connID := uuid.New().String()
		_, err = tx.Exec(`
			INSERT INTO pipeline_connections (id, pipeline_id, source_component_id, target_component_id)
			VALUES (?, ?, ?, ?)
		`, connID, def.ID, conn.SourceComponentID, conn.TargetComponentID)
		if err != nil {
			return fmt.Errorf("failed to insert connection: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Read retrieves a pipeline definition by ID with all components and connections
func (r *SQLPipelineRepository) Read(id string) (PipelineDefinition, error) {
	var def PipelineDefinition

	// Query pipeline
	err := r.db.QueryRow(`
		SELECT id, name, description, execution_mode, cron_expression, status, created_at, updated_at
		FROM pipelines
		WHERE id = ?
	`, id).Scan(&def.ID, &def.Name, &def.Description, &def.ExecutionMode, &def.CronExpression, &def.Status, &def.CreatedAt, &def.UpdatedAt)
	if err == sql.ErrNoRows {
		return def, fmt.Errorf("pipeline not found: %s", id)
	}
	if err != nil {
		return def, fmt.Errorf("failed to query pipeline: %w", err)
	}

	// Query components
	rows, err := r.db.Query(`
		SELECT id, component_type, component_config, position_in_graph
		FROM pipeline_components
		WHERE pipeline_id = ?
		ORDER BY position_in_graph
	`, id)
	if err != nil {
		return def, fmt.Errorf("failed to query components: %w", err)
	}
	defer rows.Close()

	def.Components = []ComponentConfig{}
	for rows.Next() {
		var comp ComponentConfig
		var configJSON string
		var position int

		if err := rows.Scan(&comp.ID, &comp.Type, &configJSON, &position); err != nil {
			return def, fmt.Errorf("failed to scan component: %w", err)
		}

		if err := json.Unmarshal([]byte(configJSON), &comp); err != nil {
			return def, fmt.Errorf("failed to unmarshal component config: %w", err)
		}

		def.Components = append(def.Components, comp)
	}

	if err := rows.Err(); err != nil {
		return def, fmt.Errorf("error iterating components: %w", err)
	}

	// Query connections
	rows, err = r.db.Query(`
		SELECT source_component_id, target_component_id
		FROM pipeline_connections
		WHERE pipeline_id = ?
	`, id)
	if err != nil {
		return def, fmt.Errorf("failed to query connections: %w", err)
	}
	defer rows.Close()

	def.Connections = []Connection{}
	for rows.Next() {
		var conn Connection
		if err := rows.Scan(&conn.SourceComponentID, &conn.TargetComponentID); err != nil {
			return def, fmt.Errorf("failed to scan connection: %w", err)
		}
		def.Connections = append(def.Connections, conn)
	}

	if err := rows.Err(); err != nil {
		return def, fmt.Errorf("error iterating connections: %w", err)
	}

	return def, nil
}

// Update modifies an existing pipeline definition
func (r *SQLPipelineRepository) Update(def PipelineDefinition) error {
	// Validate pipeline definition
	if err := ValidatePipelineDefinition(def); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Begin transaction for atomic operation
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update timestamp
	def.UpdatedAt = time.Now()

	// Update pipeline
	result, err := tx.Exec(`
		UPDATE pipelines
		SET name = ?, description = ?, execution_mode = ?, cron_expression = ?, status = ?, updated_at = ?
		WHERE id = ?
	`, def.Name, def.Description, def.ExecutionMode, def.CronExpression, def.Status, def.UpdatedAt, def.ID)
	if err != nil {
		return fmt.Errorf("failed to update pipeline: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("pipeline not found: %s", def.ID)
	}

	// Delete existing components and connections (cascade will handle connections)
	_, err = tx.Exec("DELETE FROM pipeline_components WHERE pipeline_id = ?", def.ID)
	if err != nil {
		return fmt.Errorf("failed to delete existing components: %w", err)
	}

	// Insert updated components
	for i, comp := range def.Components {
		configJSON, err := json.Marshal(comp)
		if err != nil {
			return fmt.Errorf("failed to marshal component config: %w", err)
		}

		_, err = tx.Exec(`
			INSERT INTO pipeline_components (id, pipeline_id, component_type, component_config, position_in_graph)
			VALUES (?, ?, ?, ?, ?)
		`, comp.ID, def.ID, comp.Type, string(configJSON), i)
		if err != nil {
			return fmt.Errorf("failed to insert component %s: %w", comp.ID, err)
		}
	}

	// Insert updated connections
	for _, conn := range def.Connections {
		connID := uuid.New().String()
		_, err = tx.Exec(`
			INSERT INTO pipeline_connections (id, pipeline_id, source_component_id, target_component_id)
			VALUES (?, ?, ?, ?)
		`, connID, def.ID, conn.SourceComponentID, conn.TargetComponentID)
		if err != nil {
			return fmt.Errorf("failed to insert connection: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Delete removes a pipeline definition (cascade will delete components and connections)
func (r *SQLPipelineRepository) Delete(id string) error {
	result, err := r.db.Exec("DELETE FROM pipelines WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete pipeline: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("pipeline not found: %s", id)
	}

	return nil
}

// List retrieves all pipeline definitions
func (r *SQLPipelineRepository) List() ([]PipelineDefinition, error) {
	rows, err := r.db.Query(`
		SELECT id, name, description, execution_mode, cron_expression, status, created_at, updated_at
		FROM pipelines
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query pipelines: %w", err)
	}
	defer rows.Close()

	var pipelineIDs []string
	pipelineMap := make(map[string]PipelineDefinition)

	for rows.Next() {
		var def PipelineDefinition
		if err := rows.Scan(&def.ID, &def.Name, &def.Description, &def.ExecutionMode, &def.CronExpression, &def.Status, &def.CreatedAt, &def.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan pipeline: %w", err)
		}
		pipelineIDs = append(pipelineIDs, def.ID)
		pipelineMap[def.ID] = def
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pipelines: %w", err)
	}

	// Load components and connections for each pipeline after closing the rows
	var pipelines []PipelineDefinition
	for _, id := range pipelineIDs {
		fullDef, err := r.Read(id)
		if err != nil {
			return nil, fmt.Errorf("failed to load pipeline details: %w", err)
		}
		pipelines = append(pipelines, fullDef)
	}

	return pipelines, nil
}

// ListActive retrieves all active pipeline definitions
func (r *SQLPipelineRepository) ListActive() ([]PipelineDefinition, error) {
	rows, err := r.db.Query(`
		SELECT id, name, description, execution_mode, cron_expression, status, created_at, updated_at
		FROM pipelines
		WHERE status = ?
		ORDER BY created_at DESC
	`, PipelineStatusActive)
	if err != nil {
		return nil, fmt.Errorf("failed to query active pipelines: %w", err)
	}
	defer rows.Close()

	var pipelineIDs []string
	pipelineMap := make(map[string]PipelineDefinition)

	for rows.Next() {
		var def PipelineDefinition
		if err := rows.Scan(&def.ID, &def.Name, &def.Description, &def.ExecutionMode, &def.CronExpression, &def.Status, &def.CreatedAt, &def.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan pipeline: %w", err)
		}
		pipelineIDs = append(pipelineIDs, def.ID)
		pipelineMap[def.ID] = def
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pipelines: %w", err)
	}

	// Load components and connections for each pipeline after closing the rows
	var pipelines []PipelineDefinition
	for _, id := range pipelineIDs {
		fullDef, err := r.Read(id)
		if err != nil {
			return nil, fmt.Errorf("failed to load pipeline details: %w", err)
		}
		pipelines = append(pipelines, fullDef)
	}

	return pipelines, nil
}
