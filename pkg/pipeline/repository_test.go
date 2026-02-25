package pipeline

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// setupTestDB creates an in-memory SQLite database for testing
func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	// Create tables
	schema := `
	CREATE TABLE pipelines (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		description TEXT,
		execution_mode TEXT NOT NULL CHECK(execution_mode IN ('scheduled', 'continuous')),
		cron_expression TEXT,
		status TEXT NOT NULL CHECK(status IN ('active', 'inactive')),
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE pipeline_components (
		id TEXT PRIMARY KEY,
		pipeline_id TEXT NOT NULL,
		component_type TEXT NOT NULL,
		component_config TEXT NOT NULL,
		position_in_graph INTEGER NOT NULL,
		FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
	);

	CREATE TABLE pipeline_connections (
		id TEXT PRIMARY KEY,
		pipeline_id TEXT NOT NULL,
		source_component_id TEXT NOT NULL,
		target_component_id TEXT NOT NULL,
		FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
		FOREIGN KEY (source_component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE,
		FOREIGN KEY (target_component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE
	);
	`

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	return db
}

// createTestPipelineDefinition creates a valid test pipeline definition
func createTestPipelineDefinition() PipelineDefinition {
	return PipelineDefinition{
		ID:             "test-pipeline-1",
		Name:           "Test Pipeline",
		Description:    "A test pipeline",
		ExecutionMode:  ExecutionModeScheduled,
		CronExpression: "0 */5 * * *",
		Status:         PipelineStatusActive,
		Components: []ComponentConfig{
			{
				ID:   "http-source-1",
				Type: ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
				},
				RetryCount: 3,
				Timeout:    30 * time.Second,
			},
			{
				ID:   "log-processor-1",
				Type: ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "info",
				},
			},
			{
				ID:   "http-sink-1",
				Type: ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url":          "https://api.example.com/output",
					"content_type": "application/json",
				},
				RetryCount: 5,
			},
		},
		Connections: []Connection{
			{
				SourceComponentID: "http-source-1",
				TargetComponentID: "log-processor-1",
			},
			{
				SourceComponentID: "log-processor-1",
				TargetComponentID: "http-sink-1",
			},
		},
	}
}

func TestRepositoryCreate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)
	def := createTestPipelineDefinition()

	// Test successful creation
	err := repo.Create(def)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Verify pipeline was created
	retrieved, err := repo.Read(def.ID)
	if err != nil {
		t.Fatalf("Failed to read created pipeline: %v", err)
	}

	if retrieved.ID != def.ID {
		t.Errorf("Expected ID %s, got %s", def.ID, retrieved.ID)
	}
	if retrieved.Name != def.Name {
		t.Errorf("Expected name %s, got %s", def.Name, retrieved.Name)
	}
	if len(retrieved.Components) != len(def.Components) {
		t.Errorf("Expected %d components, got %d", len(def.Components), len(retrieved.Components))
	}
	if len(retrieved.Connections) != len(def.Connections) {
		t.Errorf("Expected %d connections, got %d", len(def.Connections), len(retrieved.Connections))
	}
}

func TestRepositoryCreateWithoutID(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)
	def := createTestPipelineDefinition()
	def.ID = "" // Clear ID to test auto-generation

	err := repo.Create(def)
	if err != nil {
		t.Fatalf("Failed to create pipeline without ID: %v", err)
	}

	// Verify pipeline was created with generated ID
	pipelines, err := repo.List()
	if err != nil {
		t.Fatalf("Failed to list pipelines: %v", err)
	}

	if len(pipelines) != 1 {
		t.Fatalf("Expected 1 pipeline, got %d", len(pipelines))
	}

	if pipelines[0].ID == "" {
		t.Error("Expected generated ID, got empty string")
	}
}

func TestRepositoryCreateInvalidPipeline(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)

	tests := []struct {
		name    string
		def     PipelineDefinition
		wantErr bool
	}{
		{
			name: "missing name",
			def: PipelineDefinition{
				ID:            "test-1",
				ExecutionMode: ExecutionModeScheduled,
				Status:        PipelineStatusActive,
				Components:    createTestPipelineDefinition().Components,
				Connections:   createTestPipelineDefinition().Connections,
			},
			wantErr: true,
		},
		{
			name: "missing cron expression for scheduled pipeline",
			def: PipelineDefinition{
				ID:            "test-2",
				Name:          "Test",
				ExecutionMode: ExecutionModeScheduled,
				Status:        PipelineStatusActive,
				Components:    createTestPipelineDefinition().Components,
				Connections:   createTestPipelineDefinition().Connections,
			},
			wantErr: true,
		},
		{
			name: "invalid cron expression",
			def: PipelineDefinition{
				ID:             "test-3",
				Name:           "Test",
				ExecutionMode:  ExecutionModeScheduled,
				CronExpression: "invalid cron",
				Status:         PipelineStatusActive,
				Components:     createTestPipelineDefinition().Components,
				Connections:    createTestPipelineDefinition().Connections,
			},
			wantErr: true,
		},
		{
			name: "no source component",
			def: PipelineDefinition{
				ID:            "test-4",
				Name:          "Test",
				ExecutionMode: ExecutionModeContinuous,
				Status:        PipelineStatusActive,
				Components: []ComponentConfig{
					{
						ID:   "log-1",
						Type: ComponentTypeLog,
						Parameters: map[string]interface{}{
							"log_level": "info",
						},
					},
					{
						ID:   "sink-1",
						Type: ComponentTypeHTTPPost,
						Parameters: map[string]interface{}{
							"url":          "https://example.com",
							"content_type": "application/json",
						},
					},
				},
				Connections: []Connection{
					{SourceComponentID: "log-1", TargetComponentID: "sink-1"},
				},
			},
			wantErr: true,
		},
		{
			name: "no sink component",
			def: PipelineDefinition{
				ID:            "test-5",
				Name:          "Test",
				ExecutionMode: ExecutionModeContinuous,
				Status:        PipelineStatusActive,
				Components: []ComponentConfig{
					{
						ID:   "source-1",
						Type: ComponentTypeHTTPGet,
						Parameters: map[string]interface{}{
							"url": "https://example.com",
						},
					},
					{
						ID:   "log-1",
						Type: ComponentTypeLog,
						Parameters: map[string]interface{}{
							"log_level": "info",
						},
					},
				},
				Connections: []Connection{
					{SourceComponentID: "source-1", TargetComponentID: "log-1"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := repo.Create(tt.def)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRepositoryRead(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)
	def := createTestPipelineDefinition()

	// Create pipeline first
	if err := repo.Create(def); err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Test successful read
	retrieved, err := repo.Read(def.ID)
	if err != nil {
		t.Fatalf("Failed to read pipeline: %v", err)
	}

	if retrieved.ID != def.ID {
		t.Errorf("Expected ID %s, got %s", def.ID, retrieved.ID)
	}

	// Test read non-existent pipeline
	_, err = repo.Read("non-existent-id")
	if err == nil {
		t.Error("Expected error when reading non-existent pipeline")
	}
}

func TestRepositoryUpdate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)
	def := createTestPipelineDefinition()

	// Create pipeline first
	if err := repo.Create(def); err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Update pipeline
	def.Name = "Updated Pipeline Name"
	def.Description = "Updated description"
	def.Status = PipelineStatusInactive

	err := repo.Update(def)
	if err != nil {
		t.Fatalf("Failed to update pipeline: %v", err)
	}

	// Verify update
	retrieved, err := repo.Read(def.ID)
	if err != nil {
		t.Fatalf("Failed to read updated pipeline: %v", err)
	}

	if retrieved.Name != "Updated Pipeline Name" {
		t.Errorf("Expected name 'Updated Pipeline Name', got %s", retrieved.Name)
	}
	if retrieved.Description != "Updated description" {
		t.Errorf("Expected description 'Updated description', got %s", retrieved.Description)
	}
	if retrieved.Status != PipelineStatusInactive {
		t.Errorf("Expected status inactive, got %s", retrieved.Status)
	}
}

func TestRepositoryUpdateNonExistent(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)
	def := createTestPipelineDefinition()
	def.ID = "non-existent-id"

	err := repo.Update(def)
	if err == nil {
		t.Error("Expected error when updating non-existent pipeline")
	}
}

func TestRepositoryDelete(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)
	def := createTestPipelineDefinition()

	// Create pipeline first
	if err := repo.Create(def); err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Delete pipeline
	err := repo.Delete(def.ID)
	if err != nil {
		t.Fatalf("Failed to delete pipeline: %v", err)
	}

	// Verify deletion
	_, err = repo.Read(def.ID)
	if err == nil {
		t.Error("Expected error when reading deleted pipeline")
	}
}

func TestRepositoryDeleteNonExistent(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)

	err := repo.Delete("non-existent-id")
	if err == nil {
		t.Error("Expected error when deleting non-existent pipeline")
	}
}

func TestRepositoryList(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)

	// Create multiple pipelines
	for i := 0; i < 3; i++ {
		def := createTestPipelineDefinition()
		def.ID = "test-pipeline-" + string(rune('1'+i))
		def.Name = "Test Pipeline " + string(rune('1'+i))
		// Update component IDs to be unique across pipelines
		for j := range def.Components {
			def.Components[j].ID = def.Components[j].ID + "-" + string(rune('1'+i))
		}
		// Update connection IDs to match new component IDs
		for j := range def.Connections {
			def.Connections[j].SourceComponentID = def.Connections[j].SourceComponentID + "-" + string(rune('1'+i))
			def.Connections[j].TargetComponentID = def.Connections[j].TargetComponentID + "-" + string(rune('1'+i))
		}
		if err := repo.Create(def); err != nil {
			t.Fatalf("Failed to create pipeline %d: %v", i, err)
		}
	}

	// List all pipelines
	pipelines, err := repo.List()
	if err != nil {
		t.Fatalf("Failed to list pipelines: %v", err)
	}

	if len(pipelines) != 3 {
		t.Errorf("Expected 3 pipelines, got %d", len(pipelines))
	}
}

func TestRepositoryListActive(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)

	// Create active and inactive pipelines
	for i := 0; i < 5; i++ {
		def := createTestPipelineDefinition()
		def.ID = "test-pipeline-" + string(rune('1'+i))
		def.Name = "Test Pipeline " + string(rune('1'+i))
		// Update component IDs to be unique across pipelines
		for j := range def.Components {
			def.Components[j].ID = def.Components[j].ID + "-" + string(rune('1'+i))
		}
		// Update connection IDs to match new component IDs
		for j := range def.Connections {
			def.Connections[j].SourceComponentID = def.Connections[j].SourceComponentID + "-" + string(rune('1'+i))
			def.Connections[j].TargetComponentID = def.Connections[j].TargetComponentID + "-" + string(rune('1'+i))
		}
		if i%2 == 0 {
			def.Status = PipelineStatusActive
		} else {
			def.Status = PipelineStatusInactive
		}
		if err := repo.Create(def); err != nil {
			t.Fatalf("Failed to create pipeline %d: %v", i, err)
		}
	}

	// List only active pipelines
	activePipelines, err := repo.ListActive()
	if err != nil {
		t.Fatalf("Failed to list active pipelines: %v", err)
	}

	if len(activePipelines) != 3 {
		t.Errorf("Expected 3 active pipelines, got %d", len(activePipelines))
	}

	// Verify all returned pipelines are active
	for _, p := range activePipelines {
		if p.Status != PipelineStatusActive {
			t.Errorf("Expected active status, got %s for pipeline %s", p.Status, p.ID)
		}
	}
}

func TestRepositoryTransactionRollback(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewSQLPipelineRepository(db)
	def := createTestPipelineDefinition()

	// Create a pipeline with invalid component to trigger rollback
	def.Components = append(def.Components, ComponentConfig{
		ID:   "", // Invalid: empty ID
		Type: ComponentTypeHTTPGet,
	})

	err := repo.Create(def)
	if err == nil {
		t.Error("Expected error due to invalid component")
	}

	// Verify no pipeline was created (transaction rolled back)
	pipelines, err := repo.List()
	if err != nil {
		t.Fatalf("Failed to list pipelines: %v", err)
	}

	if len(pipelines) != 0 {
		t.Errorf("Expected 0 pipelines after rollback, got %d", len(pipelines))
	}
}
