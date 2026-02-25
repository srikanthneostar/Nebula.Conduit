package database

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPipelineSchemaIntegration tests the complete pipeline schema integration
func TestPipelineSchemaIntegration(t *testing.T) {
	// Create a temporary directory for the test database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Set NEBULA_CONDUIT_HOME to temp directory
	oldHome := os.Getenv("NEBULA_CONDUIT_HOME")
	os.Setenv("NEBULA_CONDUIT_HOME", tempDir)
	defer os.Setenv("NEBULA_CONDUIT_HOME", oldHome)

	// Copy migration files to temp directory
	migrationsDir := filepath.Join(tempDir, "migrations")
	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		t.Fatalf("Failed to create migrations directory: %v", err)
	}

	sourceMigrationsDir := filepath.Join("..", "..", "commons", "migrations")
	files, err := os.ReadDir(sourceMigrationsDir)
	if err != nil {
		t.Fatalf("Failed to read source migrations directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		sourceFile := filepath.Join(sourceMigrationsDir, file.Name())
		destFile := filepath.Join(migrationsDir, file.Name())

		content, err := os.ReadFile(sourceFile)
		if err != nil {
			t.Fatalf("Failed to read migration file %s: %v", file.Name(), err)
		}

		if err := os.WriteFile(destFile, content, 0644); err != nil {
			t.Fatalf("Failed to write migration file %s: %v", file.Name(), err)
		}
	}

	// Initialize database
	db, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Enable foreign keys
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	// Test complete workflow: Create pipeline -> Add components -> Add connections -> Execute -> Record results
	t.Run("CompleteWorkflow", func(t *testing.T) {
		// 1. Create a pipeline
		pipelineID := "test-pipeline-1"
		_, err := db.Exec(`
			INSERT INTO pipelines (id, name, description, execution_mode, cron_expression, status, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, pipelineID, "Test Pipeline", "Integration test pipeline", "scheduled", "0 */5 * * *", "active", time.Now(), time.Now())
		if err != nil {
			t.Fatalf("Failed to insert pipeline: %v", err)
		}

		// 2. Add components
		component1ID := "http-source-1"
		component2ID := "python-processor-1"
		component3ID := "kafka-sink-1"

		components := []struct {
			id     string
			typ    string
			config string
			pos    int
		}{
			{component1ID, "http_get", `{"url": "https://api.example.com/data"}`, 1},
			{component2ID, "python_code_block", `{"code": "data['processed'] = True"}`, 2},
			{component3ID, "kafka_producer", `{"brokers": ["localhost:9092"], "topic": "test"}`, 3},
		}

		for _, comp := range components {
			_, err := db.Exec(`
				INSERT INTO pipeline_components (id, pipeline_id, component_type, component_config, position_in_graph)
				VALUES (?, ?, ?, ?, ?)
			`, comp.id, pipelineID, comp.typ, comp.config, comp.pos)
			if err != nil {
				t.Fatalf("Failed to insert component %s: %v", comp.id, err)
			}
		}

		// 3. Add connections
		connections := []struct {
			source string
			target string
		}{
			{component1ID, component2ID},
			{component2ID, component3ID},
		}

		for i, conn := range connections {
			_, err := db.Exec(`
				INSERT INTO pipeline_connections (id, pipeline_id, source_component_id, target_component_id)
				VALUES (?, ?, ?, ?)
			`, "conn-"+string(rune(i+1)), pipelineID, conn.source, conn.target)
			if err != nil {
				t.Fatalf("Failed to insert connection: %v", err)
			}
		}

		// 4. Create a pipeline execution
		executionID := "exec-1"
		startTime := time.Now()
		_, err = db.Exec(`
			INSERT INTO pipeline_executions (id, pipeline_id, status, started_at)
			VALUES (?, ?, ?, ?)
		`, executionID, pipelineID, "running", startTime)
		if err != nil {
			t.Fatalf("Failed to insert pipeline execution: %v", err)
		}

		// 5. Record component executions
		for i, comp := range components {
			compStartTime := startTime.Add(time.Duration(i) * time.Second)
			compEndTime := compStartTime.Add(500 * time.Millisecond)

			_, err := db.Exec(`
				INSERT INTO component_executions (id, pipeline_execution_id, component_id, status, started_at, ended_at, output_data_size)
				VALUES (?, ?, ?, ?, ?, ?, ?)
			`, "comp-exec-"+string(rune(i+1)), executionID, comp.id, "completed", compStartTime, compEndTime, 1024)
			if err != nil {
				t.Fatalf("Failed to insert component execution: %v", err)
			}
		}

		// 6. Complete the pipeline execution
		endTime := time.Now()
		_, err = db.Exec(`
			UPDATE pipeline_executions 
			SET status = ?, ended_at = ?
			WHERE id = ?
		`, "completed", endTime, executionID)
		if err != nil {
			t.Fatalf("Failed to update pipeline execution: %v", err)
		}

		// 7. Verify data integrity - Query the complete pipeline with all relationships
		var pipelineName string
		var pipelineStatus string
		err = db.QueryRow(`
			SELECT name, status FROM pipelines WHERE id = ?
		`, pipelineID).Scan(&pipelineName, &pipelineStatus)
		if err != nil {
			t.Fatalf("Failed to query pipeline: %v", err)
		}
		if pipelineName != "Test Pipeline" {
			t.Errorf("Expected pipeline name 'Test Pipeline', got '%s'", pipelineName)
		}
		if pipelineStatus != "active" {
			t.Errorf("Expected pipeline status 'active', got '%s'", pipelineStatus)
		}

		// 8. Verify components count
		var componentCount int
		err = db.QueryRow(`
			SELECT COUNT(*) FROM pipeline_components WHERE pipeline_id = ?
		`, pipelineID).Scan(&componentCount)
		if err != nil {
			t.Fatalf("Failed to count components: %v", err)
		}
		if componentCount != 3 {
			t.Errorf("Expected 3 components, got %d", componentCount)
		}

		// 9. Verify connections count
		var connectionCount int
		err = db.QueryRow(`
			SELECT COUNT(*) FROM pipeline_connections WHERE pipeline_id = ?
		`, pipelineID).Scan(&connectionCount)
		if err != nil {
			t.Fatalf("Failed to count connections: %v", err)
		}
		if connectionCount != 2 {
			t.Errorf("Expected 2 connections, got %d", connectionCount)
		}

		// 10. Verify execution record
		var execStatus string
		err = db.QueryRow(`
			SELECT status FROM pipeline_executions WHERE id = ?
		`, executionID).Scan(&execStatus)
		if err != nil {
			t.Fatalf("Failed to query execution: %v", err)
		}
		if execStatus != "completed" {
			t.Errorf("Expected execution status 'completed', got '%s'", execStatus)
		}

		// 11. Verify component execution records
		var compExecCount int
		err = db.QueryRow(`
			SELECT COUNT(*) FROM component_executions WHERE pipeline_execution_id = ?
		`, executionID).Scan(&compExecCount)
		if err != nil {
			t.Fatalf("Failed to count component executions: %v", err)
		}
		if compExecCount != 3 {
			t.Errorf("Expected 3 component executions, got %d", compExecCount)
		}
	})

	// Test cascade delete behavior
	t.Run("CascadeDelete", func(t *testing.T) {
		// Create a pipeline with components
		pipelineID := "test-cascade"
		_, err := db.Exec(`
			INSERT INTO pipelines (id, name, execution_mode, status)
			VALUES (?, ?, ?, ?)
		`, pipelineID, "Cascade Test", "scheduled", "active")
		if err != nil {
			t.Fatalf("Failed to insert pipeline: %v", err)
		}

		// Add a component
		componentID := "test-comp"
		_, err = db.Exec(`
			INSERT INTO pipeline_components (id, pipeline_id, component_type, component_config, position_in_graph)
			VALUES (?, ?, ?, ?, ?)
		`, componentID, pipelineID, "http_get", "{}", 1)
		if err != nil {
			t.Fatalf("Failed to insert component: %v", err)
		}

		// Delete the pipeline
		_, err = db.Exec("DELETE FROM pipelines WHERE id = ?", pipelineID)
		if err != nil {
			t.Fatalf("Failed to delete pipeline: %v", err)
		}

		// Verify component was also deleted (cascade)
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM pipeline_components WHERE id = ?", componentID).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count components: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected component to be deleted via cascade, but it still exists")
		}
	})

	// Test index usage for performance queries
	t.Run("IndexUsage", func(t *testing.T) {
		// Query by status (should use idx_pipelines_status)
		rows, err := db.Query("SELECT id FROM pipelines WHERE status = ?", "active")
		if err != nil {
			t.Fatalf("Failed to query by status: %v", err)
		}
		rows.Close()

		// Query executions by pipeline_id (should use idx_pipeline_executions_pipeline_id)
		rows, err = db.Query("SELECT id FROM pipeline_executions WHERE pipeline_id = ?", "test-pipeline-1")
		if err != nil {
			t.Fatalf("Failed to query executions by pipeline_id: %v", err)
		}
		rows.Close()

		// Query executions by started_at (should use idx_pipeline_executions_started_at)
		rows, err = db.Query("SELECT id FROM pipeline_executions WHERE started_at > ?", time.Now().Add(-24*time.Hour))
		if err != nil {
			t.Fatalf("Failed to query executions by started_at: %v", err)
		}
		rows.Close()
	})
}
