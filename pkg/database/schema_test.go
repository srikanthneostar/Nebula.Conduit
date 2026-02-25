package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestPipelineSchemaCreation verifies that all pipeline tables are created correctly
func TestPipelineSchemaCreation(t *testing.T) {
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

	// Copy migration files from commons/migrations to temp migrations directory
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

	// Test 1: Verify pipelines table exists with correct columns
	t.Run("PipelinesTable", func(t *testing.T) {
		verifyTableExists(t, db, "pipelines")
		verifyColumnsExist(t, db, "pipelines", []string{
			"id", "name", "description", "execution_mode",
			"cron_expression", "status", "created_at", "updated_at",
		})
	})

	// Test 2: Verify pipeline_components table exists with correct columns
	t.Run("PipelineComponentsTable", func(t *testing.T) {
		verifyTableExists(t, db, "pipeline_components")
		verifyColumnsExist(t, db, "pipeline_components", []string{
			"id", "pipeline_id", "component_type",
			"component_config", "position_in_graph",
		})
	})

	// Test 3: Verify pipeline_connections table exists with correct columns
	t.Run("PipelineConnectionsTable", func(t *testing.T) {
		verifyTableExists(t, db, "pipeline_connections")
		verifyColumnsExist(t, db, "pipeline_connections", []string{
			"id", "pipeline_id", "source_component_id", "target_component_id",
		})
	})

	// Test 4: Verify pipeline_executions table exists with correct columns
	t.Run("PipelineExecutionsTable", func(t *testing.T) {
		verifyTableExists(t, db, "pipeline_executions")
		verifyColumnsExist(t, db, "pipeline_executions", []string{
			"id", "pipeline_id", "status",
			"started_at", "ended_at", "error_message",
		})
	})

	// Test 5: Verify component_executions table exists with correct columns
	t.Run("ComponentExecutionsTable", func(t *testing.T) {
		verifyTableExists(t, db, "component_executions")
		verifyColumnsExist(t, db, "component_executions", []string{
			"id", "pipeline_execution_id", "component_id", "status",
			"started_at", "ended_at", "output_data_size", "error_message",
		})
	})

	// Test 6: Verify indexes exist
	t.Run("Indexes", func(t *testing.T) {
		indexes := []string{
			"idx_pipelines_status",
			"idx_pipeline_components_pipeline_id",
			"idx_pipeline_connections_pipeline_id",
			"idx_pipeline_executions_pipeline_id",
			"idx_pipeline_executions_started_at",
			"idx_component_executions_pipeline_execution_id",
		}

		for _, indexName := range indexes {
			verifyIndexExists(t, db, indexName)
		}
	})

	// Test 7: Verify foreign key constraints
	t.Run("ForeignKeys", func(t *testing.T) {
		// Enable foreign keys
		_, err := db.Exec("PRAGMA foreign_keys = ON")
		if err != nil {
			t.Fatalf("Failed to enable foreign keys: %v", err)
		}

		// Insert a test pipeline
		_, err = db.Exec(`
			INSERT INTO pipelines (id, name, description, execution_mode, status)
			VALUES ('test-pipeline', 'Test Pipeline', 'Test', 'scheduled', 'active')
		`)
		if err != nil {
			t.Fatalf("Failed to insert test pipeline: %v", err)
		}

		// Try to insert a component with invalid pipeline_id (should fail)
		_, err = db.Exec(`
			INSERT INTO pipeline_components (id, pipeline_id, component_type, component_config, position_in_graph)
			VALUES ('test-comp', 'invalid-pipeline', 'http_get', '{}', 1)
		`)
		if err == nil {
			t.Error("Expected foreign key constraint violation, but insert succeeded")
		}

		// Insert a valid component
		_, err = db.Exec(`
			INSERT INTO pipeline_components (id, pipeline_id, component_type, component_config, position_in_graph)
			VALUES ('test-comp', 'test-pipeline', 'http_get', '{}', 1)
		`)
		if err != nil {
			t.Fatalf("Failed to insert valid component: %v", err)
		}
	})

	// Test 8: Verify CHECK constraints
	t.Run("CheckConstraints", func(t *testing.T) {
		// Try to insert pipeline with invalid execution_mode (should fail)
		_, err := db.Exec(`
			INSERT INTO pipelines (id, name, execution_mode, status)
			VALUES ('test-invalid', 'Test', 'invalid_mode', 'active')
		`)
		if err == nil {
			t.Error("Expected CHECK constraint violation for execution_mode, but insert succeeded")
		}

		// Try to insert pipeline with invalid status (should fail)
		_, err = db.Exec(`
			INSERT INTO pipelines (id, name, execution_mode, status)
			VALUES ('test-invalid2', 'Test', 'scheduled', 'invalid_status')
		`)
		if err == nil {
			t.Error("Expected CHECK constraint violation for status, but insert succeeded")
		}
	})
}

// verifyTableExists checks if a table exists in the database
func verifyTableExists(t *testing.T, db *sql.DB, tableName string) {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='table' AND name=?
	`, tableName).Scan(&count)

	if err != nil {
		t.Fatalf("Failed to check if table %s exists: %v", tableName, err)
	}

	if count == 0 {
		t.Errorf("Table %s does not exist", tableName)
	}
}

// verifyColumnsExist checks if all specified columns exist in a table
func verifyColumnsExist(t *testing.T, db *sql.DB, tableName string, columns []string) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		t.Fatalf("Failed to get table info for %s: %v", tableName, err)
	}
	defer rows.Close()

	existingColumns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue sql.NullString

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("Failed to scan column info: %v", err)
		}
		existingColumns[name] = true
	}

	for _, col := range columns {
		if !existingColumns[col] {
			t.Errorf("Column %s does not exist in table %s", col, tableName)
		}
	}
}

// verifyIndexExists checks if an index exists in the database
func verifyIndexExists(t *testing.T, db *sql.DB, indexName string) {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='index' AND name=?
	`, indexName).Scan(&count)

	if err != nil {
		t.Fatalf("Failed to check if index %s exists: %v", indexName, err)
	}

	if count == 0 {
		t.Errorf("Index %s does not exist", indexName)
	}
}
