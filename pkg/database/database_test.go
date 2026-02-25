package database

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitDB_AppliesMigrations(t *testing.T) {
	// Create a temporary directory for the test database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Set NEBULA_CONDUIT_HOME to temp directory
	originalHome := os.Getenv("NEBULA_CONDUIT_HOME")
	defer os.Setenv("NEBULA_CONDUIT_HOME", originalHome)

	// Create migrations directory in temp location
	migrationsDir := filepath.Join(tempDir, "migrations")
	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		t.Fatalf("Failed to create migrations directory: %v", err)
	}

	// Copy migration files to temp location
	sourceMigrationsDir := filepath.Join("..", "..", "commons", "migrations")
	files, err := os.ReadDir(sourceMigrationsDir)
	if err != nil {
		t.Fatalf("Failed to read source migrations directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		content, err := os.ReadFile(filepath.Join(sourceMigrationsDir, file.Name()))
		if err != nil {
			t.Fatalf("Failed to read migration file %s: %v", file.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(migrationsDir, file.Name()), content, 0644); err != nil {
			t.Fatalf("Failed to write migration file %s: %v", file.Name(), err)
		}
	}

	os.Setenv("NEBULA_CONDUIT_HOME", tempDir)

	// Initialize database
	db, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()

	// Verify migrations table exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query migrations table: %v", err)
	}

	if count == 0 {
		t.Error("Expected at least one migration to be applied")
	}

	// Verify pipeline tables exist
	tables := []string{
		"pipelines",
		"pipeline_components",
		"pipeline_connections",
		"pipeline_executions",
		"component_executions",
	}

	for _, table := range tables {
		var tableExists int
		err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&tableExists)
		if err != nil {
			t.Fatalf("Failed to check if table %s exists: %v", table, err)
		}
		if tableExists == 0 {
			t.Errorf("Expected table %s to exist", table)
		}
	}

	// Verify indexes exist
	indexes := []string{
		"idx_pipelines_status",
		"idx_pipeline_components_pipeline_id",
		"idx_pipeline_connections_pipeline_id",
		"idx_pipeline_executions_pipeline_id",
		"idx_pipeline_executions_started_at",
		"idx_component_executions_pipeline_execution_id",
	}

	for _, index := range indexes {
		var indexExists int
		err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", index).Scan(&indexExists)
		if err != nil {
			t.Fatalf("Failed to check if index %s exists: %v", index, err)
		}
		if indexExists == 0 {
			t.Errorf("Expected index %s to exist", index)
		}
	}
}

func TestInitDB_MigrationsAreIdempotent(t *testing.T) {
	// Create a temporary directory for the test database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Set NEBULA_CONDUIT_HOME to temp directory
	originalHome := os.Getenv("NEBULA_CONDUIT_HOME")
	defer os.Setenv("NEBULA_CONDUIT_HOME", originalHome)

	// Create migrations directory in temp location
	migrationsDir := filepath.Join(tempDir, "migrations")
	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		t.Fatalf("Failed to create migrations directory: %v", err)
	}

	// Copy migration files to temp location
	sourceMigrationsDir := filepath.Join("..", "..", "commons", "migrations")
	files, err := os.ReadDir(sourceMigrationsDir)
	if err != nil {
		t.Fatalf("Failed to read source migrations directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		content, err := os.ReadFile(filepath.Join(sourceMigrationsDir, file.Name()))
		if err != nil {
			t.Fatalf("Failed to read migration file %s: %v", file.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(migrationsDir, file.Name()), content, 0644); err != nil {
			t.Fatalf("Failed to write migration file %s: %v", file.Name(), err)
		}
	}

	os.Setenv("NEBULA_CONDUIT_HOME", tempDir)

	// Initialize database first time
	db1, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("First InitDB failed: %v", err)
	}

	// Get migration count after first init
	var count1 int
	err = db1.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&count1)
	if err != nil {
		t.Fatalf("Failed to query migrations table: %v", err)
	}
	db1.Close()

	// Initialize database second time (should not apply migrations again)
	db2, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("Second InitDB failed: %v", err)
	}
	defer db2.Close()

	// Get migration count after second init
	var count2 int
	err = db2.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&count2)
	if err != nil {
		t.Fatalf("Failed to query migrations table: %v", err)
	}

	// Counts should be the same (migrations not applied twice)
	if count1 != count2 {
		t.Errorf("Expected migration count to remain %d, got %d", count1, count2)
	}
}
