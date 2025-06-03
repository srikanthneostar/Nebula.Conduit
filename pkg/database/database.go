package database

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// InitDB initializes the database and applies any pending migrations
func InitDB(path string) (*sql.DB, error) {
	// Ensure the database directory exists
	dbDir := filepath.Dir(path)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Open database (this will create the file if it doesn't exist)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Apply migrations
	if err := applyMigrations(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	return db, nil
}

// applyMigrations applies all pending migrations in order
func applyMigrations(db *sql.DB) error {
	// Read migration files
	files, err := ioutil.ReadDir("/Users/srikanthjonnalagedda/Nebula.Conduit/migrations/")
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Sort migration files by name
	var migrations []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sql") {
			migrations = append(migrations, file.Name())
		}
	}
	sort.Strings(migrations)

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create migrations table if it doesn't exist
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS migrations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Apply each migration
	for _, migration := range migrations {
		// Check if migration has already been applied
		var count int
		err := tx.QueryRow("SELECT COUNT(*) FROM migrations WHERE name = ?", migration).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check migration status: %w", err)
		}
		if count > 0 {
			continue
		}

		// Read and execute migration
		content, err := ioutil.ReadFile(filepath.Join("/Users/srikanthjonnalagedda/Nebula.Conduit/migrations/", migration))
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", migration, err)
		}

		_, err = tx.Exec(string(content))
		if err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", migration, err)
		}

		// Record migration
		_, err = tx.Exec("INSERT INTO migrations (name) VALUES (?)", migration)
		if err != nil {
			return fmt.Errorf("failed to record migration %s: %w", migration, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migrations: %w", err)
	}

	return nil
}
