package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	_ "github.com/mattn/go-sqlite3"
)

var (
	log = logger.InitLogger()
)

// InitDB initializes the database and applies any pending migrations
func InitDB(path string) (*sql.DB, error) {
	// If path is not absolute, make it relative to NEBULA_CONDUIT_HOME
	if !filepath.IsAbs(path) {
		basePath := os.Getenv("NEBULA_CONDUIT_HOME")
		if basePath == "" {
			return nil, fmt.Errorf("environment variable NEBULA_CONDUIT_HOME is not set")
		}
		path = filepath.Join(basePath, path)
	}

	// Ensure the database directory exists
	dbDir := filepath.Dir(path)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		log.Error().Err(err).Str("path", dbDir).Msg("Failed to create database directory")
		return nil, err
	}

	// Open database (this will create the file if it doesn't exist)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("Failed to open database")
		return nil, err
	}

	if err := db.Ping(); err != nil {
		log.Error().Err(err).Str("path", path).Msg("Failed to ping database")
		return nil, err
	}

	// Enable foreign key constraints (required for SQLite)
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		log.Error().Err(err).Msg("Failed to enable foreign key constraints")
		db.Close()
		return nil, err
	}
	log.Info().Msg("Foreign key constraints enabled")

	// Apply migrations
	if err := applyMigrations(db); err != nil {
		db.Close()
		log.Error().Err(err).Str("path", path).Msg("Failed to apply migrations")
		return nil, err
	}

	return db, nil
}

// InitMongoDB initializes MongoDB connection if enabled
func InitMongoDB(connectionString, dbName, username, password string) (*pipeline.MongoDBQueue, error) {
	if connectionString == "" {
		return nil, nil
	}

	return pipeline.NewMongoDBQueue(connectionString, dbName, username, password)
}

// applyMigrations applies all pending migrations in order
func applyMigrations(db *sql.DB) error {
	// Get the base directory from environment variable
	basePath := os.Getenv("NEBULA_CONDUIT_HOME")
	if basePath == "" {
		return fmt.Errorf("environment variable NEBULA_CONDUIT_HOME is not set")
	}

	// Build migrations directory path
	migrationsDir := filepath.Join(basePath, "migrations")

	// Read migration files
	files, err := os.ReadDir(migrationsDir)
	if err != nil {
		log.Error().Err(err).Str("migrations_dir", migrationsDir).Msg("Failed to read migrations directory")
		return err
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
		log.Error().Err(err).Msg("Failed to begin transaction for migrations")
		return err
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
		log.Error().Err(err).Msg("Failed to create migrations table")
		return err
	}

	// Apply each migration
	for _, migration := range migrations {
		// Check if migration has already been applied
		var count int
		err := tx.QueryRow("SELECT COUNT(*) FROM migrations WHERE name = ?", migration).Scan(&count)
		if err != nil {
			log.Error().Err(err).Str("migration", migration).Msg("Failed to check migration status")
			return err
		}
		if count > 0 {
			continue
		}

		// Read and execute migration file
		migrationPath := filepath.Join(migrationsDir, migration)
		content, err := os.ReadFile(migrationPath)
		if err != nil {
			log.Error().Err(err).Str("migration", migration).Msg("Failed to read migration file")
			return err
		}

		_, err = tx.Exec(string(content))
		if err != nil {
			log.Error().Err(err).Str("migration", migration).Msg("Failed to apply migration")
			return err
		}

		// Record migration
		_, err = tx.Exec("INSERT INTO migrations (name) VALUES (?)", migration)
		if err != nil {
			log.Error().Err(err).Str("migration", migration).Msg("Failed to record migration")
			return err
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Error().Err(err).Msg("Failed to commit migrations transaction")
		return err
	}

	return nil
}
