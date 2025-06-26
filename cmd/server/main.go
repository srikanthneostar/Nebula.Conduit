package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/Xecutables/Nebula.Conduit/internal/api"
	"github.com/Xecutables/Nebula.Conduit/pkg/database"
	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
)

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	envValue := os.Getenv("NEBULA_CONDUIT_HOME")
	logger := logger.InitLogger()
	if envValue == "" {
		logger.Error().Msg("NEBULA_CONDUIT_HOME environment variable is not set")
	}

	configPath := filepath.Join(envValue, "config.yaml")

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to load configuration")
	}

	// Initialize database
	db, err := database.InitDB(cfg.Database.Path)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to initialize database")
	}

	// Create server
	server := api.NewServer(db, cfg)

	// Start server
	logger.Info().Msgf("Starting server on %s:%d", cfg.Server.Host, cfg.Server.Port)
	log.Fatal(http.ListenAndServe(
		fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		corsMiddleware(server.Router),
	))
}
