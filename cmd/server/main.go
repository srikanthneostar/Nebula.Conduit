package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/Xecutables/Nebula.Conduit/internal/api"
	"github.com/Xecutables/Nebula.Conduit/pkg/database"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("/Users/srikanthjonnalagedda/Nebula.Conduit/config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	db, err := database.InitDB(cfg.Database.Path)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Create server
	server := api.NewServer(db, cfg)

	// Start server
	log.Printf("Starting server on %s:%d", cfg.Server.Host, cfg.Server.Port)
	log.Fatal(http.ListenAndServe(
		fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		server.Router,
	))
}
