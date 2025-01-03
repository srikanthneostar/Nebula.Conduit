package main

import (
	"bytes"
	"encoding/json"
	"log"

	"Nebula.Conduit/stages"
)

func main() {

	// Create test input data
	inputData := map[string]interface{}{
		"index":      "nebulastore",
		"filter":     `{"term": {"has_embedding": false}}`,
		"collection": "documents",
	}

	// Serialize input data to JSON
	inputJSON, err := json.Marshal(inputData)
	if err != nil {
		log.Fatalf("Failed to marshal input data: %v", err)
	}

	// Create an instance of Embeddings stage
	embeddingsStage := stages.NewEmbedings()

	// Create a bytes.Reader from the input JSON
	inputReader := bytes.NewReader(inputJSON)

	// Execute the stage with the input data
	if err := embeddingsStage.Execute(inputReader); err != nil {
		log.Fatalf("Execution failed: %v", err)
	}

	log.Println("Embeddings generation completed successfully")
}
