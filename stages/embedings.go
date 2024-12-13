package stages

/// Create embedings of missing records

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"Nebula.Conduit/framework"
	"github.com/elastic/go-elasticsearch"
)

type Embedings struct{}

// Execute implements framework.Stage.
func (e *Embedings) Execute(input io.Reader) error {
	// Create Elasticsearch client
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		return fmt.Errorf("error creating elasticsearch client: %v", err)
	}

	// Create LLM client
	client := llm.NewClient("http://localhost:11434")

	// Read input data
	data, err := io.ReadAll(input)
	if err != nil {
		return fmt.Errorf("error reading input: %v", err)
	}

	// Generate embeddings
	embedding, err := client.CreateEmbedding(context.Background(), string(data))
	if err != nil {
		return fmt.Errorf("error generating embedding: %v", err)
	}

	// Prepare document for Elasticsearch
	doc := map[string]interface{}{
		"content":   string(data),
		"embedding": embedding,
	}

	// Convert doc to JSON
	jsonDoc, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("error marshaling document: %v", err)
	}

	// Index document in Elasticsearch
	_, err = es.Index(
		"embeddings",
		strings.NewReader(string(jsonDoc)),
		es.Index.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("error indexing document: %v", err)
	}

	return nil
}

// Output implements framework.Stage.
func (e *Embedings) Output() io.Reader {
	panic("unimplemented")
}

func NewEmbedings() framework.Stage {
	return &Embedings{}
}
