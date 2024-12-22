package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/tmc/langchaingo/llms/ollama"
)

type SearchResult struct {
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
}

func searchWithCosineSimilarity(query string, index string, model string, esClient *elasticsearch.Client) ([]SearchResult, error) {
	// Create Ollama client for embeddings
	ollamaClient, err := ollama.New(
		ollama.WithServerURL("http://192.168.1.10:11434"),
		ollama.WithModel(model),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Ollama client: %v", err)
	}

	// Generate embeddings for the query
	embedding, err := ollamaClient.CreateEmbedding(context.Background(), []string{query})
	if err != nil {
		return nil, fmt.Errorf("failed to create embedding: %v", err)
	}

	// Prepare the Elasticsearch query
	searchQuery := map[string]interface{}{
		"query": map[string]interface{}{
			"script_score": map[string]interface{}{
				"query": map[string]interface{}{
					"match_all": map[string]interface{}{},
				},
				"script": map[string]interface{}{
					"source": "cosineSimilarity(params.query_vector, 'embeddings') + 1.0",
					"params": map[string]interface{}{
						"query_vector": embedding,
					},
				},
			},
		},
	}

	// Convert query to JSON
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, fmt.Errorf("failed to encode query: %v", err)
	}

	// Perform the search
	res, err := esClient.Search(
		esClient.Search.WithContext(context.Background()),
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(&buf),
		esClient.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to perform search: %v", err)
	}
	defer res.Body.Close()

	// Parse the response
	var result struct {
		Hits struct {
			Hits []SearchResult `json:"hits"`
		} `json:"hits"`
	}

	bodyBytes, _ := io.ReadAll(res.Body)
	fmt.Printf("Response body: %s\n", string(bodyBytes))
	res.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result.Hits.Hits, nil
}
func main() {
	// Initialize Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{"http://192.168.1.232:9200/"},
	}

	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// Example usage
	results, err := searchWithCosineSimilarity("ID 8094", "nebulastore", "phi3", esClient)
	if err != nil {
		log.Fatalf("Search failed: %s", err)
	}

	for _, hit := range results {
		fmt.Printf("Score: %f, Document: %v\n", hit.Score, hit.Source)
	}
}
