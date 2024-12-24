package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	embedding "Nebula.Conduit/utilities/embedding_utils"
	"github.com/elastic/go-elasticsearch/v8"
)

type SearchResult struct {
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
}

func searchWithCosineSimilarity(query string, index string, esClient *elasticsearch.Client) ([]SearchResult, error) {
	emb, err := embedding.GenerateEmbedding(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create embedding: %v", err)
	}

	if len(emb) == 0 {
		return nil, fmt.Errorf("embedding is empty")
	}

	fmt.Printf("Generated embedding: %v\n", emb)

	// Prepare the Elasticsearch query
	searchQuery := map[string]interface{}{
		"query": map[string]interface{}{
			"script_score": map[string]interface{}{
				"query": map[string]interface{}{
					"match_all": map[string]interface{}{},
				},
				"script": map[string]interface{}{
					"source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
					"params": map[string]interface{}{
						"query_vector": emb,
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
		Addresses: []string{"http://192.168.1.233:9200/"},
	}

	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// Example usage
	results, err := searchWithCosineSimilarity("Details of ID 7964", "nebulastore_index", esClient)
	if err != nil {
		log.Fatalf("Search failed: %s", err)
	}

	for _, hit := range results {
		fmt.Printf("Score: %f, Document: %v\n", hit.Score, hit.Source)
	}
}
