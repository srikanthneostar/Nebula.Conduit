package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticSearchService struct {
	client *elasticsearch.Client
}

// VectorSearchResult represents the search result structure
type VectorSearchResult struct {
	ID       string                 `json:"_id"`
	Score    float64                `json:"_score"`
	Document map[string]interface{} `json:"_source"`
}

// CreateEmbeddingAndIndex creates an index if it doesn't exist and saves a document with embedding
func (es *ElasticSearchService) CreateEmbeddingAndIndex(ctx context.Context, index string, document map[string]interface{}, embedding []float32) error {
	// Check if index exists
	exists, err := es.client.Indices.Exists([]string{index})
	if err != nil {
		return fmt.Errorf("failed to check index existence: %w", err)
	}

	// Create index if it doesn't exist
	if exists.StatusCode == 404 {
		mapping := map[string]interface{}{
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"embedding": map[string]interface{}{
						"type":       "dense_vector",
						"dims":       len(embedding),
						"index":      true,
						"similarity": "cosine",
					},
					"text": map[string]interface{}{
						"type": "text",
					},
				},
			},
		}

		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(mapping); err != nil {
			return fmt.Errorf("failed to encode mapping: %w", err)
		}

		res, err := es.client.Indices.Create(
			index,
			es.client.Indices.Create.WithBody(&buf),
			es.client.Indices.Create.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
		if res.IsError() {
			return fmt.Errorf("error creating index: %s", res.String())
		}
	}

	// Add embedding to document
	document["embedding"] = embedding

	// Index the document
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(document); err != nil {
		return fmt.Errorf("failed to encode document: %w", err)
	}

	res, err := es.client.Index(
		index,
		&buf,
		es.client.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to index document: %w", err)
	}
	if res.IsError() {
		return fmt.Errorf("error indexing document: %s", res.String())
	}

	return nil
}

// NewElasticSearchService creates a new instance of ElasticSearchService
func NewElasticSearchService(config elasticsearch.Config) (*ElasticSearchService, error) {
	client, err := elasticsearch.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &ElasticSearchService{client: client}, nil
}

// VectorSearchWithQuery performs a vector similarity search in Elasticsearch with additional text query
func (es *ElasticSearchService) VectorSearchWithQuery(ctx context.Context, index string, vector []float32, query string, size int) ([]VectorSearchResult, error) {
	// Construct the search query
	searchQuery := map[string]interface{}{
		"size": size,
		"query": map[string]interface{}{
			"script_score": map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{
								"match": map[string]interface{}{
									"text": query,
								},
							},
						},
					},
				},
				"script": map[string]interface{}{
					"source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
					"params": map[string]interface{}{
						"query_vector": vector,
					},
				},
			},
		},
	}

	// Convert query to JSON
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, err
	}

	// Perform the search request
	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// Parse the response
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	// Check for errors in response
	if res.IsError() {
		return nil, errors.New("elasticsearch error: " + res.String())
	}

	// Extract hits
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid response format")
	}

	// Extract hit array
	hitArr, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, errors.New("invalid hits format")
	}

	// Convert hits to VectorSearchResult
	var searchResults []VectorSearchResult
	for _, hit := range hitArr {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		result := VectorSearchResult{
			ID:       hitMap["_id"].(string),
			Score:    hitMap["_score"].(float64),
			Document: hitMap["_source"].(map[string]interface{}),
		}
		searchResults = append(searchResults, result)
	}

	return searchResults, nil
}

// VectorSearch performs a vector similarity search in Elasticsearch
func (es *ElasticSearchService) VectorSearch(ctx context.Context, index string, vector []float32, size int) ([]VectorSearchResult, error) {
	// Construct the search query
	query := map[string]interface{}{
		"size": size,
		"query": map[string]interface{}{
			"script_score": map[string]interface{}{
				"query": map[string]interface{}{
					"match_all": map[string]interface{}{},
				},
				"script": map[string]interface{}{
					"source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
					"params": map[string]interface{}{
						"query_vector": vector,
					},
				},
			},
		},
	}

	// Convert query to JSON
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}

	// Perform the search request
	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// Parse the response
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	// Check for errors in response
	if res.IsError() {
		return nil, errors.New("elasticsearch error: " + res.String())
	}

	// Extract hits
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid response format")
	}

	hitsArray, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, errors.New("invalid hits format")
	}

	// Convert hits to VectorSearchResult
	var searchResults []VectorSearchResult
	for _, hit := range hitsArray {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		searchResult := VectorSearchResult{
			ID:       hitMap["_id"].(string),
			Score:    hitMap["_score"].(float64),
			Document: hitMap["_source"].(map[string]interface{}),
		}
		searchResults = append(searchResults, searchResult)
	}

	return searchResults, nil
}

// Example usage of vector search
func ExampleVectorSearch() {
	// Initialize Elasticsearch client
	es, err := NewElasticSearchService(elasticsearch.Config{Addresses: []string{"http://192.168.1.232:9200"}})
	if err != nil {
		log.Fatalf("Error creating client: %s", err)
	}

	// Example vector (embedding) to search with
	vector := []float32{0.1, 0.2, 0.3, 0.4}

	// Search parameters
	index := "my_index"
	size := 10

	// Perform vector search
	ctx := context.Background()
	results, err := es.VectorSearch(ctx, index, vector, size)
	if err != nil {
		log.Fatalf("Search error: %s", err)
	}

	// Process results
	for _, result := range results {
		fmt.Printf("ID: %s, Score: %.2f\n", result.ID, result.Score)
		fmt.Printf("Document: %v\n", result.Document)
	}
}
