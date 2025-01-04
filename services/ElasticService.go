package services

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v7"
)

type ElasticService struct {
	client *elasticsearch.Client
}

func createElasticClient(addresses []string) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: addresses,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func NewElasticService(addresses []string) *ElasticService {
	client, err := createElasticClient(addresses)
	if err != nil {
		panic(err)
	}
	return &ElasticService{client: client}
}

// Package services provides functionality for interacting with Elasticsearch.

// ElasticService handles Elasticsearch operations and queries.
// It maintains a client connection to the Elasticsearch cluster.

// createElasticClient initializes and returns a new Elasticsearch client.
// Parameters:
//   - addresses: A slice of strings containing Elasticsearch server addresses
// Returns:
//   - *elasticsearch.Client: The initialized Elasticsearch client
//   - error: Any error encountered during client creation

// NewElasticService creates and returns a new ElasticService instance.
// Parameters:
//   - addresses: A slice of strings containing Elasticsearch server addresses
// Returns:
//   - *ElasticService: A new ElasticService instance
// Panics if client creation fails

// SearchByCondition searches the specified index using the provided query.
// Parameters:
//   - index: The name of the Elasticsearch index to search
//   - query: A map containing the Elasticsearch query
// Returns:
//   - []map[string]interface{}: Slice of documents matching the query
//   - error: Any error encountered during the search operation

func (es *ElasticService) SearchByCondition(index string, query any) ([]map[string]interface{}, error) {
	var buf bytes.Buffer
	searchQuery := map[string]interface{}{
		"query": query,
	}

	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, err
	}

	// Add compatibility headers
	res, err := es.client.Search(
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(&buf),
	)

	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	var documents []map[string]interface{}
	hits, _ := result["hits"].(map[string]interface{})
	if hitsArr, ok := hits["hits"].([]interface{}); ok {
		for _, hit := range hitsArr {
			if doc, ok := hit.(map[string]interface{}); ok {
				if source, ok := doc["_source"].(map[string]interface{}); ok {
					documents = append(documents, source)
				}
			}
		}
	}

	return documents, nil
}
