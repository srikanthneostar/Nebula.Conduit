package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/tmc/langchaingo/llms/ollama"
)

func searchSimilarDocuments(question string) error {
	// Generate embedding for the question
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://192.168.1.232:9200/"},
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 30 * time.Second, // Increase timeout
			}).DialContext,
		},
	})
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}
	model, err := ollama.New(ollama.WithModel("llama3"))
	if err != nil {
		return fmt.Errorf("error creating ollama model: %v", err)
	}
	ctx := context.Background()
	embs, err := model.CreateEmbedding(ctx, []string{question})
	if err != nil {
		return fmt.Errorf("error generating embedding: %v", err)
	}

	// Create the search query
	searchQuery := struct {
		Query struct {
			ScriptScore struct {
				Query struct {
					MatchAll interface{} `json:"match_all"`
				} `json:"query"`
				Script struct {
					Source string `json:"source"`
					Params struct {
						QueryVector []float64 `json:"query_vector"`
					} `json:"params"`
				} `json:"script"`
			} `json:"script_score"`
		} `json:"query"`
	}{}

	// Set up the script score query
	searchQuery.Query.ScriptScore.Query.MatchAll = struct{}{}
	searchQuery.Query.ScriptScore.Script.Source = "cosineSimilarity(params.query_vector, 'embeddings') + 1.0"

	// Convert embedding to float64 and set as query vector
	queryVector := make([]float64, len(embs[0]))
	for i, v := range embs[0] {
		queryVector[i] = float64(v)
	}
	searchQuery.Query.ScriptScore.Script.Params.QueryVector = queryVector

	// Serialize the query
	queryBody, err := json.Marshal(searchQuery)
	if err != nil {
		return fmt.Errorf("error marshaling search query: %v", err)
	}

	// Execute search
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("nebulastore"),
		es.Search.WithBody(bytes.NewReader(queryBody)),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		return fmt.Errorf("error executing search: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch error: %s", res.String())
	}

	// Parse response
	var searchResponse struct {
		Hits struct {
			Hits []struct {
				Score  float64                `json:"_score"`
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	// Print results
	fmt.Printf("Found %d similar documents\n", len(searchResponse.Hits.Hits))
	for _, hit := range searchResponse.Hits.Hits {
		fmt.Printf("Score: %.4f\n", hit.Score)
		fmt.Printf("Document: %+v\n", hit.Source)
		fmt.Println("---")
	}

	return nil
}

func main() {
	question := "ID 8094"
	if err := searchSimilarDocuments(question); err != nil {
		log.Fatalf("Error searching for similar documents: %v", err)
	}

}
