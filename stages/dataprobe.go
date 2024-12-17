package stages

/// Based on configuration ask the question and return questions and similarity search results
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"Nebula.Conduit/framework"
	"Nebula.Conduit/services"
	"github.com/parakeet-nest/parakeet/embeddings"
	"github.com/parakeet-nest/parakeet/llm"
)

type Config struct {
	ElasticSearchHost string `json:"elasticSearchHost"`
	IndexName         string `json:"indexName"`
	EmbeddingsModel   string `json:"embeddingsModel"`
}

type DataProbe struct {
	Config        Config
	ElasticSearch *services.ElasticSearchService
}

// Execute implements framework.Stage.
func (e *DataProbe) Execute(input io.Reader) error {
	var queryInput struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(input).Decode(&queryInput); err != nil {
		return fmt.Errorf("failed to decode query input: %w", err)
	}
	if queryInput.Query == "" {
		return errors.New("query cannot be empty")
	}

	queryEmbedding, err := embeddings.CreateEmbedding(
		e.Config.EmbeddingsModel,
		llm.Query4Embedding{Prompt: queryInput.Query},
		"query",
	)
	if err != nil {
		return fmt.Errorf("failed to generate embedding: %w", err)
	}
	ctx := context.Background()
	results, err := e.ElasticSearch.VectorSearchWithQuery(ctx, e.Config.IndexName, queryEmbedding.Embedding, queryInput.Query, 1000)
	if err != nil {
		return fmt.Errorf("failed to perform vector search: %w", err)
	}
	fmt.Println("Search Results:")
	for _, result := range results {
		fmt.Printf("ID: %s, Score: %.2f\n", result.ID, result.Score)
		fmt.Printf("Document: %v\n", result.Document)
	}

	return nil
}

// Output implements framework.Stage.
func (e *DataProbe) Output() io.Reader {
	panic("unimplemented")
}

func NewDataProbe(e *services.ElasticSearchService) framework.Stage {
	return &DataProbe{
		ElasticSearch: e,
	}
}
