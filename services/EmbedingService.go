package services

import (
	"context"
	"strings"
	"time"

	"github.com/philippgille/chromem-go"

	"log"
)

type EmbeddingService interface {
	GetCollection(collectionName string) *chromem.Collection
	SearchResults(collectionName string, query string) []string
}

type OllamaEmbeddingService struct {
	OllamaUrl      string
	Model          string
	PersistentPath string
}

func NewEmbeddingService(ollamaUrl string, model string) EmbeddingService {
	return &OllamaEmbeddingService{
		OllamaUrl:      ollamaUrl,
		Model:          model,
		PersistentPath: "./db",
	}
}

func (s *OllamaEmbeddingService) SearchResults(collectionName string, query string) []string {
	question := "search_query: " + query
	collection := s.GetCollection(collectionName)
	start := time.Now()
	log.Println("Querying chromem-go...")
	// "nomic-embed-text" specific prefix (not required with OpenAI's or other models)

	docRes, err := collection.Query(context.Background(), question, 2, nil, nil)
	if err != nil {
		panic(err)
	}
	log.Println("Search (incl query embedding) took", time.Since(start))

	results := make([]string, len(docRes))
	for i, res := range docRes {
		// Cut off the prefix we added before adding the document (see comment above).
		// This is specific to the "nomic-embed-text" model.
		content := strings.TrimPrefix(res.Content, "search_document: ")
		log.Printf("Document %d (similarity: %f): \"%s\"\n", i+1, res.Similarity, content)
		if res.Similarity < 0.7 {
			log.Println("Similarity too low, skipping")
			continue
		}
		results[i] = content
	}
	return results
}

// GetCollection retrieves or creates a chromem.Collection with the specified name.
// It initializes a persistent database connection using the service's PersistentPath
// and sets up an Ollama embedding function with the configured model and URL.
//
// Parameters:
//   - collectionName: The name of the collection to retrieve or create
//
// Returns:
//   - *chromem.Collection: A pointer to the retrieved or created collection
//
// The function will panic if there are any errors during database connection
// or collection creation.
func (s *OllamaEmbeddingService) GetCollection(collectionName string) *chromem.Collection {
	log.Println("Setting up chromem-go...")
	db, err := chromem.NewPersistentDB(s.PersistentPath, true)
	if err != nil {
		panic(err)
	}
	collection, err := db.GetOrCreateCollection(collectionName, nil, chromem.NewEmbeddingFuncOllama(s.Model, s.getOllamaUrl()))
	if err != nil {
		panic(err)
	}

	// Implementation of GetEmbeddingCollection method
	// This is a placeholder and should be replaced with actual implementation
	return collection
}
func (s *OllamaEmbeddingService) getOllamaUrl() string {
	if strings.Contains(s.OllamaUrl, "api") {
		return s.OllamaUrl
	} else {
		return s.OllamaUrl + "/api"
	}
}
