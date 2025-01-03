package services

import (
	"strings"

	"github.com/philippgille/chromem-go"

	"log"
)

type EmbeddingService interface {
	GetCollection(collectionName string) *chromem.Collection
}

type OllamaEmbeddingService struct {
	OllamaUrl      string
	Model          string
	PersistentPath string
}

func NewEmbeddingService(ollamaUrl string, model string) EmbeddingService {
	return &OllamaEmbeddingService{
		OllamaUrl: ollamaUrl,
		Model:     model,
	}
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
