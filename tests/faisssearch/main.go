package main

import (
	"context"
	"fmt"
	"log"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/tmc/langchaingo/llms/ollama"
)

func main() {
	// Initialize Ollama client
	llm, err := ollama.New(
		ollama.WithModel("phi3"),
		ollama.WithServerURL("http://192.168.1.10:11434"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Sample documents
	documents := []string{
		"The quick brown fox jumps over the lazy dog",
		"A fast orange fox leaps across a sleepy canine",
		"The lazy dog sleeps in the sun",
	}

	// Generate embeddings
	embeddings := make([][]float32, len(documents))
	ctx := context.Background()

	for i, doc := range documents {
		embedding, err := llm.CreateEmbedding(ctx, []string{doc})
		if err != nil {
			log.Fatal(err)
		}
		embeddings[i] = embedding[0]
	}

	// Initialize FAISS index using IndexFlatL2 instead
	dimension := len(embeddings[0])
	index, err := faiss.NewIndexFlatL2(dimension)
	if err != nil {
		log.Fatal(err)
	}

	// Add vectors to the index
	for _, embedding := range embeddings {
		index.Add(embedding)
	}

	// Perform similarity search
	query := "fox jumping"
	queryEmbedding, err := llm.CreateEmbedding(ctx, []string{query})
	if err != nil {
		log.Fatal(err)
	}

	k := int64(2) // Number of nearest neighbors to retrieve
	distances, indices, err := index.Search(queryEmbedding[0], k)
	if err != nil {
		log.Fatal(err)
	}

	// Print results
	for i := int64(0); i < k; i++ {
		fmt.Printf("Document: %s\n", documents[indices[i]])
		fmt.Printf("Similarity Score: %f\n", distances[i])
		fmt.Println("---")
	}
}
