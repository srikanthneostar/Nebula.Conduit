package stages

/// Create embedings of missing records

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"

	"Nebula.Conduit/framework"
	"Nebula.Conduit/services"
	"github.com/philippgille/chromem-go"
)

type Embedings struct{}

// Execute implements framework.Stage.
// Execute implements the Execute method of the framework.Stage interface.
// It reads input data from the provided io.Reader, processes the data using
// Elasticsearch and an embedding service, and returns any errors that occur
// during the execution.
func (e *Embedings) Execute(input io.Reader) error {

	// Create Elasticsearch client
	elasticService := services.NewElasticService([]string{"http://192.168.1.232:9200"})

	// Read input data
	data, err := io.ReadAll(input)
	if err != nil {
		return fmt.Errorf("error reading input: %v", err)
	}
	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(data, &inputData); err != nil {
		return fmt.Errorf("error unmarshaling data: %v", err)
	}

	index := inputData["index"].(string)
	filter := inputData["filter"].(string)
	var filterData map[string]interface{}
	if err := json.Unmarshal([]byte(filter), &filterData); err != nil {
		return fmt.Errorf("error unmarshaling filter: %v", err)
	}

	results, err := elasticService.SearchByCondition(index, filterData)
	if err != nil {
		return fmt.Errorf("error searching Elasticsearch: %v", err)
	}

	embedingService := services.NewEmbeddingService("http://192.168.1.10:11434", "phi3")
	embedingCollection := inputData["collection"].(string)
	embedingDocument := embedingService.GetCollection(embedingCollection)

	var docs []chromem.Document

	for _, result := range results {
		fmt.Println(result)
		d, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("error marshaling result: %v", err)
		}
		metadata := make(map[string]string)
		for k, v := range result {
			metadata[k] = fmt.Sprintf("%v", v)
		}

		docs = append(docs, chromem.Document{
			ID:       result["id"].(string),
			Metadata: metadata,
			Content:  string(d),
		})

	}

	ctx := context.Background()

	embedingDocument.AddDocuments(ctx, docs, runtime.NumCPU())

	return nil
} // Output implements framework.Stage.

func (e *Embedings) Output() io.Reader {
	panic("unimplemented")
}

func NewEmbedings() framework.Stage {
	return &Embedings{}
}
