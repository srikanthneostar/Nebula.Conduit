package stages

/// Create embedings of missing records

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"runtime"
	"strconv"
	"strings"

	"Nebula.Conduit/framework"
	"Nebula.Conduit/models"
	"Nebula.Conduit/services"
	"Nebula.Conduit/utilities"
	"github.com/philippgille/chromem-go"
)

type Embedings struct{ lastReadId *int }

// Execute implements framework.Stage.
// Execute implements the Execute method of the framework.Stage interface.
// It reads input data from the provided io.Reader, processes the data using
// Elasticsearch and an embedding service, and returns any errors that occur
// during the execution.
func (e *Embedings) Execute(input io.Reader) error {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Error occurred: %v\n", err)
		}
	}()

	// Create Elasticsearch client
	elasticService := services.NewElasticService([]string{"http://192.168.1.232:9200"})

	// Read input data
	data, err := io.ReadAll(input)
	if err != nil {
		log.Printf("Error reading input: %v\n", err)
		return fmt.Errorf("error reading input: %v", err)
	}
	// Parse input data
	var inputData models.EventsInputData
	if err := json.Unmarshal(data, &inputData); err != nil {
		log.Printf("Error unmarshaling data: %v\n", err)
		return fmt.Errorf("error unmarshaling data: %v", err)
	}

	index := inputData.Index
	var lastReadID int
	if e.lastReadId != nil && *e.lastReadId != 0 {
		lastReadID = *e.lastReadId
	} else {
		lastReadID = inputData.InitReadID
	}

	filter := utilities.GetEventsFilterCondition(lastReadID)

	// Instead of passing the whole filter
	results, err := elasticService.SearchByCondition(index, filter)
	if len(results) == 0 {
		log.Println("No records found")
		return nil
	}

	if err != nil {
		log.Printf("Error searching Elasticsearch: %v\n", err)

		return fmt.Errorf("error searching Elasticsearch: %v", err)
	}
	log.Println("printing res -->", results)

	embedingService := services.NewEmbeddingService("http://0.0.0.0:11434", "mistral-max")
	embedingCollection := inputData.Collection
	embedingDocument := embedingService.GetCollection(embedingCollection)

	var docs []chromem.Document
	var ids []float64

	for _, result := range results {
		log.Println(result)
		d, err := json.Marshal(result["entity"])
		if err != nil {
			log.Printf("Error marshaling result: %v\n", err)
			return fmt.Errorf("error marshaling result: %v", err)
		}
		metadata := make(map[string]string)
		for k, v := range result["entity"].(map[string]interface{}) {
			switch val := v.(type) {
			case string:
				metadata[k] = val
			case float64:
				metadata[k] = strconv.FormatFloat(val, 'f', -1, 64)
			case int:
				metadata[k] = strconv.Itoa(val)
			case bool:
				metadata[k] = strconv.FormatBool(val)
			case map[string]interface{}:
				if jsonBytes, err := json.Marshal(val); err == nil {
					metadata[k] = string(jsonBytes)
				}
			default:
				metadata[k] = fmt.Sprintf("%v", v)
			}
		}

		val, _ := json.Marshal(metadata)
		log.Println("meta----->", string(val))

		if entity, ok := result["entity"].(map[string]interface{}); ok {
			if id, ok := entity["id"].(float64); ok {
				ids = append(ids, id)
				log.Println("content----->", string(d))
				docs = append(docs, chromem.Document{
					ID:       strconv.Itoa(int(id)),
					Metadata: metadata,
					Content:  string(d),
				})
			} else {
				log.Printf("Error: entity.id is not an int32\n")
			}
		} else {
			log.Printf("Error: result does not contain an entity map\n")
		}
	}

	ctx := context.Background()

	err = embedingDocument.AddDocuments(ctx, docs, runtime.NumCPU())
	if err != nil {
		return err
	}

	maxId := 0.0
	for _, id := range ids {
		if id > maxId {
			maxId = id
		}
	}

	log.Println("maxId -->", maxId)
	lastReadId := int(maxId)
	e.lastReadId = &lastReadId
	return nil
}

// Output implements framework.Stage.Output.

func (e *Embedings) Output() io.Reader {
	return strings.NewReader(strconv.Itoa(*e.lastReadId))
}
func NewEmbedings() framework.Stage {
	lastReadId := 0
	return &Embedings{lastReadId: &lastReadId}
}
