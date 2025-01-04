package spouts

import (
	"Nebula.Conduit/framework"
	"Nebula.Conduit/stages"
)

func CreateEmbedingsPipeline() (*framework.Pipeline, interface{}) {
	pipeline := framework.NewPipeline()
	pipeline.Continues = true

	type InputData struct {
		Index      string                 `json:"index"`
		Filter     map[string]interface{} `json:"filter"`
		Collection string                 `json:"collection"`
		LastReadID int                    `json:"lastreadid"`
	}

	inputData := InputData{
		Index:      "nebulastore",
		Filter:     map[string]interface{}{"query": map[string]interface{}{"match_all": struct{}{}}},
		Collection: "journalevents",
		LastReadID: 0,
	}

	// Add a CSV reader stage to the pipeline
	pipeline.AddStage(stages.NewEmbedings())
	return pipeline, inputData
}
