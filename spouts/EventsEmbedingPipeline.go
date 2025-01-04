package spouts

import (
	"Nebula.Conduit/framework"
	"Nebula.Conduit/stages"
)

func CreateEmbedingsPipeline() (*framework.Pipeline, interface{}) {
	pipeline := framework.NewPipeline()
	pipeline.Continues = true

	type InputData struct {
		Index      string `json:"index"`
		Filter     string `json:"filter"`
		Collection string `json:"collection"`
		LastReadID int    `json:"lastreadid"`
	}

	inputData := InputData{
		Index:      "nebulastore",
		Filter:     `{"query": {"match_all": {}}}`,
		Collection: "journalevents",
		LastReadID: 0,
	}

	// Add a CSV reader stage to the pipeline
	pipeline.AddStage(stages.NewEmbedings())
	return pipeline, inputData
}
