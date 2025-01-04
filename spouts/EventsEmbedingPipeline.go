package spouts

import (
	"Nebula.Conduit/framework"
	"Nebula.Conduit/models"
	"Nebula.Conduit/stages"
)

func CreateEmbedingsPipeline() (*framework.Pipeline, interface{}) {
	pipeline := framework.NewPipeline()
	pipeline.Continues = true

	inputData := models.EventsInputData{
		Index:      "nebulastore",
		Collection: "journalevents",
		LastReadID: 0,
	}

	// Add a CSV reader stage to the pipeline
	pipeline.AddStage(stages.NewEmbedings())
	return pipeline, inputData
}
