package spouts

import (
	"Nebula.Conduit/framework"
	"Nebula.Conduit/stages"
)

func CsvToJsonPipeline() *framework.Pipeline {
	pipeline := framework.NewPipeline()
	// Add a CSV reader stage to the pipeline
	pipeline.AddStage(stages.NewCSVReaderStage("data.csv"))

	// Add a data processor stage to the pipeline
	pipeline.AddStage(stages.NewDataProcessorStage())

	// Add a JSON writer stage to the pipeline
	pipeline.AddStage(stages.NewJSONWriterStage("output.json"))
	return pipeline

}
