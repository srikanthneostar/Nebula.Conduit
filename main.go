package main

import (
	"Nebula.Conduit/services"
	"Nebula.Conduit/spouts"
)

// Data represents a single data point

func main() {
	services.Ingest()
	pipelineService := services.NewPipelineService()
	pipelineService.AddPipeline(spouts.CsvToJsonPipeline())
	// pipelineService.AddPipeline(spouts.NewEventsItenlligence())
	pipelineService.Execute()
}
