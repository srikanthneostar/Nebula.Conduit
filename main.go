package main

import (
	"Nebula.Conduit/services"
	"Nebula.Conduit/spouts"
)

// Data represents a single data point

func main() {
	pipelineService := services.NewPipelineService()
	pipelineService.AddPipeline(spouts.CsvToJsonPipeline())
	pipelineService.Execute()
}
