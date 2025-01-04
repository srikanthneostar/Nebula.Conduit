package main

import (
	"context"

	"Nebula.Conduit/services"
	"Nebula.Conduit/spouts"
)

// Data represents a single data point

func main() {
	ctx := context.Background()

	pipelineService := services.NewPipelineService(ctx)
	pipeline, newCtx := spouts.CreateEmbedingsPipeline()

	//pipelineService.AddPipeline(spouts.CsvToJsonPipeline())
	pipelineService.AddPipeline(pipeline)
	// pipelineService.AddPipeline(spouts.NewEventsItenlligence())
	pipelineService.Execute(newCtx)
}
