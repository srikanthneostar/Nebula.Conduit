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
	pipelineService.AddPipeline(spouts.CsvToJsonPipeline())
	pipelineService.AddPipeline(spouts.CreateEmbedingsPipeline(ctx))
	// pipelineService.AddPipeline(spouts.NewEventsItenlligence())
	pipelineService.Execute(ctx)
}
