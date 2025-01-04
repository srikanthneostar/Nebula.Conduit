package spouts

import (
	"context"

	"Nebula.Conduit/framework"
	"Nebula.Conduit/stages"
)

func CreateEmbedingsPipeline(ctx context.Context) *framework.Pipeline {
	pipeline := framework.NewPipeline()
	pipeline.Continues = true
	inputData := map[string]interface{}{
		"index":      "nebulastore",
		"filter":     `{"term": {"has_embedding": false}}`,
		"collection": "journalevents",
	}

	ctx = context.WithValue(ctx, "input", inputData)

	// Add a CSV reader stage to the pipeline
	pipeline.AddStage(stages.NewEmbedings())
	return pipeline
}
