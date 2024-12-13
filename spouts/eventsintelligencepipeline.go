package spouts

import (
	"Nebula.Conduit/framework"
	"Nebula.Conduit/stages"
)

func NewEventsIntelligence() *framework.Pipeline {
	pipeline := framework.NewPipeline()
	pipeline.AddStage(stages.NewEmbedings())
	pipeline.AddStage(stages.NewDataProbe())
	pipeline.AddStage(stages.NewResponder())
	pipeline.AddStage(stages.NewSummary())
	pipeline.AddStage(stages.NewQueryGenerator())
	pipeline.AddStage(stages.NewKnowledgeBase())
	return pipeline

}
