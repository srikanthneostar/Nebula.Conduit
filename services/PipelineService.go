package services

import (
	"context"

	"Nebula.Conduit/framework"
)

type PipelineService struct {
	pipelines *[]framework.Pipeline
}

func NewPipelineService(ctx ...context.Context) *PipelineService {
	return &PipelineService{
		pipelines: &[]framework.Pipeline{},
	}
}
func (s *PipelineService) AddPipeline(pipeline *framework.Pipeline) error {
	if s.pipelines == nil {
		s.pipelines = &[]framework.Pipeline{}
	}
	*s.pipelines = append(*s.pipelines, *pipeline)
	return nil
}
func (s *PipelineService) Execute(ctx ...context.Context) {
	pipelines := *s.pipelines
	s.executePipelines(pipelines, ctx)
}

func (s *PipelineService) executePipelines(pipelines []framework.Pipeline, ctx []context.Context) {
	for _, pipeline := range pipelines {
		if pipeline.Continues {
			for pipeline.Continues {
				s.executePipeline(ctx, pipeline)
			}
		} else {
			s.executePipeline(ctx, pipeline)
		}
	}
}

func (*PipelineService) executePipeline(ctx []context.Context, pipeline framework.Pipeline) {
	go func(p *framework.Pipeline) {
		if len(ctx) > 0 {
			p.Execute(ctx[0])
		} else {
			p.Execute(context.Background())
		}
	}(&pipeline)
}
