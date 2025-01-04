package services

import (
	"context"
	"sync"
	"time"

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
func (s *PipelineService) Execute(ctx interface{}) {
	pipelines := *s.pipelines
	s.executePipelines(pipelines, ctx)
}

func (s *PipelineService) executePipelines(pipelines []framework.Pipeline, ctx interface{}) {
	for _, pipeline := range pipelines {
		if pipeline.Continues {
			for pipeline.Continues {
				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					defer wg.Done()
					s.executePipeline(ctx, pipeline)
				}()
				wg.Wait()
				time.Sleep(time.Millisecond * 20000)
			}
		} else {
			s.executePipeline(ctx, pipeline)
		}
	}
}

func (*PipelineService) executePipeline(ctx interface{}, pipeline framework.Pipeline) {
	go func(p *framework.Pipeline) {
		p.Execute(ctx)
	}(&pipeline)
}
