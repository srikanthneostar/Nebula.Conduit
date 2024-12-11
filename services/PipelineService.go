package services

import (
	"Nebula.Conduit/framework"
)

type PipelineService struct {
	pipelines *[]framework.Pipeline
}

func NewPipelineService() *PipelineService {
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
func (s *PipelineService) Execute() {
	pipelines := *s.pipelines
	for _, pipeline := range pipelines {
		go func(p *framework.Pipeline) {
			p.Execute()
		}(&pipeline)
	}
}
