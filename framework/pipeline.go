package framework

// Pipeline represents a data pipeline
type Pipeline struct {
	stages []Stage
}

// NewPipeline returns a new pipeline
func NewPipeline() *Pipeline {
	return &Pipeline{}
}

// AddStage adds a new stage to the pipeline
func (p *Pipeline) AddStage(stage Stage) {
	p.stages = append(p.stages, stage)
}

// Execute executes the pipeline
func (p *Pipeline) Execute() {
	for i, stage := range p.stages {
		if i == 0 {
			stage.Execute(nil)
		} else {
			prevStage := p.stages[i-1]
			stage.Execute(prevStage.Output())
		}
	}
}
