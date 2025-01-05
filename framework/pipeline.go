package framework

import (
	"encoding/json"
	"strings"
)

// Pipeline represents a data pipeline
type Pipeline struct {
	stages    []Stage
	Continues bool
	WaitTime  int
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
func (p *Pipeline) Execute(ctx interface{}) {
	for i, stage := range p.stages {
		if i == 0 {

			if ctx != nil {
				inputData, err := json.Marshal(ctx)
				if err != nil {
					panic(err)
				}

				stage.Execute(strings.NewReader(string(inputData)))
			}

		} else {
			prevStage := p.stages[i-1]
			stage.Execute(prevStage.Output())
		}
	}
}
