package stages

/// Based on configuration ask the question and return questions and similarity search results
import (
	"io"

	"Nebula.Conduit/framework"
)

type DataProbe struct{}

// Execute implements framework.Stage.
func (e *DataProbe) Execute(input io.Reader) error {
	panic("unimplemented")
}

// Output implements framework.Stage.
func (e *DataProbe) Output() io.Reader {
	panic("unimplemented")
}

func NewDataProbe() framework.Stage {
	return &DataProbe{}
}
