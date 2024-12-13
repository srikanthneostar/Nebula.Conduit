package stages

/// Take the Summary from previous step and generate question using OLLama, this 5th node
import (
	"io"

	"Nebula.Conduit/framework"
)

type QueryGenerator struct{}

// Execute implements framework.Stage.
func (e *QueryGenerator) Execute(input io.Reader) error {
	panic("unimplemented")
}

// Output implements framework.Stage.
func (e *QueryGenerator) Output() io.Reader {
	panic("unimplemented")
}

func NewQueryGenerator() framework.Stage {
	return &QueryGenerator{}
}
