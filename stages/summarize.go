package stages

/// Take the content from previous step and summarize using OLLama , this is 4th node
import (
	"io"

	"Nebula.Conduit/framework"
)

type Summarize struct{}

// Execute implements framework.Stage.
func (e *Summarize) Execute(input io.Reader) error {
	panic("unimplemented")
}

// Output implements framework.Stage.
func (e *Summarize) Output() io.Reader {
	panic("unimplemented")
}

func NewSummary() framework.Stage {
	return &Summarize{}
}
