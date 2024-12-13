package stages

/// Take the content and question from DataProbe and get the Answer from OLLama , till here 3 nodes
import (
	"io"

	"Nebula.Conduit/framework"
)

type Responder struct{}

// Execute implements framework.Stage.
func (e *Responder) Execute(input io.Reader) error {
	panic("unimplemented")
}

// Output implements framework.Stage.
func (e *Responder) Output() io.Reader {
	panic("unimplemented")
}

func NewResponder() framework.Stage {
	return &Responder{}
}
