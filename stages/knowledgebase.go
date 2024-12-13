package stages

/// Take all nodes and create an embeding and save into knowledgebase index, elastic search
import (
	"io"

	"Nebula.Conduit/framework"
)

type KnowledgeBase struct{}

// Execute implements framework.Stage.
func (e *KnowledgeBase) Execute(input io.Reader) error {
	panic("unimplemented")
}

// Output implements framework.Stage.
func (e *KnowledgeBase) Output() io.Reader {
	panic("unimplemented")
}

func NewKnowledgeBase() framework.Stage {
	return &KnowledgeBase{}
}
