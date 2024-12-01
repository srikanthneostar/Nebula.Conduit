package stages

import (
	"io"
	"os"

	"Nebula.Conduit/framework"
)

// JSONWriterStage writes the data to a JSON file
type JSONWriterStage struct {
	filename string
}

// NewJSONWriterStage returns a new JSON writer stage
func NewJSONWriterStage(filename string) framework.Stage {
	return &JSONWriterStage{filename: filename}
}

// Execute executes the JSON writer stage
func (s *JSONWriterStage) Execute(input io.Reader) error {
	file, err := os.Create(s.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, input)
	if err != nil {
		return err
	}

	return nil
}

// Output returns the output of the JSON writer stage
func (s *JSONWriterStage) Output() io.Reader {
	return nil
}
