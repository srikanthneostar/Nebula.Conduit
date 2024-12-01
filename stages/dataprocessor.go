package stages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"Nebula.Conduit/framework"
)

// DataProcessorStage processes the data
type DataProcessorStage struct {
	records *[]map[string]string
}

// NewDataProcessorStage returns a new data processor stage
func NewDataProcessorStage() framework.Stage {
	return &DataProcessorStage{}
}

// Execute executes the data processor stage
func (s *DataProcessorStage) Execute(input io.Reader) error {
	decoder := json.NewDecoder(input)

	var dataRecords []map[string]string
	if err := decoder.Decode(&dataRecords); err != nil {
		return fmt.Errorf("failed to decode data: %v", err.Error())
	}

	s.records = &dataRecords
	return nil
}

// Output returns the output of the data processor stage
func (s *DataProcessorStage) Output() io.Reader {
	jsonData, _ := json.Marshal(&s.records)
	return bytes.NewReader(jsonData)
}
