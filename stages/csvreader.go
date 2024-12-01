package stages

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"

	"Nebula.Conduit/framework"
)

type CSVReaderStage struct {
	filename string
	records *[]map[string]string
}

// Execute executes the CSV reader stage
func (s *CSVReaderStage) Execute(input io.Reader) error {
	file, err := os.Open(s.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	headers := records[0]
	var dataRecords []map[string]string

	for _, record := range records[1:] {
		row := make(map[string]string)
		for i, value := range record {
			if i < len(headers) {
				row[headers[i]] = value
			}
		}
		dataRecords = append(dataRecords, row)
	}
	
	s.records = &dataRecords
	return nil
}

// Output implements framework.Stage.
func (c *CSVReaderStage) Output() io.Reader {
	jsonData, _ := json.Marshal(*c.records)
	return bytes.NewReader(jsonData)

}

// NewCSVReaderStage returns a new CSV reader stage
func NewCSVReaderStage(filename string) framework.Stage {
	return &CSVReaderStage{filename: filename}
}
