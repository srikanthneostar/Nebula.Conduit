package components

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// HL7ReaderComponent reads and parses HL7 flat files
type HL7ReaderComponent struct {
	config   pipeline.ComponentConfig
	filePath string
}

// NewHL7ReaderComponent creates a new HL7 reader component
func NewHL7ReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	filePath, ok := config.Parameters["file_path"].(string)
	if !ok || filePath == "" {
		return nil, fmt.Errorf("file_path parameter is required")
	}

	return &HL7ReaderComponent{
		config:   config,
		filePath: filePath,
	}, nil
}

// Execute runs the HL7 reader component
func (h *HL7ReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	return h.Start(ctx)
}

// Start begins reading and parsing the HL7 file
func (h *HL7ReaderComponent) Start(ctx context.Context) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 100)

	file, err := os.Open(h.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open HL7 file: %w", err)
	}

	go func() {
		defer close(output)
		defer file.Close()

		scanner := bufio.NewScanner(file)
		// HL7 messages are separated by MSH segments
		var currentMessage []string
		messageCount := 0

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			line := scanner.Text()
			if strings.TrimSpace(line) == "" {
				continue
			}

			// MSH segment marks the start of a new message
			if strings.HasPrefix(line, "MSH") {
				// Emit previous message if exists
				if len(currentMessage) > 0 {
					messageCount++
					data := h.buildData(currentMessage, messageCount)
					select {
					case output <- data:
					case <-ctx.Done():
						return
					}
				}
				currentMessage = []string{line}
			} else {
				currentMessage = append(currentMessage, line)
			}
		}

		// Emit last message
		if len(currentMessage) > 0 {
			messageCount++
			data := h.buildData(currentMessage, messageCount)
			select {
			case output <- data:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output, nil
}

// buildData converts HL7 message lines into a pipeline Data struct
func (h *HL7ReaderComponent) buildData(lines []string, messageNum int) pipeline.Data {
	// Parse segments into a structured map
	segments := make(map[string][]map[string]string)
	for _, line := range lines {
		segType, fields := parseHL7Segment(line)
		segments[segType] = append(segments[segType], fields)
	}

	return pipeline.Data{
		Payload: map[string]interface{}{
			"raw_message": strings.Join(lines, "\r"),
			"segments":    segments,
		},
		Metadata: map[string]string{
			"source":        h.filePath,
			"message_index": fmt.Sprintf("%d", messageNum),
			"message_type":  extractMessageType(lines),
		},
		Timestamp: time.Now(),
		TraceID:   h.config.ID,
	}
}

// parseHL7Segment parses a single HL7 segment line into segment type and fields
func parseHL7Segment(line string) (string, map[string]string) {
	// Default separator is |
	separator := "|"
	if len(line) > 3 && strings.HasPrefix(line, "MSH") {
		separator = string(line[3])
	}

	parts := strings.Split(line, separator)
	segType := parts[0]

	fields := make(map[string]string)
	for i, part := range parts {
		fields[fmt.Sprintf("%s.%d", segType, i)] = part
	}

	return segType, fields
}

// extractMessageType extracts the message type from the MSH segment
func extractMessageType(lines []string) string {
	for _, line := range lines {
		if strings.HasPrefix(line, "MSH") {
			sep := "|"
			if len(line) > 3 {
				sep = string(line[3])
			}
			parts := strings.Split(line, sep)
			if len(parts) > 8 {
				return parts[8] // MSH-9 is message type
			}
		}
	}
	return "unknown"
}

func (h *HL7ReaderComponent) Validate() error {
	if h.filePath == "" {
		return fmt.Errorf("file_path is required")
	}
	return nil
}

func (h *HL7ReaderComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeHL7Reader
}

func (h *HL7ReaderComponent) ID() string { return h.config.ID }

func (h *HL7ReaderComponent) Config() pipeline.ComponentConfig { return h.config }
