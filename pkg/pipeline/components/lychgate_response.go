package components

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/Xecutables/Nebula.Conduit/pkg/s3bucket"
	_ "github.com/mattn/go-sqlite3"
)

// CommunicationMode represents the supported messaging protocols for Lychgate responses.
type CommunicationMode int

const (
	CommunicationModeRabbitMQ CommunicationMode = iota
	CommunicationModeKafka
	CommunicationModeMQTT
)

// String returns the human-readable name of the CommunicationMode.
func (m CommunicationMode) String() string {
	switch m {
	case CommunicationModeRabbitMQ:
		return "RabbitMQ"
	case CommunicationModeKafka:
		return "Kafka"
	case CommunicationModeMQTT:
		return "MQTT"
	default:
		return "Unknown"
	}
}

// ParseCommunicationMode converts a string value into a CommunicationMode enum.
// Matching is case-insensitive. Returns an error for unrecognised values.
func ParseCommunicationMode(s string) (CommunicationMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "rabbitmq":
		return CommunicationModeRabbitMQ, nil
	case "kafka":
		return CommunicationModeKafka, nil
	case "mqtt":
		return CommunicationModeMQTT, nil
	default:
		return 0, fmt.Errorf("invalid communication_mode %q: must be one of RabbitMQ, Kafka, MQTT", s)
	}
}

// LychgateResponseComponent is a processor component that enriches pipeline data
// with Lychgate system routing metadata (SystemId, EntityId, SchemaClass, CommunicationMode).
type LychgateResponseComponent struct {
	config            pipeline.ComponentConfig
	systemID          int
	entityID          int
	schemaClass       string
	communicationMode CommunicationMode
	db                *sql.DB
}

// NewLychgateResponseComponent creates a new Lychgate Response component.
//
// Required pipeline parameters:
//   - system_id          (int)    – the Lychgate system identifier
//   - entity_id          (int)    – the Lychgate entity identifier
//   - schema_class       (string) – the schema class name
//   - communication_mode (string) – one of "RabbitMQ", "Kafka", "MQTT"
func NewLychgateResponseComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	systemID, err := pipeline.ParamInt(config.Parameters, "system_id", 0)
	if err != nil {
		return nil, fmt.Errorf("lychgate_response: %w", err)
	}
	if systemID == 0 {
		return nil, fmt.Errorf("lychgate_response: parameter \"system_id\" is required and must be non-zero")
	}

	entityID, err := pipeline.ParamInt(config.Parameters, "entity_id", 0)
	if err != nil {
		return nil, fmt.Errorf("lychgate_response: %w", err)
	}
	if entityID == 0 {
		return nil, fmt.Errorf("lychgate_response: parameter \"entity_id\" is required and must be non-zero")
	}

	schemaClass, err := pipeline.ParamStringRequired(config.Parameters, "schema_class")
	if err != nil {
		return nil, fmt.Errorf("lychgate_response: %w", err)
	}

	modeStr, err := pipeline.ParamStringRequired(config.Parameters, "communication_mode")
	if err != nil {
		return nil, fmt.Errorf("lychgate_response: %w", err)
	}
	communicationMode, err := ParseCommunicationMode(modeStr)
	if err != nil {
		return nil, fmt.Errorf("lychgate_response: %w", err)
	}

	// Download Nebula.db from S3 only if it doesn't already exist locally
	const dbPath = "./Nebula.db"
	if _, statErr := os.Stat(dbPath); os.IsNotExist(statErr) {
		s3svc, err := s3bucket.NewS3BucketService(nil)
		if err != nil {
			return nil, fmt.Errorf("lychgate_response: failed to initialize S3 bucket service: %w", err)
		}
		if err := s3svc.DownloadFile("Nebula.db", dbPath); err != nil {
			return nil, fmt.Errorf("lychgate_response: failed to download Nebula.db from bucket: %w", err)
		}
	}

	// Open SQLite client for the downloaded database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("lychgate_response: failed to open SQLite database: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("lychgate_response: failed to ping SQLite database: %w", err)
	}

	return &LychgateResponseComponent{
		config:            config,
		systemID:          systemID,
		entityID:          entityID,
		schemaClass:       schemaClass,
		communicationMode: communicationMode,
		db:                db,
	}, nil
}

// produceRequest enriches the given Data with Lychgate routing metadata and returns
// the modified Data ready for downstream consumption. It has full access to the
// component's configured properties (systemID, entityID, schemaClass, communicationMode).
func (l *LychgateResponseComponent) produceRequest(data pipeline.Data) pipeline.Data {
	if data.Metadata == nil {
		data.Metadata = make(map[string]string)
	}
	data.Metadata["lychgate_system_id"] = fmt.Sprintf("%d", l.systemID)
	data.Metadata["lychgate_entity_id"] = fmt.Sprintf("%d", l.entityID)
	data.Metadata["lychgate_schema_class"] = l.schemaClass
	data.Metadata["lychgate_communication_mode"] = l.communicationMode.String()

	return data
}

// Execute processes each incoming Data item via produceRequest and forwards it downstream.
func (l *LychgateResponseComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 100)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					return
				}

				enriched := l.produceRequest(data)

				select {
				case output <- enriched:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

// Validate checks that all required configuration is present and valid.
func (l *LychgateResponseComponent) Validate() error {
	if l.systemID == 0 {
		return fmt.Errorf("system_id is required and must be non-zero")
	}
	if l.entityID == 0 {
		return fmt.Errorf("entity_id is required and must be non-zero")
	}
	if l.schemaClass == "" {
		return fmt.Errorf("schema_class is required")
	}
	return nil
}

func (l *LychgateResponseComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeLychgateResponse
}

func (l *LychgateResponseComponent) ID() string {
	return l.config.ID
}

func (l *LychgateResponseComponent) Config() pipeline.ComponentConfig {
	return l.config
}
