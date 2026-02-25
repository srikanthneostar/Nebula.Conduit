package components

import (
	"context"
	"fmt"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/segmentio/kafka-go"
)

// KafkaConsumerComponent consumes messages from Kafka topics
type KafkaConsumerComponent struct {
	config      pipeline.ComponentConfig
	brokers     []string
	topic       string
	groupID     string
	reader      *kafka.Reader
	startOffset int64
	maxWait     time.Duration
	minBytes    int
	maxBytes    int
}

// NewKafkaConsumerComponent creates a new Kafka consumer component
func NewKafkaConsumerComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract brokers from parameters
	brokersParam, ok := config.Parameters["brokers"]
	if !ok {
		return nil, fmt.Errorf("brokers parameter is required")
	}

	var brokers []string
	switch v := brokersParam.(type) {
	case []interface{}:
		for _, broker := range v {
			if strBroker, ok := broker.(string); ok {
				brokers = append(brokers, strBroker)
			}
		}
	case []string:
		brokers = v
	default:
		return nil, fmt.Errorf("brokers must be an array of strings")
	}

	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	// Extract topic from parameters
	topic, ok := config.Parameters["topic"].(string)
	if !ok || topic == "" {
		return nil, fmt.Errorf("topic parameter is required and must be a string")
	}

	// Extract group ID (optional, defaults to component ID)
	groupID, ok := config.Parameters["group_id"].(string)
	if !ok || groupID == "" {
		groupID = config.ID
	}

	// Extract start offset (optional, defaults to -1 for latest)
	startOffset := int64(-1)
	if offsetParam, ok := config.Parameters["start_offset"].(float64); ok {
		startOffset = int64(offsetParam)
	}

	// Extract max wait time (optional, defaults to 10s)
	maxWait := 10 * time.Second
	if maxWaitParam, ok := config.Parameters["max_wait"].(string); ok {
		parsedWait, err := time.ParseDuration(maxWaitParam)
		if err != nil {
			return nil, fmt.Errorf("invalid max_wait format: %w", err)
		}
		maxWait = parsedWait
	}

	// Extract min/max bytes (optional)
	minBytes := 1
	if minBytesParam, ok := config.Parameters["min_bytes"].(float64); ok {
		minBytes = int(minBytesParam)
	}

	maxBytes := int(10e6) // 10MB default
	if maxBytesParam, ok := config.Parameters["max_bytes"].(float64); ok {
		maxBytes = int(maxBytesParam)
	}

	return &KafkaConsumerComponent{
		config:      config,
		brokers:     brokers,
		topic:       topic,
		groupID:     groupID,
		startOffset: startOffset,
		maxWait:     maxWait,
		minBytes:    minBytes,
		maxBytes:    maxBytes,
	}, nil
}

// Execute runs the Kafka consumer component logic
func (k *KafkaConsumerComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	// Create Kafka reader
	k.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     k.brokers,
		Topic:       k.topic,
		GroupID:     k.groupID,
		StartOffset: k.startOffset,
		MaxWait:     k.maxWait,
		MinBytes:    k.minBytes,
		MaxBytes:    k.maxBytes,
	})

	go func() {
		defer close(output)
		defer k.reader.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Read message with context
				msg, err := k.reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						// Context cancelled, exit gracefully
						return
					}
					// Log error but continue if continue_on_error is true
					if !k.config.ContinueOnError {
						return
					}
					continue
				}

				// Create data payload
				data := pipeline.Data{
					Payload:   msg.Value,
					Metadata:  make(map[string]string),
					Timestamp: msg.Time,
					TraceID:   k.config.ID,
				}

				// Add message metadata
				data.Metadata["topic"] = msg.Topic
				data.Metadata["partition"] = fmt.Sprintf("%d", msg.Partition)
				data.Metadata["offset"] = fmt.Sprintf("%d", msg.Offset)
				data.Metadata["key"] = string(msg.Key)

				// Send data to output channel
				select {
				case output <- data:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

// Validate checks if the component configuration is valid
func (k *KafkaConsumerComponent) Validate() error {
	if len(k.brokers) == 0 {
		return fmt.Errorf("at least one broker address is required")
	}

	if k.topic == "" {
		return fmt.Errorf("topic is required")
	}

	return nil
}

// Type returns the component type identifier
func (k *KafkaConsumerComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeKafkaConsumer
}

// ID returns the unique component instance identifier
func (k *KafkaConsumerComponent) ID() string {
	return k.config.ID
}

// Config returns the component configuration
func (k *KafkaConsumerComponent) Config() pipeline.ComponentConfig {
	return k.config
}

// KafkaProducerComponent produces messages to Kafka topics
type KafkaProducerComponent struct {
	config      pipeline.ComponentConfig
	brokers     []string
	topic       string
	compression kafka.Compression
	writer      *kafka.Writer
	async       bool
}

// NewKafkaProducerComponent creates a new Kafka producer component
func NewKafkaProducerComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract brokers from parameters
	brokersParam, ok := config.Parameters["brokers"]
	if !ok {
		return nil, fmt.Errorf("brokers parameter is required")
	}

	var brokers []string
	switch v := brokersParam.(type) {
	case []interface{}:
		for _, broker := range v {
			if strBroker, ok := broker.(string); ok {
				brokers = append(brokers, strBroker)
			}
		}
	case []string:
		brokers = v
	default:
		return nil, fmt.Errorf("brokers must be an array of strings")
	}

	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	// Extract topic from parameters
	topic, ok := config.Parameters["topic"].(string)
	if !ok || topic == "" {
		return nil, fmt.Errorf("topic parameter is required and must be a string")
	}

	// Extract compression (optional, defaults to none)
	compression := kafka.Compression(0) // None
	if compressionParam, ok := config.Parameters["compression"].(string); ok {
		switch compressionParam {
		case "gzip":
			compression = kafka.Gzip
		case "snappy":
			compression = kafka.Snappy
		case "lz4":
			compression = kafka.Lz4
		case "zstd":
			compression = kafka.Zstd
		case "none":
			compression = kafka.Compression(0)
		default:
			return nil, fmt.Errorf("unsupported compression type: %s", compressionParam)
		}
	}

	// Extract async flag (optional, defaults to false)
	async := false
	if asyncParam, ok := config.Parameters["async"].(bool); ok {
		async = asyncParam
	}

	return &KafkaProducerComponent{
		config:      config,
		brokers:     brokers,
		topic:       topic,
		compression: compression,
		async:       async,
	}, nil
}

// Execute runs the Kafka producer component logic
func (k *KafkaProducerComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	// Create Kafka writer
	k.writer = &kafka.Writer{
		Addr:         kafka.TCP(k.brokers...),
		Topic:        k.topic,
		Compression:  k.compression,
		Balancer:     &kafka.LeastBytes{},
		Async:        k.async,
		RequiredAcks: kafka.RequireOne,
	}

	go func() {
		defer close(output)
		defer k.writer.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					// Input channel closed, we're done
					return
				}

				if err := k.sendMessage(ctx, data); err != nil {
					// Log error but continue if continue_on_error is true
					if !k.config.ContinueOnError {
						return
					}
				}

				// Pass data through to output channel
				select {
				case output <- data:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

// sendMessage sends a message to Kafka
func (k *KafkaProducerComponent) sendMessage(ctx context.Context, data pipeline.Data) error {
	// Convert payload to bytes
	var value []byte
	switch v := data.Payload.(type) {
	case []byte:
		value = v
	case string:
		value = []byte(v)
	default:
		return fmt.Errorf("unsupported payload type: %T", data.Payload)
	}

	// Extract key from metadata (optional)
	key := []byte(data.Metadata["key"])

	// Create Kafka message
	msg := kafka.Message{
		Key:   key,
		Value: value,
		Time:  data.Timestamp,
	}

	// Write message
	err := k.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	return nil
}

// Validate checks if the component configuration is valid
func (k *KafkaProducerComponent) Validate() error {
	if len(k.brokers) == 0 {
		return fmt.Errorf("at least one broker address is required")
	}

	if k.topic == "" {
		return fmt.Errorf("topic is required")
	}

	return nil
}

// Type returns the component type identifier
func (k *KafkaProducerComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeKafkaProducer
}

// ID returns the unique component instance identifier
func (k *KafkaProducerComponent) ID() string {
	return k.config.ID
}

// Config returns the component configuration
func (k *KafkaProducerComponent) Config() pipeline.ComponentConfig {
	return k.config
}
