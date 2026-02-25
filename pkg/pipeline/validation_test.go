package pipeline

import (
	"testing"
)

// TestValidateHTTPGetConfig tests HTTP GET component validation
func TestValidateHTTPGetConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid HTTP GET config",
			config: ComponentConfig{
				ID:   "http-1",
				Type: ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
				},
			},
			wantErr: false,
		},
		{
			name: "missing URL parameter",
			config: ComponentConfig{
				ID:         "http-1",
				Type:       ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{},
			},
			wantErr: true,
			errMsg:  "requires 'url' parameter",
		},
		{
			name: "invalid URL format",
			config: ComponentConfig{
				ID:   "http-1",
				Type: ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": "not-a-valid-url",
				},
			},
			wantErr: true,
			errMsg:  "invalid URL",
		},
		{
			name: "empty URL",
			config: ComponentConfig{
				ID:   "http-1",
				Type: ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": "",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateComponentConfig() error = %v, want error containing %v", err, tt.errMsg)
				}
			}
		})
	}
}

// TestValidateHTTPPostConfig tests HTTP POST component validation
func TestValidateHTTPPostConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid HTTP POST config",
			config: ComponentConfig{
				ID:   "http-post-1",
				Type: ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url":          "https://api.example.com/data",
					"content_type": "application/json",
				},
			},
			wantErr: false,
		},
		{
			name: "missing URL parameter",
			config: ComponentConfig{
				ID:   "http-post-1",
				Type: ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"content_type": "application/json",
				},
			},
			wantErr: true,
			errMsg:  "requires 'url' parameter",
		},
		{
			name: "missing content_type parameter",
			config: ComponentConfig{
				ID:   "http-post-1",
				Type: ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
				},
			},
			wantErr: true,
			errMsg:  "requires 'content_type' parameter",
		},
		{
			name: "empty content_type",
			config: ComponentConfig{
				ID:   "http-post-1",
				Type: ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url":          "https://api.example.com/data",
					"content_type": "",
				},
			},
			wantErr: true,
			errMsg:  "cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateComponentConfig() error = %v, want error containing %v", err, tt.errMsg)
				}
			}
		})
	}
}

// TestValidateSQLQueryConfig tests SQL Query component validation
func TestValidateSQLQueryConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid SQL Query config",
			config: ComponentConfig{
				ID:   "sql-1",
				Type: ComponentTypeSQLQuery,
				Parameters: map[string]interface{}{
					"connection_string": "Server=localhost;Database=test;",
					"query":             "SELECT * FROM users",
				},
			},
			wantErr: false,
		},
		{
			name: "missing connection_string",
			config: ComponentConfig{
				ID:   "sql-1",
				Type: ComponentTypeSQLQuery,
				Parameters: map[string]interface{}{
					"query": "SELECT * FROM users",
				},
			},
			wantErr: true,
			errMsg:  "requires 'connection_string' parameter",
		},
		{
			name: "missing query",
			config: ComponentConfig{
				ID:   "sql-1",
				Type: ComponentTypeSQLQuery,
				Parameters: map[string]interface{}{
					"connection_string": "Server=localhost;Database=test;",
				},
			},
			wantErr: true,
			errMsg:  "requires 'query' parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateComponentConfig() error = %v, want error containing %v", err, tt.errMsg)
				}
			}
		})
	}
}

// TestValidateCSVReaderConfig tests CSV Reader component validation
func TestValidateCSVReaderConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid CSV Reader config",
			config: ComponentConfig{
				ID:   "csv-1",
				Type: ComponentTypeCSVReader,
				Parameters: map[string]interface{}{
					"file_path": "/data/input.csv",
				},
			},
			wantErr: false,
		},
		{
			name: "missing file_path",
			config: ComponentConfig{
				ID:         "csv-1",
				Type:       ComponentTypeCSVReader,
				Parameters: map[string]interface{}{},
			},
			wantErr: true,
			errMsg:  "requires 'file_path' parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateKafkaConsumerConfig tests Kafka Consumer component validation
func TestValidateKafkaConsumerConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid Kafka Consumer config with string slice",
			config: ComponentConfig{
				ID:   "kafka-consumer-1",
				Type: ComponentTypeKafkaConsumer,
				Parameters: map[string]interface{}{
					"brokers": []string{"localhost:9092", "localhost:9093"},
					"topic":   "test-topic",
				},
			},
			wantErr: false,
		},
		{
			name: "valid Kafka Consumer config with interface slice",
			config: ComponentConfig{
				ID:   "kafka-consumer-1",
				Type: ComponentTypeKafkaConsumer,
				Parameters: map[string]interface{}{
					"brokers": []interface{}{"localhost:9092"},
					"topic":   "test-topic",
				},
			},
			wantErr: false,
		},
		{
			name: "missing brokers",
			config: ComponentConfig{
				ID:   "kafka-consumer-1",
				Type: ComponentTypeKafkaConsumer,
				Parameters: map[string]interface{}{
					"topic": "test-topic",
				},
			},
			wantErr: true,
			errMsg:  "requires 'brokers' parameter",
		},
		{
			name: "missing topic",
			config: ComponentConfig{
				ID:   "kafka-consumer-1",
				Type: ComponentTypeKafkaConsumer,
				Parameters: map[string]interface{}{
					"brokers": []string{"localhost:9092"},
				},
			},
			wantErr: true,
			errMsg:  "requires 'topic' parameter",
		},
		{
			name: "invalid broker format",
			config: ComponentConfig{
				ID:   "kafka-consumer-1",
				Type: ComponentTypeKafkaConsumer,
				Parameters: map[string]interface{}{
					"brokers": []string{"invalid-broker"},
					"topic":   "test-topic",
				},
			},
			wantErr: true,
			errMsg:  "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateKafkaProducerConfig tests Kafka Producer component validation
func TestValidateKafkaProducerConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
	}{
		{
			name: "valid Kafka Producer config",
			config: ComponentConfig{
				ID:   "kafka-producer-1",
				Type: ComponentTypeKafkaProducer,
				Parameters: map[string]interface{}{
					"brokers": []string{"localhost:9092"},
					"topic":   "output-topic",
				},
			},
			wantErr: false,
		},
		{
			name: "missing brokers",
			config: ComponentConfig{
				ID:   "kafka-producer-1",
				Type: ComponentTypeKafkaProducer,
				Parameters: map[string]interface{}{
					"topic": "output-topic",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateRabbitMQConsumerConfig tests RabbitMQ Consumer component validation
func TestValidateRabbitMQConsumerConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid RabbitMQ Consumer config",
			config: ComponentConfig{
				ID:   "rabbitmq-consumer-1",
				Type: ComponentTypeRabbitMQConsumer,
				Parameters: map[string]interface{}{
					"connection_url": "amqp://guest:guest@localhost:5672/",
					"queue":          "test-queue",
				},
			},
			wantErr: false,
		},
		{
			name: "valid RabbitMQ Consumer config with amqps",
			config: ComponentConfig{
				ID:   "rabbitmq-consumer-1",
				Type: ComponentTypeRabbitMQConsumer,
				Parameters: map[string]interface{}{
					"connection_url": "amqps://user:pass@example.com:5671/",
					"queue":          "test-queue",
				},
			},
			wantErr: false,
		},
		{
			name: "missing connection_url",
			config: ComponentConfig{
				ID:   "rabbitmq-consumer-1",
				Type: ComponentTypeRabbitMQConsumer,
				Parameters: map[string]interface{}{
					"queue": "test-queue",
				},
			},
			wantErr: true,
			errMsg:  "requires 'connection_url' parameter",
		},
		{
			name: "missing queue",
			config: ComponentConfig{
				ID:   "rabbitmq-consumer-1",
				Type: ComponentTypeRabbitMQConsumer,
				Parameters: map[string]interface{}{
					"connection_url": "amqp://localhost:5672/",
				},
			},
			wantErr: true,
			errMsg:  "requires 'queue' parameter",
		},
		{
			name: "invalid connection URL scheme",
			config: ComponentConfig{
				ID:   "rabbitmq-consumer-1",
				Type: ComponentTypeRabbitMQConsumer,
				Parameters: map[string]interface{}{
					"connection_url": "http://localhost:5672/",
					"queue":          "test-queue",
				},
			},
			wantErr: true,
			errMsg:  "must use amqp or amqps scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateRabbitMQProducerConfig tests RabbitMQ Producer component validation
func TestValidateRabbitMQProducerConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid RabbitMQ Producer config",
			config: ComponentConfig{
				ID:   "rabbitmq-producer-1",
				Type: ComponentTypeRabbitMQProducer,
				Parameters: map[string]interface{}{
					"connection_url": "amqp://guest:guest@localhost:5672/",
					"exchange":       "test-exchange",
				},
			},
			wantErr: false,
		},
		{
			name: "missing exchange",
			config: ComponentConfig{
				ID:   "rabbitmq-producer-1",
				Type: ComponentTypeRabbitMQProducer,
				Parameters: map[string]interface{}{
					"connection_url": "amqp://localhost:5672/",
				},
			},
			wantErr: true,
			errMsg:  "requires 'exchange' parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateHL7ReaderConfig tests HL7 Reader component validation
func TestValidateHL7ReaderConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
	}{
		{
			name: "valid HL7 Reader config",
			config: ComponentConfig{
				ID:   "hl7-1",
				Type: ComponentTypeHL7Reader,
				Parameters: map[string]interface{}{
					"file_path": "/data/hl7/message.hl7",
				},
			},
			wantErr: false,
		},
		{
			name: "missing file_path",
			config: ComponentConfig{
				ID:         "hl7-1",
				Type:       ComponentTypeHL7Reader,
				Parameters: map[string]interface{}{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateTCPReadConfig tests TCP Read component validation
func TestValidateTCPReadConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid TCP Read config - server mode",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "0.0.0.0",
					"port": 8080,
					"mode": "server",
				},
			},
			wantErr: false,
		},
		{
			name: "valid TCP Read config - client mode",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"port": 8080,
					"mode": "client",
				},
			},
			wantErr: false,
		},
		{
			name: "valid TCP Read config - port as float64",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"port": float64(8080),
					"mode": "server",
				},
			},
			wantErr: false,
		},
		{
			name: "missing host",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"port": 8080,
					"mode": "server",
				},
			},
			wantErr: true,
			errMsg:  "requires 'host' parameter",
		},
		{
			name: "missing port",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"mode": "server",
				},
			},
			wantErr: true,
			errMsg:  "requires 'port' parameter",
		},
		{
			name: "missing mode",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"port": 8080,
				},
			},
			wantErr: true,
			errMsg:  "requires 'mode' parameter",
		},
		{
			name: "invalid mode",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"port": 8080,
					"mode": "invalid",
				},
			},
			wantErr: true,
			errMsg:  "must be either 'server' or 'client'",
		},
		{
			name: "invalid port - too high",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"port": 70000,
					"mode": "server",
				},
			},
			wantErr: true,
			errMsg:  "port must be between 1 and 65535",
		},
		{
			name: "invalid port - zero",
			config: ComponentConfig{
				ID:   "tcp-read-1",
				Type: ComponentTypeTCPRead,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"port": 0,
					"mode": "server",
				},
			},
			wantErr: true,
			errMsg:  "port must be between 1 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateTCPWriteConfig tests TCP Write component validation
func TestValidateTCPWriteConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid TCP Write config - server mode",
			config: ComponentConfig{
				ID:   "tcp-write-1",
				Type: ComponentTypeTCPWrite,
				Parameters: map[string]interface{}{
					"host": "0.0.0.0",
					"port": 9090,
					"mode": "server",
				},
			},
			wantErr: false,
		},
		{
			name: "valid TCP Write config - client mode",
			config: ComponentConfig{
				ID:   "tcp-write-1",
				Type: ComponentTypeTCPWrite,
				Parameters: map[string]interface{}{
					"host": "remote.server.com",
					"port": 9090,
					"mode": "client",
				},
			},
			wantErr: false,
		},
		{
			name: "missing host",
			config: ComponentConfig{
				ID:   "tcp-write-1",
				Type: ComponentTypeTCPWrite,
				Parameters: map[string]interface{}{
					"port": 9090,
					"mode": "server",
				},
			},
			wantErr: true,
			errMsg:  "requires 'host' parameter",
		},
		{
			name: "invalid mode",
			config: ComponentConfig{
				ID:   "tcp-write-1",
				Type: ComponentTypeTCPWrite,
				Parameters: map[string]interface{}{
					"host": "localhost",
					"port": 9090,
					"mode": "both",
				},
			},
			wantErr: true,
			errMsg:  "must be either 'server' or 'client'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidatePythonCodeBlockConfig tests Python Code Block component validation
func TestValidatePythonCodeBlockConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid Python Code Block config",
			config: ComponentConfig{
				ID:   "python-1",
				Type: ComponentTypePythonCodeBlock,
				Parameters: map[string]interface{}{
					"code": "import json\ndata = json.loads(input_data)\noutput_data = json.dumps(data)",
				},
			},
			wantErr: false,
		},
		{
			name: "missing code parameter",
			config: ComponentConfig{
				ID:         "python-1",
				Type:       ComponentTypePythonCodeBlock,
				Parameters: map[string]interface{}{},
			},
			wantErr: true,
			errMsg:  "requires 'code' parameter",
		},
		{
			name: "empty code",
			config: ComponentConfig{
				ID:   "python-1",
				Type: ComponentTypePythonCodeBlock,
				Parameters: map[string]interface{}{
					"code": "",
				},
			},
			wantErr: true,
			errMsg:  "cannot be empty",
		},
		{
			name: "whitespace-only code",
			config: ComponentConfig{
				ID:   "python-1",
				Type: ComponentTypePythonCodeBlock,
				Parameters: map[string]interface{}{
					"code": "   \n\t  ",
				},
			},
			wantErr: true,
			errMsg:  "cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateLogConfig tests Log component validation
func TestValidateLogConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ComponentConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid Log config - debug",
			config: ComponentConfig{
				ID:   "log-1",
				Type: ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "debug",
				},
			},
			wantErr: false,
		},
		{
			name: "valid Log config - info",
			config: ComponentConfig{
				ID:   "log-1",
				Type: ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "info",
				},
			},
			wantErr: false,
		},
		{
			name: "valid Log config - warn",
			config: ComponentConfig{
				ID:   "log-1",
				Type: ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "warn",
				},
			},
			wantErr: false,
		},
		{
			name: "valid Log config - error",
			config: ComponentConfig{
				ID:   "log-1",
				Type: ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "error",
				},
			},
			wantErr: false,
		},
		{
			name: "missing log_level parameter",
			config: ComponentConfig{
				ID:         "log-1",
				Type:       ComponentTypeLog,
				Parameters: map[string]interface{}{},
			},
			wantErr: true,
			errMsg:  "requires 'log_level' parameter",
		},
		{
			name: "invalid log_level",
			config: ComponentConfig{
				ID:   "log-1",
				Type: ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "trace",
				},
			},
			wantErr: true,
			errMsg:  "must be one of: debug, info, warn, error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateComponentConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateComponentConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateComponentConfig_MissingID tests validation of missing component ID
func TestValidateComponentConfig_MissingID(t *testing.T) {
	config := ComponentConfig{
		Type: ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": "https://api.example.com",
		},
	}

	err := ValidateComponentConfig(config)
	if err == nil {
		t.Error("ValidateComponentConfig() expected error for missing ID, got nil")
	}
	if !contains(err.Error(), "component ID is required") {
		t.Errorf("ValidateComponentConfig() error = %v, want error containing 'component ID is required'", err)
	}
}

// TestValidateComponentConfig_UnknownType tests validation of unknown component type
func TestValidateComponentConfig_UnknownType(t *testing.T) {
	config := ComponentConfig{
		ID:         "test-1",
		Type:       ComponentType("unknown_type"),
		Parameters: map[string]interface{}{},
	}

	err := ValidateComponentConfig(config)
	if err == nil {
		t.Error("ValidateComponentConfig() expected error for unknown type, got nil")
	}
	if !contains(err.Error(), "unknown component type") {
		t.Errorf("ValidateComponentConfig() error = %v, want error containing 'unknown component type'", err)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && containsHelper(s, substr)))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
