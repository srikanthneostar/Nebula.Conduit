package pipeline

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/robfig/cron/v3"
)

// ValidatePipelineDefinition validates a complete pipeline definition
func ValidatePipelineDefinition(def PipelineDefinition) error {
	// Validate basic fields
	if def.Name == "" {
		return fmt.Errorf("pipeline name is required")
	}

	if def.ExecutionMode == "" {
		return fmt.Errorf("execution mode is required")
	}

	// Validate execution mode
	if def.ExecutionMode != ExecutionModeScheduled && def.ExecutionMode != ExecutionModeContinuous {
		return fmt.Errorf("execution mode must be 'scheduled' or 'continuous', got: %s", def.ExecutionMode)
	}

	// Validate cron expression for scheduled pipelines
	if def.ExecutionMode == ExecutionModeScheduled {
		if def.CronExpression == "" {
			return fmt.Errorf("cron expression is required for scheduled pipelines")
		}
		if err := validateCronExpression(def.CronExpression); err != nil {
			return fmt.Errorf("invalid cron expression: %w", err)
		}
	}

	// Validate status
	if def.Status != PipelineStatusActive && def.Status != PipelineStatusInactive {
		return fmt.Errorf("status must be 'active' or 'inactive', got: %s", def.Status)
	}

	// Validate components
	if len(def.Components) == 0 {
		return fmt.Errorf("pipeline must have at least one component")
	}

	for i, comp := range def.Components {
		if err := ValidateComponentConfig(comp); err != nil {
			return fmt.Errorf("component[%d] validation failed: %w", i, err)
		}
	}

	// Validate graph structure
	if err := ValidateGraph(def); err != nil {
		return fmt.Errorf("graph validation failed: %w", err)
	}

	return nil
}

// validateCronExpression validates a cron expression using the cron library
func validateCronExpression(expr string) error {
	if expr == "" {
		return fmt.Errorf("cron expression cannot be empty")
	}

	// Use the cron parser to validate the expression
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	_, err := parser.Parse(expr)
	if err != nil {
		return fmt.Errorf("invalid cron expression format: %w", err)
	}

	return nil
}

// ValidateComponentConfig validates a component configuration based on its type
func ValidateComponentConfig(config ComponentConfig) error {
	if config.ID == "" {
		return fmt.Errorf("component ID is required")
	}

	if config.Type == "" {
		return fmt.Errorf("component type is required")
	}

	// Validate based on component type
	switch config.Type {
	case ComponentTypeHTTPGet:
		return validateHTTPGetConfig(config)
	case ComponentTypeHTTPPost:
		return validateHTTPPostConfig(config)
	case ComponentTypeSQLQuery:
		return validateSQLQueryConfig(config)
	case ComponentTypeCSVReader:
		return validateCSVReaderConfig(config)
	case ComponentTypeKafkaConsumer:
		return validateKafkaConsumerConfig(config)
	case ComponentTypeKafkaProducer:
		return validateKafkaProducerConfig(config)
	case ComponentTypeRabbitMQConsumer:
		return validateRabbitMQConsumerConfig(config)
	case ComponentTypeRabbitMQProducer:
		return validateRabbitMQProducerConfig(config)
	case ComponentTypeHL7Reader:
		return validateHL7ReaderConfig(config)
	case ComponentTypeTCPRead:
		return validateTCPReadConfig(config)
	case ComponentTypeTCPWrite:
		return validateTCPWriteConfig(config)
	case ComponentTypePythonCodeBlock:
		return validatePythonCodeBlockConfig(config)
	case ComponentTypeLog:
		return validateLogConfig(config)
	case ComponentTypeLogSink:
		return validateLogSinkConfig(config)
	case ComponentTypeAttributeUpdate:
		return validateAttributeUpdateConfig(config)
	case ComponentTypeJSONExtractor:
		return nil // validated at construction time
	case ComponentTypeJSONTransform:
		return nil // validated at construction time
	case ComponentTypePrintLog:
		return validatePrintLogConfig(config)
	case ComponentTypeLychgateResponse:
		return nil // validated at construction time
	case ComponentTypeS3Reader, ComponentTypeS3Writer:
		return nil // validated at construction time
	case ComponentTypeMinIOReader, ComponentTypeMinIOWriter:
		return nil // validated at construction time
	case ComponentTypeAzureBlobReader, ComponentTypeAzureBlobWriter:
		return nil // validated at construction time
	case ComponentTypeLocalStorageReader, ComponentTypeLocalStorageWriter:
		return nil // validated at construction time
	default:
		return fmt.Errorf("unknown component type: %s", config.Type)
	}
}

// validateHTTPGetConfig validates HTTP GET component configuration
func validateHTTPGetConfig(config ComponentConfig) error {
	urlParam, ok := config.Parameters["url"]
	if !ok {
		return fmt.Errorf("HTTP_GET_Component requires 'url' parameter")
	}

	urlStr, ok := urlParam.(string)
	if !ok {
		return fmt.Errorf("HTTP_GET_Component 'url' must be a string")
	}

	if err := validateURL(urlStr); err != nil {
		return fmt.Errorf("HTTP_GET_Component has invalid URL: %w", err)
	}

	return nil
}

// validateHTTPPostConfig validates HTTP POST component configuration
func validateHTTPPostConfig(config ComponentConfig) error {
	urlParam, ok := config.Parameters["url"]
	if !ok {
		return fmt.Errorf("HTTP_POST_Component requires 'url' parameter")
	}

	urlStr, ok := urlParam.(string)
	if !ok {
		return fmt.Errorf("HTTP_POST_Component 'url' must be a string")
	}

	if err := validateURL(urlStr); err != nil {
		return fmt.Errorf("HTTP_POST_Component has invalid URL: %w", err)
	}

	contentTypeParam, ok := config.Parameters["content_type"]
	if !ok {
		return fmt.Errorf("HTTP_POST_Component requires 'content_type' parameter")
	}

	contentType, ok := contentTypeParam.(string)
	if !ok {
		return fmt.Errorf("HTTP_POST_Component 'content_type' must be a string")
	}

	if contentType == "" {
		return fmt.Errorf("HTTP_POST_Component 'content_type' cannot be empty")
	}

	return nil
}

// validateSQLQueryConfig validates SQL Query component configuration
func validateSQLQueryConfig(config ComponentConfig) error {
	connStrParam, ok := config.Parameters["connection_string"]
	if !ok {
		return fmt.Errorf("SQL_Query_Component requires 'connection_string' parameter")
	}

	connStr, ok := connStrParam.(string)
	if !ok {
		return fmt.Errorf("SQL_Query_Component 'connection_string' must be a string")
	}

	if connStr == "" {
		return fmt.Errorf("SQL_Query_Component 'connection_string' cannot be empty")
	}

	queryParam, ok := config.Parameters["query"]
	if !ok {
		return fmt.Errorf("SQL_Query_Component requires 'query' parameter")
	}

	query, ok := queryParam.(string)
	if !ok {
		return fmt.Errorf("SQL_Query_Component 'query' must be a string")
	}

	if query == "" {
		return fmt.Errorf("SQL_Query_Component 'query' cannot be empty")
	}

	return nil
}

// validateCSVReaderConfig validates CSV Reader component configuration
func validateCSVReaderConfig(config ComponentConfig) error {
	filePathParam, ok := config.Parameters["file_path"]
	if !ok {
		return fmt.Errorf("CSV_Reader_Component requires 'file_path' parameter")
	}

	filePath, ok := filePathParam.(string)
	if !ok {
		return fmt.Errorf("CSV_Reader_Component 'file_path' must be a string")
	}

	if filePath == "" {
		return fmt.Errorf("CSV_Reader_Component 'file_path' cannot be empty")
	}

	return nil
}

// validateKafkaConsumerConfig validates Kafka Consumer component configuration
func validateKafkaConsumerConfig(config ComponentConfig) error {
	brokersParam, ok := config.Parameters["brokers"]
	if !ok {
		return fmt.Errorf("Kafka_Consumer_Component requires 'brokers' parameter")
	}

	if err := validateBrokerAddresses(brokersParam); err != nil {
		return fmt.Errorf("Kafka_Consumer_Component has invalid brokers: %w", err)
	}

	topicParam, ok := config.Parameters["topic"]
	if !ok {
		return fmt.Errorf("Kafka_Consumer_Component requires 'topic' parameter")
	}

	topic, ok := topicParam.(string)
	if !ok {
		return fmt.Errorf("Kafka_Consumer_Component 'topic' must be a string")
	}

	if topic == "" {
		return fmt.Errorf("Kafka_Consumer_Component 'topic' cannot be empty")
	}

	return nil
}

// validateKafkaProducerConfig validates Kafka Producer component configuration
func validateKafkaProducerConfig(config ComponentConfig) error {
	brokersParam, ok := config.Parameters["brokers"]
	if !ok {
		return fmt.Errorf("Kafka_Producer_Component requires 'brokers' parameter")
	}

	if err := validateBrokerAddresses(brokersParam); err != nil {
		return fmt.Errorf("Kafka_Producer_Component has invalid brokers: %w", err)
	}

	topicParam, ok := config.Parameters["topic"]
	if !ok {
		return fmt.Errorf("Kafka_Producer_Component requires 'topic' parameter")
	}

	topic, ok := topicParam.(string)
	if !ok {
		return fmt.Errorf("Kafka_Producer_Component 'topic' must be a string")
	}

	if topic == "" {
		return fmt.Errorf("Kafka_Producer_Component 'topic' cannot be empty")
	}

	return nil
}

// validateRabbitMQConsumerConfig validates RabbitMQ Consumer component configuration
func validateRabbitMQConsumerConfig(config ComponentConfig) error {
	connURLParam, ok := config.Parameters["connection_url"]
	if !ok {
		return fmt.Errorf("RabbitMQ_Consumer_Component requires 'connection_url' parameter")
	}

	connURL, ok := connURLParam.(string)
	if !ok {
		return fmt.Errorf("RabbitMQ_Consumer_Component 'connection_url' must be a string")
	}

	if err := validateRabbitMQURL(connURL); err != nil {
		return fmt.Errorf("RabbitMQ_Consumer_Component has invalid connection_url: %w", err)
	}

	queueParam, ok := config.Parameters["queue"]
	if !ok {
		return fmt.Errorf("RabbitMQ_Consumer_Component requires 'queue' parameter")
	}

	queue, ok := queueParam.(string)
	if !ok {
		return fmt.Errorf("RabbitMQ_Consumer_Component 'queue' must be a string")
	}

	if queue == "" {
		return fmt.Errorf("RabbitMQ_Consumer_Component 'queue' cannot be empty")
	}

	return nil
}

// validateRabbitMQProducerConfig validates RabbitMQ Producer component configuration
func validateRabbitMQProducerConfig(config ComponentConfig) error {
	connURLParam, ok := config.Parameters["connection_url"]
	if !ok {
		return fmt.Errorf("RabbitMQ_Producer_Component requires 'connection_url' parameter")
	}

	connURL, ok := connURLParam.(string)
	if !ok {
		return fmt.Errorf("RabbitMQ_Producer_Component 'connection_url' must be a string")
	}

	if err := validateRabbitMQURL(connURL); err != nil {
		return fmt.Errorf("RabbitMQ_Producer_Component has invalid connection_url: %w", err)
	}

	exchangeParam, ok := config.Parameters["exchange"]
	if !ok {
		return fmt.Errorf("RabbitMQ_Producer_Component requires 'exchange' parameter")
	}

	exchange, ok := exchangeParam.(string)
	if !ok {
		return fmt.Errorf("RabbitMQ_Producer_Component 'exchange' must be a string")
	}

	if exchange == "" {
		return fmt.Errorf("RabbitMQ_Producer_Component 'exchange' cannot be empty")
	}

	return nil
}

// validateHL7ReaderConfig validates HL7 Reader component configuration
func validateHL7ReaderConfig(config ComponentConfig) error {
	filePathParam, ok := config.Parameters["file_path"]
	if !ok {
		return fmt.Errorf("HL7_Reader_Component requires 'file_path' parameter")
	}

	filePath, ok := filePathParam.(string)
	if !ok {
		return fmt.Errorf("HL7_Reader_Component 'file_path' must be a string")
	}

	if filePath == "" {
		return fmt.Errorf("HL7_Reader_Component 'file_path' cannot be empty")
	}

	return nil
}

// validateTCPReadConfig validates TCP Read component configuration
func validateTCPReadConfig(config ComponentConfig) error {
	hostParam, ok := config.Parameters["host"]
	if !ok {
		return fmt.Errorf("TCP_Read_Component requires 'host' parameter")
	}

	host, ok := hostParam.(string)
	if !ok {
		return fmt.Errorf("TCP_Read_Component 'host' must be a string")
	}

	if host == "" {
		return fmt.Errorf("TCP_Read_Component 'host' cannot be empty")
	}

	portParam, ok := config.Parameters["port"]
	if !ok {
		return fmt.Errorf("TCP_Read_Component requires 'port' parameter")
	}

	if err := validatePort(portParam); err != nil {
		return fmt.Errorf("TCP_Read_Component has invalid port: %w", err)
	}

	modeParam, ok := config.Parameters["mode"]
	if !ok {
		return fmt.Errorf("TCP_Read_Component requires 'mode' parameter")
	}

	mode, ok := modeParam.(string)
	if !ok {
		return fmt.Errorf("TCP_Read_Component 'mode' must be a string")
	}

	if mode != "server" && mode != "client" {
		return fmt.Errorf("TCP_Read_Component 'mode' must be either 'server' or 'client', got: %s", mode)
	}

	return nil
}

// validateTCPWriteConfig validates TCP Write component configuration
func validateTCPWriteConfig(config ComponentConfig) error {
	hostParam, ok := config.Parameters["host"]
	if !ok {
		return fmt.Errorf("TCP_Write_Component requires 'host' parameter")
	}

	host, ok := hostParam.(string)
	if !ok {
		return fmt.Errorf("TCP_Write_Component 'host' must be a string")
	}

	if host == "" {
		return fmt.Errorf("TCP_Write_Component 'host' cannot be empty")
	}

	portParam, ok := config.Parameters["port"]
	if !ok {
		return fmt.Errorf("TCP_Write_Component requires 'port' parameter")
	}

	if err := validatePort(portParam); err != nil {
		return fmt.Errorf("TCP_Write_Component has invalid port: %w", err)
	}

	modeParam, ok := config.Parameters["mode"]
	if !ok {
		return fmt.Errorf("TCP_Write_Component requires 'mode' parameter")
	}

	mode, ok := modeParam.(string)
	if !ok {
		return fmt.Errorf("TCP_Write_Component 'mode' must be a string")
	}

	if mode != "server" && mode != "client" {
		return fmt.Errorf("TCP_Write_Component 'mode' must be either 'server' or 'client', got: %s", mode)
	}

	return nil
}

// validatePythonCodeBlockConfig validates Python Code Block component configuration
func validatePythonCodeBlockConfig(config ComponentConfig) error {
	codeParam, ok := config.Parameters["code"]
	if !ok {
		return fmt.Errorf("Python_Code_Block_Component requires 'code' parameter")
	}

	code, ok := codeParam.(string)
	if !ok {
		return fmt.Errorf("Python_Code_Block_Component 'code' must be a string")
	}

	if strings.TrimSpace(code) == "" {
		return fmt.Errorf("Python_Code_Block_Component 'code' cannot be empty")
	}

	return nil
}

// validateLogConfig validates Log component configuration
func validateLogConfig(config ComponentConfig) error {
	logLevelParam, ok := config.Parameters["log_level"]
	if !ok {
		return fmt.Errorf("Log_Component requires 'log_level' parameter")
	}

	logLevel, ok := logLevelParam.(string)
	if !ok {
		return fmt.Errorf("Log_Component 'log_level' must be a string")
	}

	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLevels[logLevel] {
		return fmt.Errorf("Log_Component 'log_level' must be one of: debug, info, warn, error; got: %s", logLevel)
	}

	return nil
}

// Helper validation functions

// validateURL validates that a string is a valid URL
func validateURL(urlStr string) error {
	if urlStr == "" {
		return fmt.Errorf("URL cannot be empty")
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	if parsedURL.Scheme == "" {
		return fmt.Errorf("URL must include a scheme (e.g., http, https)")
	}

	if parsedURL.Host == "" {
		return fmt.Errorf("URL must include a host")
	}

	return nil
}

// validateRabbitMQURL validates a RabbitMQ connection URL
func validateRabbitMQURL(urlStr string) error {
	if urlStr == "" {
		return fmt.Errorf("connection URL cannot be empty")
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("invalid connection URL format: %w", err)
	}

	if parsedURL.Scheme != "amqp" && parsedURL.Scheme != "amqps" {
		return fmt.Errorf("connection URL must use amqp or amqps scheme, got: %s", parsedURL.Scheme)
	}

	if parsedURL.Host == "" {
		return fmt.Errorf("connection URL must include a host")
	}

	return nil
}

// validateBrokerAddresses validates Kafka broker addresses
func validateBrokerAddresses(brokersParam interface{}) error {
	// Handle both []string and []interface{} types
	switch brokers := brokersParam.(type) {
	case []string:
		if len(brokers) == 0 {
			return fmt.Errorf("brokers list cannot be empty")
		}
		for i, broker := range brokers {
			if err := validateHostPort(broker); err != nil {
				return fmt.Errorf("broker[%d] is invalid: %w", i, err)
			}
		}
	case []interface{}:
		if len(brokers) == 0 {
			return fmt.Errorf("brokers list cannot be empty")
		}
		for i, broker := range brokers {
			brokerStr, ok := broker.(string)
			if !ok {
				return fmt.Errorf("broker[%d] must be a string", i)
			}
			if err := validateHostPort(brokerStr); err != nil {
				return fmt.Errorf("broker[%d] is invalid: %w", i, err)
			}
		}
	default:
		return fmt.Errorf("brokers must be a list of strings")
	}

	return nil
}

// validateHostPort validates a host:port string
func validateHostPort(hostPort string) error {
	if hostPort == "" {
		return fmt.Errorf("host:port cannot be empty")
	}

	// Simple regex to validate host:port format
	hostPortRegex := regexp.MustCompile(`^[a-zA-Z0-9.-]+:\d+$`)
	if !hostPortRegex.MatchString(hostPort) {
		return fmt.Errorf("invalid host:port format: %s (expected format: host:port)", hostPort)
	}

	// Extract and validate port
	parts := strings.Split(hostPort, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid host:port format: %s", hostPort)
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("invalid port number: %s", parts[1])
	}

	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got: %d", port)
	}

	return nil
}

// validatePort validates a port number
func validatePort(portParam interface{}) error {
	var port int

	switch p := portParam.(type) {
	case int:
		port = p
	case float64:
		port = int(p)
	case string:
		var err error
		port, err = strconv.Atoi(p)
		if err != nil {
			return fmt.Errorf("port must be a number, got: %s", p)
		}
	default:
		return fmt.Errorf("port must be a number")
	}

	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got: %d", port)
	}

	return nil
}

// validateLogSinkConfig validates Log Sink component configuration
func validateLogSinkConfig(config ComponentConfig) error {
	filePathParam, ok := config.Parameters["file_path"]
	if !ok {
		return fmt.Errorf("Log_Sink_Component requires 'file_path' parameter")
	}

	filePath, ok := filePathParam.(string)
	if !ok {
		return fmt.Errorf("Log_Sink_Component 'file_path' must be a string")
	}

	// Allow template variables for file_path
	if !isTemplateVariable(filePath) && filePath == "" {
		return fmt.Errorf("Log_Sink_Component 'file_path' cannot be empty")
	}

	logLevelParam, ok := config.Parameters["log_level"]
	if !ok {
		return fmt.Errorf("Log_Sink_Component requires 'log_level' parameter")
	}

	logLevel, ok := logLevelParam.(string)
	if !ok {
		return fmt.Errorf("Log_Sink_Component 'log_level' must be a string")
	}

	// Allow template variables (e.g., {{variable_name}})
	if isTemplateVariable(logLevel) {
		return nil
	}

	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLevels[logLevel] {
		return fmt.Errorf("Log_Sink_Component 'log_level' must be one of: debug, info, warn, error; got: %s", logLevel)
	}

	return nil
}

// isTemplateVariable checks if a string is a template variable (e.g., {{variable_name}})
func isTemplateVariable(s string) bool {
	return strings.HasPrefix(s, "{{") && strings.HasSuffix(s, "}}")
}

// validateAttributeUpdateConfig validates Attribute Update component configuration
func validateAttributeUpdateConfig(config ComponentConfig) error {
	mappingsParam, ok := config.Parameters["mappings"]
	if !ok {
		return fmt.Errorf("Attribute_Update_Component requires 'mappings' parameter")
	}

	mappings, ok := mappingsParam.([]interface{})
	if !ok {
		return fmt.Errorf("Attribute_Update_Component 'mappings' must be an array")
	}

	if len(mappings) == 0 {
		return fmt.Errorf("Attribute_Update_Component 'mappings' must have at least one entry")
	}

	for i, item := range mappings {
		m, ok := item.(map[string]interface{})
		if !ok {
			return fmt.Errorf("Attribute_Update_Component mapping[%d] must be an object", i)
		}
		name, _ := m["name"].(string)
		expression, _ := m["expression"].(string)
		if name == "" {
			return fmt.Errorf("Attribute_Update_Component mapping[%d] 'name' is required", i)
		}
		if expression == "" {
			return fmt.Errorf("Attribute_Update_Component mapping[%d] 'expression' is required", i)
		}
	}

	return nil
}

// validatePrintLogConfig validates Print Log component configuration
func validatePrintLogConfig(config ComponentConfig) error {
	if ll, ok := config.Parameters["log_level"].(string); ok && ll != "" {
		validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
		if !validLevels[ll] {
			return fmt.Errorf("Print_Log_Component 'log_level' must be one of: debug, info, warn, error; got: %s", ll)
		}
	}
	return nil
}
