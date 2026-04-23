package lychgate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/Xecutables/Nebula.Conduit/pkg/s3bucket"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/mattn/go-sqlite3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

// CommunicationMode represents the supported messaging protocols for Lychgate responses.
type CommunicationMode int

const topic = "nebula.connector.lychgate.response"
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

// LychgateResponseComponent is a sink component that sends pipeline data
// to Lychgate via RabbitMQ, Kafka, or MQTT, enriched with routing metadata.
type LychgateResponseComponent struct {
	config            pipeline.ComponentConfig
	systemID          int
	entityID          int
	schemaClass       string
	communicationMode CommunicationMode
	db                *sql.DB
	logStore          pipeline.LogStore
	pipelineID        string
	executionID       string
}

// SetLogStore injects the LogStore dependency.
func (l *LychgateResponseComponent) SetLogStore(store pipeline.LogStore) {
	l.logStore = store
}

// SetPipelineContext injects pipeline and execution IDs.
func (l *LychgateResponseComponent) SetPipelineContext(pipelineID, executionID string) {
	l.pipelineID = pipelineID
	l.executionID = executionID
}

// log writes a structured log entry to the database and stdout.
func (l *LychgateResponseComponent) log(level, message string) {
	fmt.Printf("Lychgate[%s]: %s\n", l.config.ID, message)
	if l.logStore != nil {
		entry := pipeline.PipelineLogEntry{
			PipelineID:  l.pipelineID,
			ExecutionID: l.executionID,
			ComponentID: l.config.ID,
			LogLevel:    level,
			Message:     message,
		}
		_ = l.logStore.InsertLog(context.Background(), entry)
	}
}

// logWithPayload writes a log entry that includes payload data.
func (l *LychgateResponseComponent) logWithPayload(level, message string, payload string) {
	fmt.Printf("Lychgate[%s]: %s\n", l.config.ID, message)
	if l.logStore != nil {
		entry := pipeline.PipelineLogEntry{
			PipelineID:  l.pipelineID,
			ExecutionID: l.executionID,
			ComponentID: l.config.ID,
			LogLevel:    level,
			Message:     message,
			Payload:     payload,
		}
		_ = l.logStore.InsertLog(context.Background(), entry)
	}
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

	schemaClass, err := pipeline.ParamString(config.Parameters, "schema_class", "")
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
	const dbPath = "./vault.db"
	if _, statErr := os.Stat(dbPath); os.IsNotExist(statErr) {
		s3svc, err := s3bucket.NewS3BucketService(nil)
		if err != nil {
			return nil, fmt.Errorf("lychgate_response: failed to initialize S3 bucket service: %w", err)
		}
		if err := s3svc.DownloadFile("vault.db", dbPath); err != nil {
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

func payloadRecords(payload interface{}) ([]interface{}, string, bool) {
	if payload == nil {
		return []interface{}{payload}, "payload", false
	}

	// If the payload is raw bytes or a string, parse it into a generic JSON value first.
	// This is the common case when upstream components (e.g. json_transform) marshal
	// their output back to []byte before forwarding.
	parsed := payload
	switch v := payload.(type) {
	case []byte:
		var decoded interface{}
		if err := json.Unmarshal(v, &decoded); err == nil {
			parsed = decoded
		}
	case string:
		var decoded interface{}
		if err := json.Unmarshal([]byte(v), &decoded); err == nil {
			parsed = decoded
		}
	}

	if items, ok := interfaceSlice(parsed); ok {
		return items, "payload", true
	}

	if payloadMap, ok := parsed.(map[string]interface{}); ok {
		if nested, exists := payloadMap["data"]; exists {
			if items, ok := interfaceSlice(nested); ok {
				return items, "payload.data", true
			}
		}
	}

	return []interface{}{parsed}, "payload", false
}

func interfaceSlice(value interface{}) ([]interface{}, bool) {
	if value == nil {
		return nil, false
	}

	switch v := value.(type) {
	case []interface{}:
		return v, true
	case []byte:
		return nil, false
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}

	items := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		items[i] = rv.Index(i).Interface()
	}
	return items, true
}

// produceRequest enriches the given Data with Lychgate routing metadata and sends
// the resulting payload to Lychgate using the configured transport.
func (l *LychgateResponseComponent) produceRequest(data pipeline.Data) error {
	if data.Metadata == nil {
		data.Metadata = make(map[string]string)
	}
	data.Metadata["lychgate_system_id"] = fmt.Sprintf("%d", l.systemID)
	data.Metadata["lychgate_entity_id"] = fmt.Sprintf("%d", l.entityID)
	data.Metadata["lychgate_schema_class"] = l.schemaClass
	data.Metadata["lychgate_communication_mode"] = l.communicationMode.String()

	l.log("info", fmt.Sprintf("Data received — enriching with routing metadata (system_id=%d, entity_id=%d, mode=%s)",
		l.systemID, l.entityID, l.communicationMode.String()))
	l.log("debug", fmt.Sprintf("Execute payload inspection started (trace=%s, payload_type=%T)", data.TraceID, data.Payload))

	records, source, split := payloadRecords(data.Payload)
	if split {
		l.log("info", fmt.Sprintf("Detected %d record(s) in %s — producing one Lychgate message per record", len(records), source))
	} else {
		l.log("info", fmt.Sprintf("Detected non-array %s — producing a single Lychgate message", source))
	}

	var schemaClassPtr *string
	if l.schemaClass != "" {
		schemaClassPtr = &l.schemaClass
	}

	var publishErrs []string
	for i, record := range records {
		l.log("debug", fmt.Sprintf("Preparing record %d/%d from %s (record_type=%T)", i+1, len(records), source, record))

		requestJson, err := json.Marshal(record)
		if err != nil {
			publishErrs = append(publishErrs, fmt.Sprintf("record %d: marshal failed: %v", i+1, err))
			l.log("error", fmt.Sprintf("Failed to marshal record %d/%d from %s: %v", i+1, len(records), source, err))
			continue
		}

		l.log("debug", fmt.Sprintf("Record %d/%d serialized (%d bytes)", i+1, len(records), len(requestJson)))
		l.logWithPayload("debug", fmt.Sprintf("Record %d/%d requestJson content", i+1, len(records)), string(requestJson))

		requestPayload := NewRequestPayload(l.systemID, l.entityID, string(requestJson), schemaClassPtr)

		l.log("info", fmt.Sprintf("Record %d/%d — sending to %s (system_id=%d, entity_id=%d, topic=%s, message_id=%s)",
			i+1, len(records), l.communicationMode.String(), l.systemID, l.entityID, topic, *requestPayload.SystemRequests.MessageID))

		if err := l.sendToLychgate(requestPayload); err != nil {
			publishErrs = append(publishErrs, fmt.Sprintf("record %d: %v", i+1, err))
			l.log("error", fmt.Sprintf("Failed to publish record %d/%d from %s: %v", i+1, len(records), source, err))
			continue
		}

		l.log("info", fmt.Sprintf("Record %d/%d from %s published successfully to topic=%s via %s",
			i+1, len(records), source, topic, l.communicationMode.String()))
	}

	if len(publishErrs) > 0 {
		return fmt.Errorf("failed to publish %d of %d record(s): %s", len(publishErrs), len(records), strings.Join(publishErrs, "; "))
	}

	l.log("info", fmt.Sprintf("Completed publishing %d record(s) for trace=%s", len(records), data.TraceID))
	return nil
}

func (l *LychgateResponseComponent) sendToLychgate(requestPayload *RequestPayload) error {
	category := strings.ToUpper(l.communicationMode.String())
	if category == "MQTT" {
		category = "NEBULASTREAMER"
	}

	l.log("debug", fmt.Sprintf("Fetching broker config from vault.db (category=%s)", category))

	appConfigs, err := l.GetByCategory(category)
	if err != nil {
		return fmt.Errorf("failed to fetch app configs for category %q: %w", category, err)
	}

	cfgMap := make(map[string]string, len(appConfigs))
	for _, c := range appConfigs {
		cfgMap[c.Key] = c.Value
	}

	l.log("debug", fmt.Sprintf("Broker config loaded (%d keys)", len(cfgMap)))

	switch l.communicationMode {
	case CommunicationModeRabbitMQ:
		return l.publishToRabbitMQ(cfgMap, requestPayload)
	case CommunicationModeKafka:
		return l.publishToKafka(cfgMap, requestPayload)
	case CommunicationModeMQTT:
		return l.publishToMQTT(cfgMap, requestPayload)
	default:
		return fmt.Errorf("unsupported communication mode: %s", l.communicationMode.String())
	}

}

// publishToRabbitMQ connects using the app_configs values and publishes the
// serialised RequestPayload to a topic exchange with the configured routing key.
// It also ensures the target queue exists and is bound to the exchange.
func (l *LychgateResponseComponent) publishToRabbitMQ(cfgMap map[string]string, requestPayload *RequestPayload) error {
	host := cfgMap["Host"]
	port := cfgMap["Port"]
	username := cfgMap["Username"]
	password := cfgMap["Password"]
	virtualHost := cfgMap["VirtualHost"]

	if host == "" || port == "" || username == "" || password == "" {
		return fmt.Errorf("rabbitmq config incomplete: Host, Port, Username, Password are required")
	}

	vhost := ""
	if virtualHost != "" {
		vhost = "/" + virtualHost
	}
	connURL := fmt.Sprintf("amqp://%s:%s@%s:%s%s", username, password, host, port, vhost)

	l.log("debug", fmt.Sprintf("Connecting to RabbitMQ at %s:%s (vhost=%s)", host, port, virtualHost))

	conn, err := amqp.Dial(connURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open RabbitMQ channel: %w", err)
	}
	defer ch.Close()

	exchange := "nebula.exchange"
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare exchange %q: %w", exchange, err)
	}

	// Ensure the queue exists and is bound to the exchange with the routing key.
	queueName := topic // use the topic constant as the queue name
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue %q: %w", queueName, err)
	}

	if err := ch.QueueBind(queueName, topic, exchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind queue %q to exchange %q with key %q: %w", queueName, exchange, topic, err)
	}

	l.log("debug", fmt.Sprintf("RabbitMQ connected — exchange=%s, queue=%s, routing_key=%s", exchange, queueName, topic))

	body, err := json.Marshal(requestPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal RequestPayload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
	}

	if err := ch.PublishWithContext(ctx, exchange, topic, false, false, msg); err != nil {
		return fmt.Errorf("failed to publish to RabbitMQ: %w", err)
	}

	l.logWithPayload("info",
		fmt.Sprintf("Published to RabbitMQ — exchange=%s, queue=%s, routing_key=%s (%d bytes)", exchange, queueName, topic, len(body)),
		string(body))
	return nil
}

// publishToKafka connects using the app_configs values and writes the serialised
// RequestPayload to the configured Kafka topic(s).
// Expected config keys: SERVERS, TOPICS, GROUP (optional), PARTITIONS (optional).
func (l *LychgateResponseComponent) publishToKafka(cfgMap map[string]string, requestPayload *RequestPayload) error {
	servers := cfgMap["SERVERS"]

	if servers == "" {
		return fmt.Errorf("kafka config incomplete: SERVERS is required")
	}

	brokers := strings.Split(servers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	topicList := strings.Split(topic, ",")
	for i := range topicList {
		topicList[i] = strings.TrimSpace(topicList[i])
	}

	body, err := json.Marshal(requestPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal RequestPayload for Kafka: %w", err)
	}

	l.log("debug", fmt.Sprintf("Connecting to Kafka brokers=%v, topics=%v", brokers, topicList))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var publishErrs []string
	for _, t := range topicList {
		writer := &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        t,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
		}

		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(l.schemaClass),
			Value: body,
			Time:  time.Now(),
		})
		writer.Close()

		if err != nil {
			publishErrs = append(publishErrs, fmt.Sprintf("topic=%s: %v", t, err))
			continue
		}

		l.logWithPayload("info",
			fmt.Sprintf("Published to Kafka — topic=%s, key=%s (%d bytes)", t, l.schemaClass, len(body)),
			string(body))
	}

	if len(publishErrs) > 0 {
		return fmt.Errorf("failed to publish to Kafka: %s", strings.Join(publishErrs, "; "))
	}

	return nil
}

// publishToMQTT connects using the app_configs values (category NEBULASTREAMER)
// and publishes the serialised RequestPayload to the schemaClass topic.
// Expected config keys: HOST_VAL, USER_NAME, PASS.
func (l *LychgateResponseComponent) publishToMQTT(cfgMap map[string]string, requestPayload *RequestPayload) error {
	broker := cfgMap["HOST_VAL"]
	username := cfgMap["USER_NAME"]
	password := cfgMap["PASS"]

	if broker == "" {
		return fmt.Errorf("mqtt config incomplete: HOST_VAL is required")
	}

	l.log("debug", fmt.Sprintf("Connecting to MQTT broker=%s", broker))

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(fmt.Sprintf("nebula-conduit-%s", l.config.ID)).
		SetConnectTimeout(10 * time.Second)

	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("mqtt connection timed out to %s", broker)
	}
	if token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker %s: %w", broker, token.Error())
	}
	defer client.Disconnect(250)

	l.log("debug", "MQTT connected successfully")

	body, err := json.Marshal(requestPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal RequestPayload for MQTT: %w", err)
	}

	pubToken := client.Publish(topic, 1, false, body)
	if !pubToken.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("mqtt publish timed out on topic=%s", topic)
	}
	if pubToken.Error() != nil {
		return fmt.Errorf("failed to publish to MQTT topic=%s: %w", topic, pubToken.Error())
	}

	l.logWithPayload("info",
		fmt.Sprintf("Published to MQTT — broker=%s, topic=%s (%d bytes)", broker, topic, len(body)),
		string(body))
	return nil
}

// Execute consumes each incoming Data item via produceRequest and sends it to Lychgate.
// As a sink component, it blocks until all input is consumed, then returns nil.
func (l *LychgateResponseComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	itemCount := 0
	for {
		select {
		case <-ctx.Done():
			l.log("warn", "Context cancelled — stopping consumption")
			return nil, ctx.Err()
		case data, ok := <-input:
			if !ok {
				l.log("info", fmt.Sprintf("Input channel closed — processed %d item(s)", itemCount))
				return nil, nil
			}

			itemCount++
			l.log("debug", fmt.Sprintf("Processing item #%d (trace=%s)", itemCount, data.TraceID))

			if err := l.produceRequest(data); err != nil {
				l.log("error", fmt.Sprintf("Failed to process item #%d (trace=%s): %v", itemCount, data.TraceID, err))
				if !l.config.ContinueOnError {
					return nil, err
				}
			}
		}
	}
}

// Validate checks that all required configuration is present and valid.
func (l *LychgateResponseComponent) Validate() error {
	if l.systemID == 0 {
		return fmt.Errorf("system_id is required and must be non-zero")
	}
	if l.entityID == 0 {
		return fmt.Errorf("entity_id is required and must be non-zero")
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

var _ pipeline.Component = (*LychgateResponseComponent)(nil)
var _ pipeline.LogStoreInjectable = (*LychgateResponseComponent)(nil)
