package lychgate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
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
	requestJson, err := json.Marshal(data.Payload)
	if err != nil {
		fmt.Printf("Error occurred converting the Payload to json: %v\n", err)
		return data
	}

	requestPayload := NewRequestPayload(l.systemID, l.entityID, string(requestJson), l.schemaClass)
	l.sendToLychgate(requestPayload)
	return data
}

func (l *LychgateResponseComponent) sendToLychgate(requestPayload *RequestPayload) {
	category := strings.ToUpper(l.communicationMode.String())
	if category == "MQTT" {
		category = "NEBULASTREAMER"
	}
	appConfigs, err := l.GetByCategory(category)
	if err != nil {
		fmt.Printf("Error fetching app configs: %v\n", err)
		return
	}

	// Build a key→value lookup from the app_configs rows
	cfgMap := make(map[string]string, len(appConfigs))
	for _, c := range appConfigs {
		cfgMap[c.Key] = c.Value
	}

	switch l.communicationMode {
	case CommunicationModeRabbitMQ:
		l.publishToRabbitMQ(cfgMap, requestPayload)
	case CommunicationModeKafka:
		l.publishToKafka(cfgMap, requestPayload)
	case CommunicationModeMQTT:
		l.publishToMQTT(cfgMap, requestPayload)
	}
}

// publishToRabbitMQ connects using the app_configs values and publishes the
// serialised RequestPayload to a topic exchange with schemaClass as the routing key.
func (l *LychgateResponseComponent) publishToRabbitMQ(cfgMap map[string]string, requestPayload *RequestPayload) {
	host := cfgMap["Host"]
	port := cfgMap["Port"]
	username := cfgMap["Username"]
	password := cfgMap["Password"]
	virtualHost := cfgMap["VirtualHost"]

	if host == "" || port == "" || username == "" || password == "" {
		fmt.Println("RabbitMQ config incomplete: Host, Port, Username, Password are required")
		return
	}

	// Build AMQP connection URL: amqp://user:pass@host:port/vhost
	vhost := ""
	if virtualHost != "" {
		vhost = "/" + virtualHost
	}
	connURL := fmt.Sprintf("amqp://%s:%s@%s:%s%s", username, password, host, port, vhost)

	conn, err := amqp.Dial(connURL)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %v\n", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open RabbitMQ channel: %v\n", err)
		return
	}
	defer ch.Close()

	// Declare a topic exchange using the schemaClass
	exchange := "nebula.exchange"
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		fmt.Printf("Failed to declare exchange %q: %v\n", exchange, err)
		return
	}

	// Serialise the payload
	body, err := json.Marshal(requestPayload)
	if err != nil {
		fmt.Printf("Failed to marshal RequestPayload: %v\n", err)
		return
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
		fmt.Printf("Failed to publish message to RabbitMQ: %v\n", err)
		return
	}

	fmt.Printf("Published message to RabbitMQ exchange=%s routingKey=%s (%d bytes)\n",
		exchange, l.schemaClass, len(body))
}

// publishToKafka connects using the app_configs values and writes the serialised
// RequestPayload to the configured Kafka topic(s).
// Expected config keys: SERVERS, TOPICS, GROUP (optional), PARTITIONS (optional).
func (l *LychgateResponseComponent) publishToKafka(cfgMap map[string]string, requestPayload *RequestPayload) {
	servers := cfgMap["SERVERS"]

	if servers == "" {
		fmt.Println("Kafka config incomplete: SERVERS and TOPICS are required")
		return
	}

	// SERVERS and TOPICS can be comma-separated
	brokers := strings.Split(servers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	topicList := strings.Split(topic, ",")
	for i := range topicList {
		topicList[i] = strings.TrimSpace(topicList[i])
	}

	// Serialise the payload
	body, err := json.Marshal(requestPayload)
	if err != nil {
		fmt.Printf("Failed to marshal RequestPayload for Kafka: %v\n", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish to each configured topic
	for _, topic := range topicList {
		writer := &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
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
			fmt.Printf("Failed to publish message to Kafka topic=%s: %v\n", topic, err)
			continue
		}

		fmt.Printf("Published message to Kafka topic=%s key=%s (%d bytes)\n",
			topic, l.schemaClass, len(body))
	}
}

// publishToMQTT connects using the app_configs values (category NEBULASTREAMER)
// and publishes the serialised RequestPayload to the schemaClass topic.
// Expected config keys: HOST_VAL, USER_NAME, PASS.
func (l *LychgateResponseComponent) publishToMQTT(cfgMap map[string]string, requestPayload *RequestPayload) {
	broker := cfgMap["HOST_VAL"]
	username := cfgMap["USER_NAME"]
	password := cfgMap["PASS"]

	if broker == "" {
		fmt.Println("MQTT config incomplete: HOST_VAL is required")
		return
	}

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
		fmt.Printf("MQTT connection timed out to %s\n", broker)
		return
	}
	if token.Error() != nil {
		fmt.Printf("Failed to connect to MQTT broker %s: %v\n", broker, token.Error())
		return
	}
	defer client.Disconnect(250)

	body, err := json.Marshal(requestPayload)
	if err != nil {
		fmt.Printf("Failed to marshal RequestPayload for MQTT: %v\n", err)
		return
	}

	pubToken := client.Publish(topic, 1, false, body)
	if !pubToken.WaitTimeout(10 * time.Second) {
		fmt.Printf("MQTT publish timed out on topic=%s\n", topic)
		return
	}
	if pubToken.Error() != nil {
		fmt.Printf("Failed to publish message to MQTT topic=%s: %v\n", topic, pubToken.Error())
		return
	}

	fmt.Printf("Published message to MQTT broker=%s topic=%s (%d bytes)\n",
		broker, topic, len(body))
}

// Execute consumes each incoming Data item via produceRequest and sends it to Lychgate.
// As a sink component, it does not forward data downstream.
func (l *LychgateResponseComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					return
				}
				l.produceRequest(data)
			}
		}
	}()

	return nil, nil
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
