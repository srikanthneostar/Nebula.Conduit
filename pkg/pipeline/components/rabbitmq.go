package components

import (
	"context"
	"fmt"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConsumerComponent consumes messages from a RabbitMQ queue
type RabbitMQConsumerComponent struct {
	config        pipeline.ComponentConfig
	connectionURL string
	queue         string
	autoAck       bool
	prefetchCount int
}

// NewRabbitMQConsumerComponent creates a new RabbitMQ consumer component
func NewRabbitMQConsumerComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	connURL, ok := config.Parameters["connection_url"].(string)
	if !ok || connURL == "" {
		return nil, fmt.Errorf("connection_url parameter is required")
	}

	queue, ok := config.Parameters["queue"].(string)
	if !ok || queue == "" {
		return nil, fmt.Errorf("queue parameter is required")
	}

	autoAck := false
	if v, ok := config.Parameters["auto_ack"].(bool); ok {
		autoAck = v
	}

	prefetchCount := 10
	if v, ok := config.Parameters["prefetch_count"].(float64); ok {
		prefetchCount = int(v)
	}

	return &RabbitMQConsumerComponent{
		config:        config,
		connectionURL: connURL,
		queue:         queue,
		autoAck:       autoAck,
		prefetchCount: prefetchCount,
	}, nil
}

// Execute runs the RabbitMQ consumer component
func (r *RabbitMQConsumerComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	return r.Start(ctx)
}

// Start begins consuming messages from RabbitMQ
func (r *RabbitMQConsumerComponent) Start(ctx context.Context) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 100)

	conn, err := amqp.Dial(r.connectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.Qos(r.prefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	// Declare queue to ensure it exists
	_, err = ch.QueueDeclare(r.queue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	msgs, err := ch.Consume(r.queue, r.config.ID, r.autoAck, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to start consuming: %w", err)
	}

	go func() {
		defer close(output)
		defer ch.Close()
		defer conn.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}

				data := pipeline.Data{
					Payload:   msg.Body,
					Metadata:  make(map[string]string),
					Timestamp: msg.Timestamp,
					TraceID:   r.config.ID,
				}
				data.Metadata["routing_key"] = msg.RoutingKey
				data.Metadata["exchange"] = msg.Exchange
				data.Metadata["message_id"] = msg.MessageId
				data.Metadata["content_type"] = msg.ContentType

				if !r.autoAck {
					if err := msg.Ack(false); err != nil {
						if !r.config.ContinueOnError {
							return
						}
						continue
					}
				}

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

func (r *RabbitMQConsumerComponent) Validate() error {
	if r.connectionURL == "" {
		return fmt.Errorf("connection_url is required")
	}
	if r.queue == "" {
		return fmt.Errorf("queue is required")
	}
	return nil
}

func (r *RabbitMQConsumerComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeRabbitMQConsumer
}

func (r *RabbitMQConsumerComponent) ID() string { return r.config.ID }

func (r *RabbitMQConsumerComponent) Config() pipeline.ComponentConfig { return r.config }

// RabbitMQProducerComponent publishes messages to a RabbitMQ exchange
type RabbitMQProducerComponent struct {
	config        pipeline.ComponentConfig
	connectionURL string
	exchange      string
	routingKey    string
	mandatory     bool
	persistent    bool
	contentType   string
}

// NewRabbitMQProducerComponent creates a new RabbitMQ producer component
func NewRabbitMQProducerComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	connURL, ok := config.Parameters["connection_url"].(string)
	if !ok || connURL == "" {
		return nil, fmt.Errorf("connection_url parameter is required")
	}

	exchange, ok := config.Parameters["exchange"].(string)
	if !ok || exchange == "" {
		return nil, fmt.Errorf("exchange parameter is required")
	}

	routingKey := ""
	if v, ok := config.Parameters["routing_key"].(string); ok {
		routingKey = v
	}

	mandatory := false
	if v, ok := config.Parameters["mandatory"].(bool); ok {
		mandatory = v
	}

	persistent := true
	if v, ok := config.Parameters["persistent"].(bool); ok {
		persistent = v
	}

	contentType := "application/octet-stream"
	if v, ok := config.Parameters["content_type"].(string); ok && v != "" {
		contentType = v
	}

	return &RabbitMQProducerComponent{
		config:        config,
		connectionURL: connURL,
		exchange:      exchange,
		routingKey:    routingKey,
		mandatory:     mandatory,
		persistent:    persistent,
		contentType:   contentType,
	}, nil
}

// Execute runs the RabbitMQ producer component
func (r *RabbitMQProducerComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	return nil, r.Write(ctx, input)
}

// Write consumes data and publishes to RabbitMQ
func (r *RabbitMQProducerComponent) Write(ctx context.Context, input <-chan pipeline.Data) error {
	conn, err := amqp.Dial(r.connectionURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Declare exchange to ensure it exists
	if err := ch.ExchangeDeclare(r.exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	deliveryMode := amqp.Transient
	if r.persistent {
		deliveryMode = amqp.Persistent
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				return nil
			}

			var body []byte
			switch v := data.Payload.(type) {
			case []byte:
				body = v
			case string:
				body = []byte(v)
			default:
				body = []byte(fmt.Sprintf("%v", v))
			}

			routingKey := r.routingKey
			if rk, ok := data.Metadata["routing_key"]; ok && rk != "" {
				routingKey = rk
			}

			msg := amqp.Publishing{
				DeliveryMode: deliveryMode,
				Timestamp:    time.Now(),
				ContentType:  r.contentType,
				Body:         body,
			}

			if err := ch.PublishWithContext(ctx, r.exchange, routingKey, r.mandatory, false, msg); err != nil {
				if !r.config.ContinueOnError {
					return fmt.Errorf("failed to publish message: %w", err)
				}
			}
		}
	}
}

func (r *RabbitMQProducerComponent) Validate() error {
	if r.connectionURL == "" {
		return fmt.Errorf("connection_url is required")
	}
	if r.exchange == "" {
		return fmt.Errorf("exchange is required")
	}
	return nil
}

func (r *RabbitMQProducerComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeRabbitMQProducer
}

func (r *RabbitMQProducerComponent) ID() string { return r.config.ID }

func (r *RabbitMQProducerComponent) Config() pipeline.ComponentConfig { return r.config }
