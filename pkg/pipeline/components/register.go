package components

import (
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// RegisterComponents registers all available component types with the factory
func RegisterComponents(factory pipeline.ComponentFactory) {
	// Source components
	factory.Register(pipeline.ComponentTypeHTTPGet, NewHTTPGetComponent)
	factory.Register(pipeline.ComponentTypeSQLQuery, NewSQLQueryComponent)
	factory.Register(pipeline.ComponentTypeCSVReader, NewCSVReaderComponent)
	factory.Register(pipeline.ComponentTypeTCPRead, NewTCPReadComponent)
	factory.Register(pipeline.ComponentTypeKafkaConsumer, NewKafkaConsumerComponent)
	factory.Register(pipeline.ComponentTypeRabbitMQConsumer, NewRabbitMQConsumerComponent)
	factory.Register(pipeline.ComponentTypeHL7Reader, NewHL7ReaderComponent)

	// Processor components
	factory.Register(pipeline.ComponentTypeLog, NewLogComponent)
	factory.Register(pipeline.ComponentTypePythonCodeBlock, NewPythonCodeBlockComponent)
	factory.Register(pipeline.ComponentTypeAttributeUpdate, NewAttributeUpdateComponent)

	// Sink components
	factory.Register(pipeline.ComponentTypeHTTPPost, NewHTTPPostComponent)
	factory.Register(pipeline.ComponentTypeTCPWrite, NewTCPWriteComponent)
	factory.Register(pipeline.ComponentTypeKafkaProducer, NewKafkaProducerComponent)
	factory.Register(pipeline.ComponentTypeRabbitMQProducer, NewRabbitMQProducerComponent)
	factory.Register(pipeline.ComponentTypeLogSink, NewLogSinkComponent)
}
