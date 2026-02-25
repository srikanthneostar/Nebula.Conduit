package components

import (
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// RegisterComponents registers all available component types with the factory
func RegisterComponents(factory pipeline.ComponentFactory) {
	// Register source components
	factory.Register(pipeline.ComponentTypeHTTPGet, NewHTTPGetComponent)
	factory.Register(pipeline.ComponentTypeSQLQuery, NewSQLQueryComponent)
	factory.Register(pipeline.ComponentTypeCSVReader, NewCSVReaderComponent)
	factory.Register(pipeline.ComponentTypeTCPRead, NewTCPReadComponent)

	// Register processor components
	factory.Register(pipeline.ComponentTypeLog, NewLogComponent)
	factory.Register(pipeline.ComponentTypePythonCodeBlock, NewPythonCodeBlockComponent)

	// Register sink components
	factory.Register(pipeline.ComponentTypeHTTPPost, NewHTTPPostComponent)
	factory.Register(pipeline.ComponentTypeTCPWrite, NewTCPWriteComponent)

	// Additional components will be registered here as they are implemented
	// factory.Register(pipeline.ComponentTypeKafkaConsumer, NewKafkaConsumerComponent)
	// factory.Register(pipeline.ComponentTypeKafkaProducer, NewKafkaProducerComponent)
	// factory.Register(pipeline.ComponentTypeRabbitMQConsumer, NewRabbitMQConsumerComponent)
	// factory.Register(pipeline.ComponentTypeRabbitMQProducer, NewRabbitMQProducerComponent)
	// factory.Register(pipeline.ComponentTypeHL7Reader, NewHL7ReaderComponent)
}
