package pipeline

import (
	"testing"
)

func TestValidateGraph_ValidPipeline(t *testing.T) {
	// Valid pipeline: HTTPGet -> Log -> HTTPPost
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "processor-1", Type: ComponentTypeLog},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "processor-1"},
			{SourceComponentID: "processor-1", TargetComponentID: "sink-1"},
		},
	}

	err := ValidateGraph(def)
	if err != nil {
		t.Errorf("Expected valid pipeline to pass validation, got error: %v", err)
	}
}

func TestValidateGraph_MissingSource(t *testing.T) {
	// Invalid: all components have incoming connections, no topology source
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "proc-1", Type: ComponentTypeLog},
			{ID: "proc-2", Type: ComponentTypeLog},
		},
		Connections: []Connection{
			{SourceComponentID: "proc-1", TargetComponentID: "proc-2"},
			{SourceComponentID: "proc-2", TargetComponentID: "proc-1"},
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for pipeline with cycle (no topology source)")
	}
}

func TestValidateGraph_MissingSink(t *testing.T) {
	// Invalid: all components have outgoing connections, no topology sink
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "proc-1", Type: ComponentTypeLog},
			{ID: "proc-2", Type: ComponentTypeLog},
		},
		Connections: []Connection{
			{SourceComponentID: "proc-1", TargetComponentID: "proc-2"},
			{SourceComponentID: "proc-2", TargetComponentID: "proc-1"},
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for pipeline with cycle (no topology sink)")
	}
}

func TestValidateGraph_SimpleCycle(t *testing.T) {
	// Invalid: cycle between two processors
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "processor-1", Type: ComponentTypeLog},
			{ID: "processor-2", Type: ComponentTypePythonCodeBlock},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "processor-1"},
			{SourceComponentID: "processor-1", TargetComponentID: "processor-2"},
			{SourceComponentID: "processor-2", TargetComponentID: "processor-1"}, // Cycle!
			{SourceComponentID: "processor-2", TargetComponentID: "sink-1"},
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for pipeline with cycle")
	}
}

func TestValidateGraph_SelfLoop(t *testing.T) {
	// Invalid: component connects to itself
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "processor-1", Type: ComponentTypeLog},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "processor-1"},
			{SourceComponentID: "processor-1", TargetComponentID: "processor-1"}, // Self-loop!
			{SourceComponentID: "processor-1", TargetComponentID: "sink-1"},
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for pipeline with self-loop")
	}
}

func TestValidateGraph_SinkAsSource(t *testing.T) {
	// Invalid: strict sink component used as source in connection
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeLogSink},
			{ID: "sink-2", Type: ComponentTypeKafkaProducer},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "sink-1"},
			{SourceComponentID: "sink-1", TargetComponentID: "sink-2"}, // Sink as source!
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for sink component with outgoing connection")
	}
}

func TestValidateGraph_SourceAsTarget(t *testing.T) {
	// Invalid: source component used as target in connection
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "source-2", Type: ComponentTypeKafkaConsumer},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "source-2"}, // Source as target!
			{SourceComponentID: "source-2", TargetComponentID: "sink-1"},
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for source component with incoming connection")
	}
}

func TestValidateGraph_NonExistentSourceComponent(t *testing.T) {
	// Invalid: connection references non-existent source component
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "non-existent", TargetComponentID: "sink-1"},
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for connection with non-existent source component")
	}
}

func TestValidateGraph_NonExistentTargetComponent(t *testing.T) {
	// Invalid: connection references non-existent target component
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "non-existent"},
		},
	}

	err := ValidateGraph(def)
	if err == nil {
		t.Error("Expected error for connection with non-existent target component")
	}
}

func TestValidateGraph_FanoutPattern(t *testing.T) {
	// Valid: one source feeding multiple sinks (fanout)
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
			{ID: "sink-2", Type: ComponentTypeKafkaProducer},
			{ID: "sink-3", Type: ComponentTypeRabbitMQProducer},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "sink-1"},
			{SourceComponentID: "source-1", TargetComponentID: "sink-2"},
			{SourceComponentID: "source-1", TargetComponentID: "sink-3"},
		},
	}

	err := ValidateGraph(def)
	if err != nil {
		t.Errorf("Expected valid fanout pattern to pass validation, got error: %v", err)
	}
}

func TestValidateGraph_ComplexDAG(t *testing.T) {
	// Valid: complex DAG with multiple paths
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "processor-1", Type: ComponentTypeLog},
			{ID: "processor-2", Type: ComponentTypePythonCodeBlock},
			{ID: "processor-3", Type: ComponentTypeLog},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
			{ID: "sink-2", Type: ComponentTypeKafkaProducer},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "processor-1"},
			{SourceComponentID: "source-1", TargetComponentID: "processor-2"},
			{SourceComponentID: "processor-1", TargetComponentID: "processor-3"},
			{SourceComponentID: "processor-2", TargetComponentID: "sink-1"},
			{SourceComponentID: "processor-3", TargetComponentID: "sink-2"},
		},
	}

	err := ValidateGraph(def)
	if err != nil {
		t.Errorf("Expected valid complex DAG to pass validation, got error: %v", err)
	}
}

func TestValidateGraph_DirectSourceToSink(t *testing.T) {
	// Valid: direct connection from source to sink (no processor)
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "sink-1"},
		},
	}

	err := ValidateGraph(def)
	if err != nil {
		t.Errorf("Expected valid direct source-to-sink connection to pass validation, got error: %v", err)
	}
}

func TestValidateGraph_MultipleSourcesAndSinks(t *testing.T) {
	// Valid: multiple sources and sinks
	def := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "source-2", Type: ComponentTypeKafkaConsumer},
			{ID: "processor-1", Type: ComponentTypeLog},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
			{ID: "sink-2", Type: ComponentTypeKafkaProducer},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "processor-1"},
			{SourceComponentID: "source-2", TargetComponentID: "processor-1"},
			{SourceComponentID: "processor-1", TargetComponentID: "sink-1"},
			{SourceComponentID: "processor-1", TargetComponentID: "sink-2"},
		},
	}

	err := ValidateGraph(def)
	if err != nil {
		t.Errorf("Expected valid pipeline with multiple sources and sinks to pass validation, got error: %v", err)
	}
}

func TestIsSourceComponent(t *testing.T) {
	tests := []struct {
		compType ComponentType
		expected bool
	}{
		{ComponentTypeSQLQuery, true},
		{ComponentTypeCSVReader, true},
		{ComponentTypeKafkaConsumer, true},
		{ComponentTypeRabbitMQConsumer, true},
		{ComponentTypeHL7Reader, true},
		{ComponentTypeTCPRead, true},
		{ComponentTypeS3Reader, true},
		{ComponentTypeMinIOReader, true},
		{ComponentTypeAzureBlobReader, true},
		{ComponentTypeLocalStorageReader, true},
		// Dual-mode components are NOT strict sources
		{ComponentTypeHTTPGet, false},
		{ComponentTypeHTTPPost, false},
		{ComponentTypeLog, false},
		{ComponentTypePythonCodeBlock, false},
		{ComponentTypePrintLog, false},
	}

	for _, tt := range tests {
		result := isSourceComponent(tt.compType)
		if result != tt.expected {
			t.Errorf("isSourceComponent(%s) = %v, expected %v", tt.compType, result, tt.expected)
		}
	}
}

func TestIsSinkComponent(t *testing.T) {
	tests := []struct {
		compType ComponentType
		expected bool
	}{
		{ComponentTypeKafkaProducer, true},
		{ComponentTypeRabbitMQProducer, true},
		{ComponentTypeTCPWrite, true},
		{ComponentTypeLogSink, true},
		{ComponentTypeLychgateResponse, true},
		{ComponentTypeS3Writer, true},
		{ComponentTypeMinIOWriter, true},
		{ComponentTypeAzureBlobWriter, true},
		{ComponentTypeLocalStorageWriter, true},
		// Dual-mode components are NOT strict sinks
		{ComponentTypeHTTPPost, false},
		{ComponentTypeHTTPGet, false},
		{ComponentTypeLog, false},
		{ComponentTypePythonCodeBlock, false},
		{ComponentTypePrintLog, false},
	}

	for _, tt := range tests {
		result := isSinkComponent(tt.compType)
		if result != tt.expected {
			t.Errorf("isSinkComponent(%s) = %v, expected %v", tt.compType, result, tt.expected)
		}
	}
}

func TestIsProcessorComponent(t *testing.T) {
	tests := []struct {
		compType ComponentType
		expected bool
	}{
		{ComponentTypeLog, true},
		{ComponentTypePythonCodeBlock, true},
		{ComponentTypeAttributeUpdate, true},
		{ComponentTypeJSONExtractor, true},
		{ComponentTypeJSONTransform, true},
		{ComponentTypePrintLog, true},
		// Dual-mode and other categories
		{ComponentTypeHTTPGet, false},
		{ComponentTypeHTTPPost, false},
		{ComponentTypeLogSink, false},
	}

	for _, tt := range tests {
		result := isProcessorComponent(tt.compType)
		if result != tt.expected {
			t.Errorf("isProcessorComponent(%s) = %v, expected %v", tt.compType, result, tt.expected)
		}
	}
}
