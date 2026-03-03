package pipeline

import (
	"fmt"
)

// ValidateGraph validates the pipeline component graph structure
// It checks for:
// - At least one source component
// - At least one sink component
// - No cycles (DAG property)
// - Type compatibility between connected components
func ValidateGraph(def PipelineDefinition) error {
	if err := validateComponentPresence(def); err != nil {
		return err
	}

	if err := validateNoCycles(def); err != nil {
		return err
	}

	if err := validateTypeCompatibility(def); err != nil {
		return err
	}

	return nil
}

// validateComponentPresence ensures at least one source and one sink component exist
func validateComponentPresence(def PipelineDefinition) error {
	// Build a map of components with incoming connections
	hasIncoming := make(map[string]bool)
	hasOutgoing := make(map[string]bool)

	for _, conn := range def.Connections {
		hasOutgoing[conn.SourceComponentID] = true
		hasIncoming[conn.TargetComponentID] = true
	}

	// Check for at least one component without incoming connections (topology source)
	hasSource := false
	for _, comp := range def.Components {
		if !hasIncoming[comp.ID] {
			hasSource = true
			break
		}
	}

	// Check for at least one component without outgoing connections (topology sink)
	hasSink := false
	for _, comp := range def.Components {
		if !hasOutgoing[comp.ID] {
			hasSink = true
			break
		}
	}

	if !hasSource {
		return fmt.Errorf("pipeline must have at least one source component")
	}

	if !hasSink {
		return fmt.Errorf("pipeline must have at least one sink component")
	}

	return nil
}

// validateNoCycles checks that the component graph is a DAG (no cycles)
func validateNoCycles(def PipelineDefinition) error {
	// Build adjacency list
	graph := buildAdjacencyList(def)

	// Track visited nodes and recursion stack
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	// Check each component for cycles using DFS
	for _, comp := range def.Components {
		if !visited[comp.ID] {
			if hasCycleDFS(comp.ID, graph, visited, recStack) {
				return fmt.Errorf("pipeline contains a cycle involving component %s", comp.ID)
			}
		}
	}

	return nil
}

// hasCycleDFS performs depth-first search to detect cycles
func hasCycleDFS(nodeID string, graph map[string][]string, visited, recStack map[string]bool) bool {
	visited[nodeID] = true
	recStack[nodeID] = true

	// Visit all neighbors
	for _, neighbor := range graph[nodeID] {
		if !visited[neighbor] {
			if hasCycleDFS(neighbor, graph, visited, recStack) {
				return true
			}
		} else if recStack[neighbor] {
			// Found a back edge (cycle)
			return true
		}
	}

	recStack[nodeID] = false
	return false
}

// validateTypeCompatibility checks that connected components have compatible types
func validateTypeCompatibility(def PipelineDefinition) error {
	// Build component map for quick lookup
	compMap := make(map[string]ComponentConfig)
	for _, comp := range def.Components {
		compMap[comp.ID] = comp
	}

	// Check each connection
	for _, conn := range def.Connections {
		sourceComp, sourceExists := compMap[conn.SourceComponentID]
		targetComp, targetExists := compMap[conn.TargetComponentID]

		if !sourceExists {
			return fmt.Errorf("connection references non-existent source component: %s", conn.SourceComponentID)
		}

		if !targetExists {
			return fmt.Errorf("connection references non-existent target component: %s", conn.TargetComponentID)
		}

		// Validate type compatibility
		if err := validateComponentConnection(sourceComp, targetComp); err != nil {
			return fmt.Errorf("incompatible connection from %s to %s: %w",
				conn.SourceComponentID, conn.TargetComponentID, err)
		}
	}

	return nil
}

// validateComponentConnection checks if two components can be connected
func validateComponentConnection(source, target ComponentConfig) error {
	// Only strict sink components (those that truly can't output) cannot have outgoing connections
	if isSinkComponent(source.Type) {
		return fmt.Errorf("sink component %s (%s) cannot have outgoing connections",
			source.ID, source.Type)
	}

	// Only strict source components (those that truly can't accept input) cannot have incoming connections
	if isSourceComponent(target.Type) {
		return fmt.Errorf("source component %s (%s) cannot have incoming connections",
			target.ID, target.Type)
	}

	// All other combinations are valid (processors can connect to anything)
	return nil
}

// buildAdjacencyList creates a graph representation from connections
func buildAdjacencyList(def PipelineDefinition) map[string][]string {
	graph := make(map[string][]string)

	// Initialize all components in the graph
	for _, comp := range def.Components {
		graph[comp.ID] = []string{}
	}

	// Add edges from connections
	for _, conn := range def.Connections {
		graph[conn.SourceComponentID] = append(graph[conn.SourceComponentID], conn.TargetComponentID)
	}

	return graph
}

// isSourceComponent returns true if the component type is a source
func isSourceComponent(compType ComponentType) bool {
	switch compType {
	case ComponentTypeSQLQuery,
		ComponentTypeCSVReader,
		ComponentTypeKafkaConsumer,
		ComponentTypeRabbitMQConsumer,
		ComponentTypeHL7Reader,
		ComponentTypeTCPRead:
		return true
	default:
		return false
	}
}

// isSinkComponent returns true if the component type is a sink
func isSinkComponent(compType ComponentType) bool {
	switch compType {
	case ComponentTypeKafkaProducer,
		ComponentTypeRabbitMQProducer,
		ComponentTypeTCPWrite,
		ComponentTypeLogSink:
		return true
	default:
		return false
	}
}

// isProcessorComponent returns true if the component type is a processor
func isProcessorComponent(compType ComponentType) bool {
	switch compType {
	case ComponentTypePythonCodeBlock,
		ComponentTypeLog,
		ComponentTypeAttributeUpdate:
		return true
	default:
		return false
	}
}
