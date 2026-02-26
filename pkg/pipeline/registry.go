package pipeline

import "sync"

// DefaultRegistry is the global component registry.
// Custom components can self-register here via init() functions.
var DefaultRegistry = &Registry{
	constructors: make(map[ComponentType]ComponentConstructor),
}

// Registry holds component constructors for self-registration.
type Registry struct {
	mu           sync.RWMutex
	constructors map[ComponentType]ComponentConstructor
}

// Register adds a component constructor to the global registry.
// Call this from your component package's init() function:
//
//	func init() {
//	    pipeline.DefaultRegistry.Register("my_component", NewMyComponent)
//	}
func (r *Registry) Register(componentType ComponentType, constructor ComponentConstructor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.constructors[componentType] = constructor
}

// Get returns a constructor for the given type, or nil if not found.
func (r *Registry) Get(componentType ComponentType) (ComponentConstructor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.constructors[componentType]
	return c, ok
}

// All returns all registered component types and their constructors.
func (r *Registry) All() map[ComponentType]ComponentConstructor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[ComponentType]ComponentConstructor, len(r.constructors))
	for k, v := range r.constructors {
		result[k] = v
	}
	return result
}

// RegisterAll copies all entries from the registry into a ComponentFactory.
// Call this after all init() registrations have run (e.g. in main or engine init).
func (r *Registry) RegisterAll(factory ComponentFactory) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for compType, constructor := range r.constructors {
		factory.Register(compType, constructor)
	}
}
