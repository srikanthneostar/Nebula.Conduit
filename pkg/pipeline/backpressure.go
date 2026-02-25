package pipeline

// BackpressureSystem provides integration with the existing backpressure mechanisms
type BackpressureSystem interface {
	// IsHighLoad returns true if system is under high load
	IsHighLoad() bool

	// IsCircuitOpen returns true if circuit breaker is open
	IsCircuitOpen() bool

	// GetRateLimit returns the current rate limit for operations
	GetRateLimit() int

	// RecordOperation records an operation for rate limiting
	RecordOperation(operationType string) error
}
