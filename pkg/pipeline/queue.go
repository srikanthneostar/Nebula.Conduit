package pipeline

import "context"

// DataQueue provides a queue interface for backpressure handling
type DataQueue interface {
	// Enqueue adds data to the queue
	Enqueue(ctx context.Context, componentID string, data Data) error

	// Dequeue removes and returns data from the queue
	Dequeue(ctx context.Context, componentID string) (Data, bool, error)

	// Size returns the current queue size
	Size(ctx context.Context, componentID string) (int, error)

	// Clear removes all data from the queue
	Clear(ctx context.Context, componentID string) error
}
