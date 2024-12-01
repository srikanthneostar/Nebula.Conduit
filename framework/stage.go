package framework

import "io"

// Stage represents a single stage in the pipeline
type Stage interface {
	Execute(input io.Reader) error
	Output() io.Reader
}
