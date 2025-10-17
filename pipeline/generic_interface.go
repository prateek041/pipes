package pipeline

import "sync"

// Generic Stage interface that can work with any type T
type GenericStage[T any] interface {
	// ProcessBatch is the method that runs inside a Worker Goroutine.
	ProcessBatch(batch []T) ([]T, error)

	// Connect sets up the plumbing (channels and goroutines) for this stage.
	// Waitgroup to signal when it's finished processing.
	Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter) error

	// Name returns the stage name for observability and logging.
	Name() string
}

// TransformStage is for stages that can transform from type TIn to type TOut
// This is useful for Reduce operations where input and output types differ
type TransformStage[TIn, TOut any] interface {
	ProcessBatch(batch []TIn) ([]TOut, error)
	Connect(wg *sync.WaitGroup, inChan <-chan TIn, outChan chan<- TOut, emitter Emitter) error
	Name() string
}
