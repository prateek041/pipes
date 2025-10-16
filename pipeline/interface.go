package pipeline

// Emitter interface for observability.
type Emitter interface {
	EmitMetadata(stageName string, data map[string]any)
}

type Stage interface {
	// ProcessBatch is the method that runs inside a Worker Goroutine.
	ProcessBatch(batch []SimpleEvent) ([]SimpleEvent, error)

	// Connect sets up the plumbing (channels and goroutines) for this stage.
	Connect(inChan <-chan SimpleEvent, outChan chan<- SimpleEvent, emitter Emitter) error

	// Name returns the stage name for observability and logging.
	Name() string
}
