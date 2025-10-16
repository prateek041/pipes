package pipeline

// MapStage implements the Stage interface for the Map primitive.
type MapStage struct {
	config   Config
	userFunc func(SimpleEvent) SimpleEvent
}

// NewMapStage creates a new MapStage.
func NewMapStage(cfg Config, fn func(SimpleEvent) SimpleEvent) *MapStage {
	return &MapStage{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name.
func (s *MapStage) Name() string {
	return "Map"
}

// ProcessBatch applies the user's Map function to every event in the batch.
// RIght now this is a simple, sequential for loop.
func (s *MapStage) ProcessBatch(batch []SimpleEvent) ([]SimpleEvent, error) {
	output := make([]SimpleEvent, 0, len(batch))

	for _, event := range batch {
		result := s.userFunc(event)
		output = append(output, result)
	}

	return output, nil
}

// Connect is a dummy implementation for now. Full concurrency rigt after this.
func (s *MapStage) Connect(inChan <-chan SimpleEvent, outChan chan<- SimpleEvent, emitter Emitter) error {
	// Placeholder: To be implemented next
	return nil
}
