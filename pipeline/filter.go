package pipeline

// FilterStage implements the Stage interface for the Filter primitive.
type FilterStage struct {
	config   Config
	userFunc func(SimpleEvent) bool
}

// NewFilterStage creates a new FilterStage.
func NewFilterStage(cfg Config, fn func(SimpleEvent) bool) *FilterStage {
	return &FilterStage{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name.
func (s *FilterStage) Name() string {
	return "Filter"
}

// ProcessBatch applies the user's Filter function and only passes events that return true.
func (s *FilterStage) ProcessBatch(batch []SimpleEvent) ([]SimpleEvent, error) {
	output := make([]SimpleEvent, 0, len(batch))

	for _, event := range batch {
		if s.userFunc(event) {
			output = append(output, event)
		}
	}

	return output, nil
}

func (s *FilterStage) Connect(inChan <-chan SimpleEvent, outChan chan<- SimpleEvent, emitter Emitter) error {
	// Placeholder: To be implemented in Phase 2
	return nil
}
