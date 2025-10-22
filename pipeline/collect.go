package pipeline

import "sync"

// CollectStage buffers all events it observes and provides access to the
// collected results via Results(). It also forwards events downstream so the
// stage behaves transparently when part of a larger pipeline.
type CollectStage[T any] struct {
	results []T
	lock    sync.Mutex
}

// NewCollectStage creates a new CollectStage.
func NewCollectStage[T any]() *CollectStage[T] {
	return &CollectStage[T]{
		results: make([]T, 0),
	}
}

// Name returns the stage name used for observability.
func (s *CollectStage[T]) Name() string {
	return "Collect"
}

// ProcessBatch returns the batch unchanged. The CollectStage performs
// collection in Connect where items are appended to the internal buffer.
func (s *CollectStage[T]) ProcessBatch(batch []T) ([]T, error) {
	return batch, nil
}

// Connect drains the input channel, records each item, forwards it and
// closes the output channel when finished.
func (s *CollectStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter, executionID string, index uint32) error {
	defer wg.Done()
	defer close(outChan)

	for event := range inChan {
		s.lock.Lock()
		s.results = append(s.results, event)
		s.lock.Unlock()

		// Forward the event to the next stage
		outChan <- event
	}
	return nil
}

// Results returns a snapshot of collected events. The slice is a copy of
// the internal buffer protected by a mutex.
func (s *CollectStage[T]) Results() []T {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.results
}
