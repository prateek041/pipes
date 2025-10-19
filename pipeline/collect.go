package pipeline

import "sync"

type CollectStage[T any] struct {
	results []T
	lock    sync.Mutex
}

func NewCollectStage[T any]() *CollectStage[T] {
	return &CollectStage[T]{
		results: make([]T, 0),
	}
}

func (s *CollectStage[T]) Name() string {
	return "Collect"
}

func (s *CollectStage[T]) ProcessBatch(batch []T) ([]T, error) {
	return batch, nil
}

func (s *CollectStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter) error {
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

func (s *CollectStage[T]) Results() []T {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.results
}
