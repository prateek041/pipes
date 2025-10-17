package pipeline

import "sync"

type GenericCollectStage[T any] struct {
	results []T
	lock    sync.Mutex
}

func NewGenericCollectStage[T any]() *GenericCollectStage[T] {
	return &GenericCollectStage[T]{
		results: make([]T, 0),
	}
}

func (s *GenericCollectStage[T]) Name() string {
	return "GenericCollect"
}

func (s *GenericCollectStage[T]) ProcessBatch(batch []T) ([]T, error) {
	return batch, nil
}

func (s *GenericCollectStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter) error {
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

func (s *GenericCollectStage[T]) Results() []T {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.results
}
