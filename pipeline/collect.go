package pipeline

import "sync"

type CollectStage struct {
	results []SimpleEvent
	lock    sync.Mutex
}

func NewCollectStage() *CollectStage {
	return &CollectStage{
		results: make([]SimpleEvent, 0),
	}
}

func (s *CollectStage) Name() string {
	return "Collect"
}

func (s *CollectStage) ProcessBatch(batch []SimpleEvent) ([]SimpleEvent, error) {
	return batch, nil
}

func (s *CollectStage) Connect(wg *sync.WaitGroup, inChan <-chan SimpleEvent, outChan chan<- SimpleEvent, emitter Emitter) error {
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

func (s *CollectStage) Results() []SimpleEvent {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.results
}
