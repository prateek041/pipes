package pipeline

import (
	"sync"
	"time"
)

type ReduceStage struct {
	config   Config
	userFunc func([]SimpleEvent) SimpleEvent
}

func NewReduceStage(cfg Config, fn func([]SimpleEvent) SimpleEvent) *ReduceStage {
	return &ReduceStage{
		config:   cfg,
		userFunc: fn,
	}
}

func (r *ReduceStage) Name() string {
	return "Reduce"
}

func (s *ReduceStage) ProcessBatch(batch []SimpleEvent) ([]SimpleEvent, error) {
	if len(batch) == 0 {
		return []SimpleEvent{}, nil
	}

	result := s.userFunc(batch)
	return []SimpleEvent{result}, nil
}

func (s *ReduceStage) Connect(wg *sync.WaitGroup, inChan <-chan SimpleEvent, outChan chan<- SimpleEvent, emitter Emitter) error {
	defer wg.Done()
	var workerWg sync.WaitGroup
	workChan := make(chan []SimpleEvent, s.config.MaxWorkersPerStage)
	for i := 0; i < s.config.MaxWorkersPerStage; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for batch := range workChan {
				processedBatch, err := s.ProcessBatch(batch)
				if err != nil {
					panic(err)
				}

				for _, event := range processedBatch {
					outChan <- event
				}
			}
		}()
	}

	// batching process below.
	batch := make([]SimpleEvent, 0, s.config.MaxBatchSize)
	timer := time.NewTimer(s.config.BatchTimeout)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case event, ok := <-inChan:
			if !ok {
				if len(batch) > 0 {
					workChan <- batch
				}
				close(workChan)
				workerWg.Wait()
				close(outChan)
				return nil
			}

			if len(batch) == 0 {
				timer.Reset(s.config.BatchTimeout)
			}
			batch = append(batch, event)

			if len(batch) >= s.config.MaxBatchSize {
				workChan <- batch
				batch = make([]SimpleEvent, 0, s.config.MaxBatchSize)
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}

		case <-timer.C:
			if len(batch) > 0 {
				workChan <- batch
				batch = make([]SimpleEvent, 0, s.config.MaxBatchSize)
			}
		}
	}
}
