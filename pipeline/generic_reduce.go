package pipeline

import (
	"sync"
	"time"
)

type GenericReduceStage[T any] struct {
	config   Config
	userFunc func([]T) T
}

func NewGenericReduceStage[T any](cfg Config, fn func([]T) T) *GenericReduceStage[T] {
	return &GenericReduceStage[T]{
		config:   cfg,
		userFunc: fn,
	}
}

func (r *GenericReduceStage[T]) Name() string {
	return "GenericReduce"
}

func (s *GenericReduceStage[T]) ProcessBatch(batch []T) ([]T, error) {
	if len(batch) == 0 {
		return []T{}, nil
	}

	result := s.userFunc(batch)
	return []T{result}, nil
}

func (s *GenericReduceStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter) error {
	defer wg.Done()
	var workerWg sync.WaitGroup
	workChan := make(chan []T, s.config.MaxWorkersPerStage)
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
	batch := make([]T, 0, s.config.MaxBatchSize)
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
				batch = make([]T, 0, s.config.MaxBatchSize)
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
				batch = make([]T, 0, s.config.MaxBatchSize)
			}
		}
	}
}
