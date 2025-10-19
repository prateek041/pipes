package pipeline

import (
	"sync"
	"time"
)

// MapStage implements the Stage interface for the Map primitive.
type MapStage[T any] struct {
	config   Config
	userFunc func(T) T
}

// NewMapStage creates a new MapStage.
func NewMapStage[T any](cfg Config, fn func(T) T) *MapStage[T] {
	return &MapStage[T]{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name.
func (s *MapStage[T]) Name() string {
	return "Map"
}

// ProcessBatch applies the user's Map function to every event in the batch.
func (s *MapStage[T]) ProcessBatch(batch []T) ([]T, error) {
	output := make([]T, 0, len(batch))

	for _, event := range batch {
		result := s.userFunc(event)
		output = append(output, result)
	}

	return output, nil
}

// Connect implements the concurrent processing logic with batching and worker pools.
func (s *MapStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter) error {
	defer wg.Done()

	// start the worker pool.
	workChan := make(chan []T, s.config.MaxWorkersPerStage)
	var workerWg sync.WaitGroup
	for i := 0; i < s.config.MaxWorkersPerStage; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for batch := range workChan {
				processedBatch, err := s.ProcessBatch(batch)
				if err != nil {
					panic(err) // for now just panic.
				}
				for _, event := range processedBatch {
					outChan <- event
				}
			}
		}()
	}

	batch := make([]T, 0, s.config.MaxBatchSize)
	timer := time.NewTimer(s.config.BatchTimeout)

	// time is stopped because we need it active when there is an item in the batch.
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case event, ok := <-inChan:
			if !ok {
				// input channel is closed.
				if len(batch) > 0 {
					workChan <- batch // process the remaining batches.
				}
				close(workChan) // Signal workers to stop.
				workerWg.Wait()
				close(outChan)
				return nil
			}

			// Reset timer only when the first item is added to an empty batch.
			if len(batch) == 0 {
				timer.Reset(s.config.BatchTimeout)
			}
			batch = append(batch, event)

			if len(batch) >= s.config.MaxBatchSize {
				workChan <- batch
				batch = make([]T, 0, s.config.MaxBatchSize)

				// We sent the batch to process and its empty so stop the timer.
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
