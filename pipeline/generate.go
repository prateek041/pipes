package pipeline

import (
	"sync"
	"time"
)

// GenerateStage implements the Stage interface for the Generate primitive.
// Generate takes an input event of type T and produces two events: the original + a new generated one.
type GenerateStage[T any] struct {
	config   Config
	userFunc func(T) T // Function to generate new event from original
}

// NewGenerateStage creates a new GenerateStage.
func NewGenerateStage[T any](cfg Config, fn func(T) T) *GenerateStage[T] {
	return &GenerateStage[T]{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name.
func (s *GenerateStage[T]) Name() string {
	return "Generate"
}

// ProcessBatch applies the user's Generate function to every event in the batch.
// For each input event, outputs the original event + a newly generated event.
// Input: [A, B, C] -> Output: [A, A', B, B', C, C'] where A', B', C' are generated events.
func (s *GenerateStage[T]) ProcessBatch(batch []T) ([]T, error) {
	output := make([]T, 0, len(batch)*2) // Double capacity for original + generated

	for _, event := range batch {
		output = append(output, event)     // Original event
		generated := s.userFunc(event)     // Generate new event
		output = append(output, generated) // New event
	}

	return output, nil
}

// Connect implements the concurrent processing logic with batching and worker pools.
func (s *GenerateStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter, executionID string, index uint32) error {
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
