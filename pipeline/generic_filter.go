package pipeline

import (
	"sync"
	"time"
)

// GenericFilterStage implements the GenericStage interface for the Filter primitive.
type GenericFilterStage[T any] struct {
	config   Config
	userFunc func(T) bool
}

// NewGenericFilterStage creates a new GenericFilterStage.
func NewGenericFilterStage[T any](cfg Config, fn func(T) bool) *GenericFilterStage[T] {
	return &GenericFilterStage[T]{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name.
func (s *GenericFilterStage[T]) Name() string {
	return "GenericFilter"
}

// ProcessBatch applies the user's Filter function and only passes events that return true.
func (s *GenericFilterStage[T]) ProcessBatch(batch []T) ([]T, error) {
	output := make([]T, 0, len(batch))

	for _, event := range batch {
		if s.userFunc(event) {
			output = append(output, event)
		}
	}

	return output, nil
}

func (s *GenericFilterStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter) error {
	defer wg.Done()

	// Spin up the workers and start processing.
	workerChan := make(chan []T, s.config.MaxWorkersPerStage)
	var workerWg sync.WaitGroup

	for i := 0; i < s.config.MaxWorkersPerStage; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for batch := range workerChan {
				processedBatch, err := s.ProcessBatch(batch)
				if err != nil {
					panic(err)
				}

				// Emitting event by event on the output channel.
				for _, item := range processedBatch {
					outChan <- item
				}
			}
		}()
	}

	batch := make([]T, 0, s.config.MaxBatchSize)
	timer := time.NewTimer(s.config.BatchTimeout)

	if !timer.Stop() {
		<-timer.C // flush the channel
	}

	for {
		select {
		case event, ok := <-inChan:
			if !ok {
				// process the remaining batch and close the output channel.
				if len(batch) > 0 {
					workerChan <- batch
				}

				close(workerChan) // since there are no inputs left.
				workerWg.Wait()
				close(outChan)
				return nil
			}

			if len(batch) == 0 {
				timer.Reset(s.config.BatchTimeout)
			}

			batch = append(batch, event)

			if len(batch) >= s.config.MaxBatchSize {
				workerChan <- batch
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
				workerChan <- batch
				batch = make([]T, 0, s.config.MaxBatchSize)
			}
		}
	}
}
