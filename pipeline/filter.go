package pipeline

import (
	"sync"
	"time"
)

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

func (s *FilterStage) Connect(wg *sync.WaitGroup, inChan <-chan SimpleEvent, outChan chan<- SimpleEvent, emitter Emitter) error {
	defer wg.Done()

	// Spin up the workers and start processing.
	workerChan := make(chan []SimpleEvent, s.config.MaxWorkersPerStage)
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

	batch := make([]SimpleEvent, 0, s.config.MaxBatchSize)
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
				workerChan <- batch
				batch = make([]SimpleEvent, 0, s.config.MaxBatchSize)
			}
		}
	}
}
