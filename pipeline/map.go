package pipeline

import (
	"sync"
	"time"
)

// MapStage implements the Stage interface for the Map primitive.
type MapStage struct {
	config   Config
	userFunc func(SimpleEvent) SimpleEvent
}

// NewMapStage creates a new MapStage.
func NewMapStage(cfg Config, fn func(SimpleEvent) SimpleEvent) *MapStage {
	return &MapStage{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name.
func (s *MapStage) Name() string {
	return "Map"
}

// ProcessBatch applies the user's Map function to every event in the batch.
// RIght now this is a simple, sequential for loop.
func (s *MapStage) ProcessBatch(batch []SimpleEvent) ([]SimpleEvent, error) {
	output := make([]SimpleEvent, 0, len(batch))

	for _, event := range batch {
		result := s.userFunc(event)
		output = append(output, result)
	}

	return output, nil
}

// Connect is a dummy implementation for now. Full concurrency rigt after this.
func (s *MapStage) Connect(wg *sync.WaitGroup, inChan <-chan SimpleEvent, outChan chan<- SimpleEvent, emitter Emitter) error {
	defer wg.Done()

	// start the worker pool.
	workChan := make(chan []SimpleEvent, s.config.MaxWorkersPerStage)
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

	batch := make([]SimpleEvent, 0, s.config.MaxBatchSize)
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
				batch = make([]SimpleEvent, 0, s.config.MaxBatchSize)

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
				batch = make([]SimpleEvent, 0, s.config.MaxBatchSize)
			}
		}
	}
}
