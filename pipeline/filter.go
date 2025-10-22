package pipeline

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FilterStage implements the Stage interface for the Filter primitive.
type FilterStage[T any] struct {
	config   Config
	userFunc func(T) bool
}

// NewFilterStage creates a new FilterStage.
func NewFilterStage[T any](cfg Config, fn func(T) bool) *FilterStage[T] {
	return &FilterStage[T]{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name.
func (s *FilterStage[T]) Name() string {
	return "Filter"
}

// ProcessBatch applies the user's Filter function and only passes events that return true.
func (s *FilterStage[T]) ProcessBatch(batch []T) ([]T, error) {
	output := make([]T, 0, len(batch))

	for _, event := range batch {
		if s.userFunc(event) {
			output = append(output, event)
		}
	}

	return output, nil
}

func (s *FilterStage[T]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter, executionId string, stageIndex uint32) error {
	defer wg.Done()

	stageStart := time.Now()
	var inputCount, outputCount uint64

	emitter.EmitStageStart(executionId, s.Name(), stageIndex)

	defer func() {
		duration := time.Since(stageStart)
		emitter.EmitStageEnd(executionId, s.Name(), inputCount, outputCount, duration)
	}()

	// Spin up the workers and start processing.
	workerChan := make(chan []T, s.config.MaxWorkersPerStage)
	var workerWg sync.WaitGroup

	for i := 0; i < s.config.MaxWorkersPerStage; i++ {
		workerWg.Add(1)
		go func() {
			workerId := fmt.Sprintf("worker-%d", i)
			defer workerWg.Done()
			for batch := range workerChan {
				batchId := GenerateBatchID()
				batchStart := time.Now()
				processedBatch, err := s.ProcessBatch(batch)
				if err != nil {
					emitter.EmitError(executionId, s.Name(), err.Error())
					continue
				}

				processingDuration := time.Since(batchStart)
				emitter.EmitBatchMetrics(executionId, s.Name(), batchId, workerId, uint32(len(batch)), processingDuration)

				atomic.AddUint64(&inputCount, uint64(len(batch)))
				atomic.AddUint64(&outputCount, uint64(len(processedBatch)))

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
