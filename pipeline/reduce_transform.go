package pipeline

import (
	"sync"
	"time"
)

// ReduceTransformStage transforms input type T to output type R using a reduction function.
// This implements the TransformStage[T,R] interface to enable type transformations in pipelines.
type ReduceTransformStage[T, R any] struct {
	config   Config
	userFunc func([]T) R
}

// NewReduceTransformStage creates a new ReduceTransformStage with the given configuration and function.
func NewReduceTransformStage[T, R any](cfg Config, fn func([]T) R) *ReduceTransformStage[T, R] {
	return &ReduceTransformStage[T, R]{
		config:   cfg,
		userFunc: fn,
	}
}

// Name returns the stage name for observability and logging.
func (r *ReduceTransformStage[T, R]) Name() string {
	return "ReduceTransform"
}

// ProcessBatch processes a batch of type T and returns a batch of type R.
func (r *ReduceTransformStage[T, R]) ProcessBatch(batch []T) ([]R, error) {
	if len(batch) == 0 {
		return []R{}, nil
	}

	// Apply the user function to transform T -> R
	result := r.userFunc(batch)
	return []R{result}, nil
}

// Connect sets up the plumbing (channels and goroutines) for this transform stage.
// This handles the T->R type transformation with proper channel management.
func (r *ReduceTransformStage[T, R]) Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- R, emitter Emitter) error {
	defer wg.Done()

	// Create worker pool for processing batches
	var workerWg sync.WaitGroup
	workChan := make(chan []T, r.config.MaxWorkersPerStage)

	// Start worker goroutines
	for i := 0; i < r.config.MaxWorkersPerStage; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for batch := range workChan {
				processedBatch, err := r.ProcessBatch(batch)
				if err != nil {
					// TODO: Better error handling
					panic(err)
				}

				// Send transformed results to output channel
				for _, event := range processedBatch {
					outChan <- event
				}
			}
		}()
	}

	// Batch processing logic (similar to ReduceStage but with T->R transformation)
	batch := make([]T, 0, r.config.MaxBatchSize)
	timer := time.NewTimer(r.config.BatchTimeout)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case event, ok := <-inChan:
			if !ok {
				// Input channel closed, process final batch if any
				if len(batch) > 0 {
					workChan <- batch
				}
				close(workChan)
				workerWg.Wait()
				close(outChan)
				return nil
			}

			// Start timer for first event in batch
			if len(batch) == 0 {
				timer.Reset(r.config.BatchTimeout)
			}
			batch = append(batch, event)

			// Send batch when it reaches max size
			if len(batch) >= r.config.MaxBatchSize {
				workChan <- batch
				batch = make([]T, 0, r.config.MaxBatchSize)
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}

		case <-timer.C:
			// Timeout reached, send current batch if any
			if len(batch) > 0 {
				workChan <- batch
				batch = make([]T, 0, r.config.MaxBatchSize)
			}
		}
	}
}

// ReduceTransformAndExecute runs the pipeline end to end and gives you the result directly.
// Use this if you want the final result of the execution.
func ReduceTransformAndExecute[T, R any](p *Pipeline[T], fn func([]T) R, input []T) []R {
	transformStage := NewReduceTransformStage(p.config, fn)
	return executeTransform(p, transformStage, input, p.executionID)
}

// ReduceTransformAndStream runs the pipeline and returns a channel you can use to chain the pipeline
// further.
// Use this if you want to chain this pipeline further.
// Since this is a transform pipeline, we are transforming
// and executing all the pipeline stages just before it, therefore
// we will also be emitting pipeline related tasks up till now.
func ReduceTransformAndStream[T, R any](p *Pipeline[T], fn func([]T) R, inputChan <-chan T) (<-chan R, Config) {
	p.startTime = time.Now()

	transformStage := NewReduceTransformStage(p.config, fn)
	resultChan := make(chan R, p.config.MaxBatchSize)

	p.pipelineType = p.generatePipelineType()

	p.config.Emitter.EmitPipelineStart(p.executionID, p.pipelineType, p.config)

	go func() {
		results := executeTransformStream(p, transformStage, inputChan, p.executionID)
		for res := range results {
			resultChan <- res
		}
		duration := time.Since(p.startTime)
		p.config.Emitter.EmitPipelineEnd(p.executionID, p.inputCount, p.outputCount, duration)
		close(resultChan)
	}()

	return resultChan, p.config
}

func executeTransformStream[T, R any](sourcePipeline *Pipeline[T], transformStage *ReduceTransformStage[T, R], inputChan <-chan T, executionID string) <-chan R {
	outputChan := make(chan R, sourcePipeline.config.MaxBatchSize)
	// var emitter Emitter
	if len(sourcePipeline.stages) == 0 {
		// No Source stages just apply the transform directly.
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			if err := transformStage.Connect(&wg, inputChan, outputChan, sourcePipeline.config.Emitter); err != nil {
				panic(err)
			}
		}()

		// Handle cleanup in background - transform stage will close output channel
		go func() {
			wg.Wait()
			// Output channel is already closed by the transform stage
		}()

		return outputChan
	}

	// Execute source pipeline first to get intermediate results
	var wg sync.WaitGroup

	// Create channels for source pipeline
	sourceChannels := make([]chan T, len(sourcePipeline.stages)+1)
	for i := range sourceChannels {
		sourceChannels[i] = make(chan T, sourcePipeline.config.MaxBatchSize)
	}

	// Start source pipeline stages
	for i, stage := range sourcePipeline.stages {
		wg.Add(1)
		inChan := sourceChannels[i]
		outChan := sourceChannels[i+1]
		var index uint32
		index = uint32(i)
		go func(s Stage[T], in <-chan T, out chan<- T) {
			if err := s.Connect(&wg, in, out, sourcePipeline.config.Emitter, executionID, index); err != nil {
				panic(err)
			}
		}(stage, inChan, outChan)
	}

	// Connect transform stage to source pipeline output
	wg.Add(1)
	go func() {
		if err := transformStage.Connect(&wg, sourceChannels[len(sourceChannels)-1], outputChan, sourcePipeline.config.Emitter); err != nil {
			panic(err)
		}
	}()

	// Feed input to source pipeline
	go func() {
		for event := range inputChan {
			sourceChannels[0] <- event
		}
		close(sourceChannels[0])
	}()

	// Handle cleanup in background - stages will close their own output channels
	go func() {
		wg.Wait()
		// Output channel is already closed by the final stage
	}()

	return outputChan
}

// ExecuteTransform executes a pipeline with type transformation from T to R
func executeTransform[T, R any](sourcePipeline *Pipeline[T], transformStage *ReduceTransformStage[T, R], input []T, executionID string) []R {
	if len(sourcePipeline.stages) == 0 {
		// No source stages, just apply transform directly
		result, _ := transformStage.ProcessBatch(input)
		return result
	}

	// Execute source pipeline first to get intermediate results
	var wg sync.WaitGroup
	var emitter Emitter

	// Create channels for source pipeline
	sourceChannels := make([]chan T, len(sourcePipeline.stages)+1)
	for i := range sourceChannels {
		sourceChannels[i] = make(chan T, sourcePipeline.config.MaxBatchSize)
	}

	// Start source pipeline stages
	for i, stage := range sourcePipeline.stages {
		var index uint32
		index = uint32(i)
		wg.Add(1)
		inChan := sourceChannels[i]
		outChan := sourceChannels[i+1]
		go func(s Stage[T], in <-chan T, out chan<- T) {
			if err := s.Connect(&wg, in, out, emitter, executionID, index); err != nil {
				panic(err)
			}
		}(stage, inChan, outChan)
	}

	// Create output channel for transform stage
	outputChan := make(chan R, sourcePipeline.config.MaxBatchSize)

	// Connect transform stage to source pipeline output
	wg.Add(1)
	go func() {
		if err := transformStage.Connect(&wg, sourceChannels[len(sourceChannels)-1], outputChan, emitter); err != nil {
			panic(err)
		}
	}()

	// Collect results from transform stage
	var results []R
	resultsChan := make(chan []R, 1)

	go func() {
		var collected []R
		for result := range outputChan {
			collected = append(collected, result)
		}
		resultsChan <- collected
	}()

	// Feed input to source pipeline
	go func() {
		for _, event := range input {
			sourceChannels[0] <- event
		}
		close(sourceChannels[0])
	}()

	// Wait for all stages to complete (they handle channel closing)
	wg.Wait()

	// Get the collected results
	results = <-resultsChan

	return results
}
