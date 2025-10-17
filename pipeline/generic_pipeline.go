package pipeline

import "sync"

// Generic Pipeline that can work with any type T
type GenericPipeline[T any] struct {
	config Config
	stages []GenericStage[T]
}

// NewGenericPipeline creates a new generic Pipeline instance.
func NewGenericPipeline[T any](cfg Config) *GenericPipeline[T] {
	return &GenericPipeline[T]{
		config: cfg,
		stages: make([]GenericStage[T], 0),
	}
}

// Map adds a MapStage to the pipeline.
func (p *GenericPipeline[T]) Map(fn func(T) T) *GenericPipeline[T] {
	stage := NewGenericMapStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Filter adds a FilterStage to the pipeline.
func (p *GenericPipeline[T]) Filter(fn func(T) bool) *GenericPipeline[T] {
	stage := NewGenericFilterStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Reduce adds a ReduceStage to the pipeline.
func (p *GenericPipeline[T]) Reduce(fn func([]T) T) *GenericPipeline[T] {
	stage := NewGenericReduceStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Generate adds a GenerateStage to the pipeline.
func (p *GenericPipeline[T]) Generate(fn func(T) T) *GenericPipeline[T] {
	stage := NewGenericGenerateStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Collect adds a CollectStage to the pipeline and returns it for accessing results.
func (p *GenericPipeline[T]) Collect() *GenericCollectStage[T] {
	stage := NewGenericCollectStage[T]()
	p.stages = append(p.stages, stage)
	return stage
}

// Execute runs the pipeline on the provided input.
func (p *GenericPipeline[T]) Execute(input []T) {
	if len(p.stages) == 0 {
		return
	}

	var wg sync.WaitGroup
	var emitter Emitter // will be implemented later.

	// Create channels for each connection between stages.
	channels := make([]chan T, len(p.stages)+1)
	for i := range channels {
		channels[i] = make(chan T, p.config.MaxBatchSize)
	}

	// start all concurrent stages and connect them.
	for i, stage := range p.stages {
		wg.Add(1)
		inChan := channels[i]
		outChan := channels[i+1]

		go stage.Connect(&wg, inChan, outChan, emitter)
	}

	go func() {
		for _, event := range input {
			channels[0] <- event
		}
		close(channels[0]) // once the input events are done, close the channel.
	}()

	// Start a goroutine to drain the final channel
	finalChan := channels[len(channels)-1]
	go func() {
		for range finalChan {
			// Drain any remaining events to prevent goroutine leaks
		}
	}()

	wg.Wait()
}
