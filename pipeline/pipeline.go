package pipeline

import (
	"strings"
	"sync"
	"time"
)

// Generic Pipeline that can work with any type T
type Pipeline[T any] struct {
	config          Config
	stages          []Stage[T]
	sourceTransform interface{} // Holds transformation info for mixed-type execution

	executionID  string
	pipelineType string
	startTime    time.Time
	inputCount   uint64
	outputCount  uint64
}

// NewPipeline creates a new generic Pipeline instance.
func NewPipeline[T any](cfg Config) *Pipeline[T] {
	if cfg.Emitter == nil {
		cfg.Emitter = &NoOpEmitter{}
	}

	// Generate execution ID if not provided
	if cfg.ExecutionID == "" {
		cfg.ExecutionID = GenerateExecutionID()
	}
	return &Pipeline[T]{
		config:      cfg,
		stages:      make([]Stage[T], 0),
		executionID: cfg.ExecutionID,
	}
}

// Map adds a MapStage to the pipeline.
func (p *Pipeline[T]) Map(fn func(T) T) *Pipeline[T] {
	stage := NewMapStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Filter adds a FilterStage to the pipeline.
func (p *Pipeline[T]) Filter(fn func(T) bool) *Pipeline[T] {
	stage := NewFilterStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Generate adds a GenerateStage to the pipeline.
func (p *Pipeline[T]) Generate(fn func(T) T) *Pipeline[T] {
	stage := NewGenerateStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Collect adds a CollectStage to the pipeline and returns it for accessing results.
func (p *Pipeline[T]) Collect() *CollectStage[T] {
	stage := NewCollectStage[T]()

	if p.sourceTransform != nil {
		// Handle transformation + collection in one step
		return p.collectWithTransform(stage)
	}

	// Normal collection (existing logic)
	p.stages = append(p.stages, stage)
	return stage
}

// collectWithTransform handles collection for transformed pipelines
func (p *Pipeline[T]) collectWithTransform(collectStage *CollectStage[T]) *CollectStage[T] {
	// This is a practical implementation for Phase 2B
	// We'll handle the transformation + chaining at execution time

	// Add the collect stage to the pipeline
	p.stages = append(p.stages, collectStage)
	return collectStage
}

// Execute runs the pipeline on the provided input.
func (p *Pipeline[T]) Execute(inputChan <-chan T) {
	p.startTime = time.Now()
	p.pipelineType = p.generatePipelineType()

	// Emit pipeline start
	p.config.Emitter.EmitPipelineStart(p.executionID, p.pipelineType, p.config)

	defer func() {
		duration := time.Since(p.startTime)
		p.config.Emitter.EmitPipelineEnd(p.executionID, p.inputCount, p.outputCount, duration)
	}()
	p.executeNormal(inputChan)
}

// executeNormal handles regular Pipeline[T] execution
func (p *Pipeline[T]) executeNormal(inputChan <-chan T) {
	if len(p.stages) == 0 {
		return
	}

	var wg sync.WaitGroup

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

		go func(s Stage[T], in <-chan T, out chan<- T) {
			var index uint32
			index = uint32(i)
			if err := s.Connect(&wg, in, out, p.config.Emitter, p.executionID, index); err != nil {
				// TODO: Better error handling
				panic(err)
			}
		}(stage, inChan, outChan)
	}

	go func() {
		for event := range inputChan {
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

func (p *Pipeline[T]) generatePipelineType() string {
	var stageNames []string
	for _, stage := range p.stages {
		stageNames = append(stageNames, stage.Name())
	}
	return strings.Join(stageNames, "->")
}
