// Package pipeline provides a small, generic stream processing library. It
// supports common primitives such as Map, Filter, Generate and a Reduce
// transform that can change the element type. The package focuses on
// batched processing, worker pools and simple observability hooks via the
// Emitter interface.
package pipeline

import (
	"strings"
	"sync"
	"time"
)

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

// NewPipeline creates a new generic Pipeline instance. If cfg.Emitter is nil
// a default NoOpEmitter is installed. NewPipeline will also generate an
// ExecutionID when cfg.ExecutionID is not provided.
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

// Map appends a Map stage to the Pipeline. The provided fn is applied to
// each element processed by the stage.
func (p *Pipeline[T]) Map(fn func(T) T) *Pipeline[T] {
	stage := NewMapStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Filter appends a Filter stage to the Pipeline. The provided fn decides
// whether each element is forwarded (true) or dropped (false).
func (p *Pipeline[T]) Filter(fn func(T) bool) *Pipeline[T] {
	stage := NewFilterStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Generate appends a Generate stage which emits the original event plus a
// generated event produced by the provided fn.
func (p *Pipeline[T]) Generate(fn func(T) T) *Pipeline[T] {
	stage := NewGenerateStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Collect appends a Collect stage to the pipeline and returns the stage so
// callers can access collected results via Results(). The Collect stage also
// forwards events downstream to preserve pipeline behavior.
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
