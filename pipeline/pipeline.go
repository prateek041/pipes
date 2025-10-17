package pipeline

import "sync"

type Pipeline struct {
	config Config
	stages []Stage
}

// NewPipeline creates a new Pipeline instance.
func NewPipeline(cfg Config) *Pipeline {
	return &Pipeline{
		config: cfg,
		stages: make([]Stage, 0),
	}
}

// Map adds a MapStage to the pipeline.
func (p *Pipeline) Map(fn func(SimpleEvent) SimpleEvent) *Pipeline {
	stage := NewMapStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

// Filter adds a FilterStage to the pipeline.
func (p *Pipeline) Filter(fn func(SimpleEvent) bool) *Pipeline {
	stage := NewFilterStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

func (p *Pipeline) Reduce(fn func([]SimpleEvent) SimpleEvent) *Pipeline {
	stage := NewReduceStage(p.config, fn)
	p.stages = append(p.stages, stage)
	return p
}

func (p *Pipeline) Collect() *CollectStage {
	stage := NewCollectStage()
	p.stages = append(p.stages, stage)
	return stage
}

// ExecuteWithCollector runs the pipeline and returns the collector for accessing results
func (p *Pipeline) ExecuteWithCollector(input []SimpleEvent) *CollectStage {
	// Find the collector stage
	var collector *CollectStage
	for _, stage := range p.stages {
		if collectStage, ok := stage.(*CollectStage); ok {
			collector = collectStage
			break
		}
	}

	p.Execute(input)
	return collector
}

// Execute runs the pipeline on the provided input.
// RIght now the implementation is SEQUENTIAL to prove chaining works.
func (p *Pipeline) Execute(input []SimpleEvent) {
	if len(p.stages) == 0 {
		return
	}

	var wg sync.WaitGroup
	var emitter Emitter // will be implemented later.

	// Create channels for each connection between stages.
	channels := make([]chan SimpleEvent, len(p.stages)+1)
	for i := range channels {
		channels[i] = make(chan SimpleEvent, p.config.MaxBatchSize)
	}

	// start all concurrent stages and connect them.
	for i, stage := range p.stages {
		wg.Add(1)
		inChan := channels[i]
		outChan := channels[i+1]

		// go func(s Stage) {
		go stage.Connect(&wg, inChan, outChan, emitter)
		// }(stage)

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
			// We can also send these events to another stage if needed.
			// Keeping this step open ended as of now.
		}
	}()

	wg.Wait()

}
