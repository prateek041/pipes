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

// Execute runs the pipeline on the provided input.
// RIght now the implementation is SEQUENTIAL to prove chaining works.
func (p *Pipeline) Execute(input []SimpleEvent) []SimpleEvent {
	if len(p.stages) == 0 {
		return input
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

		go stage.Connect(&wg, inChan, outChan, emitter)
	}

	go func() {
		for _, event := range input {
			channels[0] <- event
		}
		close(channels[0]) // once the input events are done, close the channel.
	}()

	// manage pipeline shutdown
	go func() {
		wg.Wait()
		close(channels[len(channels)-1]) // close the final output channel when its all done
	}()

	// collect all results.
	var results []SimpleEvent
	for event := range channels[len(channels)-1] {
		results = append(results, event)
	}

	return results
}
