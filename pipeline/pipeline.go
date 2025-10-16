package pipeline

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
	// For simplicity right now, we'll pass the entire input as one batch
	// and sequentially process it through the stages.

	currentBatch := input
	var err error

	for _, stage := range p.stages {
		// Stage.ProcessBatch is called directly on the full batch.
		// Output of one stage becomes the input of the next.
		currentBatch, err = stage.ProcessBatch(currentBatch)
		if err != nil {
			panic(err)
		}
	}

	return currentBatch
}
