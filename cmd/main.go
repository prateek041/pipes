package main

import (
	"fmt"

	"github.com/prateek041/pipes/pipeline"
)

func main() {
	fmt.Println("--- Phase 1: Sequential Pipeline Test ---")

	// This function will only keep events with a value greater than 50.0
	filterFn := func(e pipeline.SimpleEvent) bool {
		return e.Value > 50.0
	}

	// This function will modify the Data field and increment the ID
	mapFn := func(e pipeline.SimpleEvent) pipeline.SimpleEvent {
		e.ID = e.ID + 1000
		e.Data = "PROCESSED: " + e.Data
		return e
	}

	// Some sample input data
	inputEvents := []pipeline.SimpleEvent{
		{ID: 1, Data: "Event One", Value: 10.5},
		{ID: 2, Data: "Event Two", Value: 99.9},
		{ID: 3, Data: "Event Three", Value: 75.0},
		{ID: 4, Data: "Event Four", Value: 25.0},
	}
	fmt.Println("Input:", inputEvents)

	// Configure and build the pipeline
	// For the current implementatin the config values don't do anything yet, but we'll set them up.
	cfg := pipeline.Config{
		MaxWorkersPerStage: 1,
		MaxBatchSize:       10,
	}

	p := pipeline.NewPipeline(cfg).
		Filter(filterFn).
		Map(mapFn)

	// Execute the pipeline
	outputEvents := p.Execute(inputEvents)

	// Print the final result
	fmt.Println("Output:", outputEvents)
	fmt.Println("--- Test Complete ---")
}
