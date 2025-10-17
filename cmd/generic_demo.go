package main

import (
	"fmt"

	"github.com/prateek041/pipes/pipeline"
)

func genericDemo() {
	fmt.Println("--- Generic Pipeline Demo ---")

	// Test with custom NumberEvent type
	type NumberEvent struct {
		Num int
	}

	cfg := pipeline.Config{
		MaxWorkersPerStage: 4,
		MaxBatchSize:       2,
	}

	// Create a generic pipeline for NumberEvent
	p := pipeline.NewGenericPipeline[NumberEvent](cfg).
		Map(func(e NumberEvent) NumberEvent {
			e.Num = e.Num * 2
			return e
		}).
		Generate(func(e NumberEvent) NumberEvent {
			return NumberEvent{Num: e.Num + 1000} // Generate new event
		}).
		Filter(func(e NumberEvent) bool {
			return e.Num < 100 // Filter out large numbers
		})

	collector := p.Collect()

	// Input data
	input := []NumberEvent{
		{Num: 1},  // -> 2, generates 1002 -> keeps 2
		{Num: 10}, // -> 20, generates 1020 -> keeps 20
		{Num: 50}, // -> 100, generates 1100 -> keeps nothing (100 >= 100)
	}

	fmt.Println("Input:", input)

	p.Execute(input)
	results := collector.Results()

	fmt.Println("Output:", results)
	fmt.Printf("Generated %d events from %d input events\n", len(results), len(input))
	fmt.Println("--- Demo Complete ---")
}
