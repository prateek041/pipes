package main

import (
	"fmt"
	"github.com/prateek041/pipes/pipeline"
)

func filterFn(e pipeline.SimpleEvent) bool {
	return e.Value > 50.0
}

func mapFn(e pipeline.SimpleEvent) pipeline.SimpleEvent {
	e.ID = e.ID + 1000
	e.Data = "PROCESSED: " + e.Data
	return e
}

func avgFunction(batch []pipeline.SimpleEvent) float64 {
	if len(batch) == 0 {
		return 0.0
	}
	sum := 0.0
	for _, v := range batch {
		fmt.Println("Avaraging this", v)
		sum += v.Value
	}
	fmt.Println("Result of this", sum)
	return sum
}

func main() {

	cfg := pipeline.Config{
		MaxWorkersPerStage: 5,
		MaxBatchSize:       10,
	}

	// Some sample input data
	inputEvents := []pipeline.SimpleEvent{
		{ID: 1, Data: "Event One", Value: 10.5},
		{ID: 2, Data: "Event Two", Value: 99.9},
		{ID: 3, Data: "Event Three", Value: 75.0},
		{ID: 4, Data: "Event Four", Value: 25.0},
	}

	ch := make(chan pipeline.SimpleEvent)

	// Filter pipeline.
	fp := pipeline.NewPipeline[pipeline.SimpleEvent](cfg).
		Filter(filterFn)

	fp.Map(mapFn)

	secondChan := pipeline.ReduceTransformAndStream(fp, avgFunction, ch)
	// Pipeline continuing after reduce.
	finalPipeline := pipeline.NewPipeline[float64](cfg).Map(func(number float64) float64 {
		return number + 1000
	})

	filterCollector := fp.Collect()
	mapCollector := fp.Collect()
	finalCollector := finalPipeline.Collect()

	// Send events in a goroutine to avoid deadlock
	go func() {
		for _, ev := range inputEvents {
			ch <- ev
		}
		close(ch) // Important: close the channel when done sending
	}()

	// Execute the final pipeline with the output from the first pipeline
	finalPipeline.Execute(secondChan)

	// Print the final result
	fmt.Println("Filter Result:", filterCollector.Results())
	fmt.Println("Map Result:", mapCollector.Results())
	fmt.Println("Check This:", finalCollector.Results())
}
