package main

import (
	"fmt"
	"github.com/prateek041/pipes/pipeline"
)

func main() {

	cfg := pipeline.Config{
		MaxWorkersPerStage: 5,
		MaxBatchSize:       10,
	}
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

	avgFunction := func(batch []pipeline.SimpleEvent) float64 {
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

	// Some sample input data
	inputEvents := []pipeline.SimpleEvent{
		{ID: 1, Data: "Event One", Value: 10.5},
		{ID: 2, Data: "Event Two", Value: 99.9},
		{ID: 3, Data: "Event Three", Value: 75.0},
		{ID: 4, Data: "Event Four", Value: 25.0},
	}
	fmt.Println("Input:", inputEvents)

	fp := pipeline.NewPipeline[pipeline.SimpleEvent](cfg).
		Filter(filterFn)

	filterCollector := fp.Collect()

	fp.Map(mapFn)

	mapCollector := fp.Collect()

	ch := make(chan pipeline.SimpleEvent)

	secondChan := pipeline.ReduceTransformAndStream(fp, avgFunction, ch)

	finalPipeline := pipeline.NewPipeline[float64](cfg).Map(func(number float64) float64 {
		return number + 1000
	})

	finalCollector := finalPipeline.Collect()

	// Send events in a goroutine to avoid deadlock
	go func() {
		for _, ev := range inputEvents {
			ch <- ev
		}
		close(ch) // Important: close the channel when done sending
	}()

	// Execute the final pipeline with the output from the first pipeline
	fmt.Println("Executing pipeline")
	finalPipeline.Execute(secondChan)

	// Print the final result
	fmt.Println("Filter Result:", filterCollector.Results())
	fmt.Println("Map Result:", mapCollector.Results())
	fmt.Println("Check This:", finalCollector.Results())
	fmt.Println("--- Test Complete ---")

	// Run generic demo with custom types
	// runGenericDemo()
}

// func runGenericDemo() {
// 	fmt.Println("\n=== Generic Pipeline Demo ===")
//
// 	type UserEvent struct {
// 		ID       int
// 		Username string
// 		Score    float64
// 	}
//
// 	// Create pipeline with UserEvent type
// 	cfg := pipeline.Config{
// 		MaxWorkersPerStage: 2,
// 		MaxBatchSize:       3,
// 		BatchTimeout:       5 * time.Millisecond,
// 	}
//
// 	// Define transformations
// 	mapFn := func(e UserEvent) UserEvent {
// 		e.Score = e.Score * 1.5 // Boost scores
// 		e.Username = "user_" + e.Username
// 		return e
// 	}
//
// 	filterFn := func(e UserEvent) bool {
// 		return e.Score > 7.0 // Keep high scores only
// 	}
//
// 	generateFn := func(e UserEvent) UserEvent {
// 		return UserEvent{
// 			ID:       e.ID + 1000,
// 			Username: "bonus_" + e.Username,
// 			Score:    e.Score + 10.0,
// 		}
// 	}
//
// 	// Create a user event type pipeline.
// 	p := pipeline.NewPipeline[UserEvent](cfg).Filter(filterFn)
//
// 	inputCheck := []int{2, 4, 6, 8, 10}
//
// 	avgFn := func(batch []UserEvent) float64 {
// 		if len(batch) == 0 {
// 			return 0.0
// 		}
// 		sum := 0.0
// 		for _, v := range batch {
// 			sum += v.Score
// 		}
// 		return float64(sum) / float64(len(batch))
// 	}
//
// 	input := []UserEvent{
// 		{ID: 1, Username: "alice", Score: 4.0},
// 		{ID: 2, Username: "bob", Score: 6.0},
// 		{ID: 3, Username: "charlie", Score: 8.0},
// 	}
//
// 	resultsOne := pipeline.ReduceTransformAndStream(p, avgFn, input)
//
// 	// Build pipeline
// 	// p := pipeline.NewPipeline[UserEvent](cfg).
// 	// 	Map(mapFn).
// 	// 	Generate(generateFn).
// 	// 	Filter(filterFn)
// 	// collector := p.Collect()
//
// 	// Input data
//
// 	fmt.Printf("Input: %+v\n", input)
//
// 	// Execute pipeline
// 	p.Execute(input)
// 	results := collector.Results()
//
// 	fmt.Printf("Output: %+v\n", results)
// 	fmt.Println("Generic demo complete!")
// }
