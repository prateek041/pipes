package pipeline

import (
	"fmt"
	"testing"
	"time"
)

func TestPipeline_ConcurrentMapFilter(t *testing.T) {
	t.Log("Testing the concurrent pipelines (Map/Filter only)")

	filterFn := func(e SimpleEvent) bool {
		return e.ID%2 == 0 // keep only even Ids
	}

	mapFn := func(e SimpleEvent) SimpleEvent {
		e.Value = float64(e.ID) * 10.0 // change the value
		return e
	}

	cfg := Config{
		MaxWorkersPerStage: 4,
		MaxBatchSize:       10,
		BatchTimeout:       10 * time.Millisecond,
	}

	p := NewPipeline(cfg).Filter(filterFn).Map(mapFn)

	var inputEvents []SimpleEvent
	for i := 1; i <= 100; i++ {
		inputEvents = append(inputEvents, SimpleEvent{ID: i})
	}

	results := p.Execute(inputEvents)

	if len(results) != 50 {
		t.Fatalf("Expected 50 results after filtering, but got %d", len(results))
	}

	for _, event := range results {
		if event.ID%2 != 0 {
			t.Errorf("Found an odd ID %d after filtering, which should not happen", event.ID)
		}
		expectedValue := float64(event.ID) * 10.0
		if event.Value != expectedValue {
			t.Errorf("For ID %d, expected value %.2f, but got %.2f", event.ID, expectedValue, event.Value)
		}
	}
	t.Log("Map/Filter correctness test passed!")
}

func TestPipeline_FullChain_Correctness(t *testing.T) {
	t.Log("Testing the full Filter -> Map -> Reduce pipeline for correctness...")

	filterFn := func(e SimpleEvent) bool { return e.ID > 5 }             // Keep IDs > 5
	mapFn := func(e SimpleEvent) SimpleEvent { e.Value = 2.0; return e } // Map all values to 2.0
	reduceFn := func(batch []SimpleEvent) SimpleEvent {
		var sum float64
		for _, e := range batch {
			sum += e.Value
		}
		return SimpleEvent{ID: 999, Value: sum}
	}

	cfg := Config{
		MaxWorkersPerStage: 4,
		MaxBatchSize:       3,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg).Filter(filterFn).Map(mapFn).Reduce(reduceFn)

	// Create input data from 1 to 10.
	// Filter will keep [6, 7, 8, 9, 10] (5 events).
	// Map will turn them into 5 events, each with Value = 2.0.
	// Total sum should be 5 * 2.0 = 10.0
	var inputEvents []SimpleEvent
	for i := 1; i <= 10; i++ {
		inputEvents = append(inputEvents, SimpleEvent{ID: i})
	}

	// Execute
	results := p.Execute(inputEvents)

	// After map, we have 5 events. With a batch size of 3, Reduce will create two batches:
	// Batch 1: [event6, event7, event8] -> Reduce -> {ID:999, Value: 6.0}
	// Batch 2: [event9, event10]       -> Reduce -> {ID:999, Value: 4.0}
	// So we expect 2 result events.
	if len(results) != 2 {
		t.Fatalf("Expected 2 final reduced events, but got %d", len(results))
	}

	var finalSum float64
	for _, res := range results {
		finalSum += res.Value
	}

	if finalSum != 10.0 {
		t.Errorf("Expected final summed value of 10.0, but got %.2f", finalSum)
	}

	t.Log("Full pipeline correctness test passed!")
}

func executeSequential(stages []Stage, input []SimpleEvent) []SimpleEvent {
	currentBatch := input
	var err error
	for _, stage := range stages {
		currentBatch, err = stage.ProcessBatch(currentBatch)
		if err != nil {
			panic(err)
		}
	}
	return currentBatch
}

func BenchmarkPipeline_FullChain(b *testing.B) {
	eventSizes := []int{10, 100, 1000, 10000, 100000, 1000000}

	filterFn := func(e SimpleEvent) bool { return e.ID%2 == 0 }
	mapFn := func(e SimpleEvent) SimpleEvent {
		time.Sleep(20 * time.Microsecond) // Simulate work
		e.Value = (float64(e.ID) * 3.14159) / 2.71828
		return e
	}
	reduceFn := func(batch []SimpleEvent) SimpleEvent {
		var sum float64
		for _, e := range batch {
			sum += e.Value
		}
		// Return a single event. Add a small sleep to simulate aggregation work.
		time.Sleep(5 * time.Microsecond)
		return SimpleEvent{ID: 999, Value: sum}
	}

	for _, size := range eventSizes {
		b.Run(fmt.Sprintf("Events-%d", size), func(b *testing.B) {
			inputEvents := make([]SimpleEvent, size)
			for i := 0; i < size; i++ {
				inputEvents[i] = SimpleEvent{ID: i}
			}

			// Sequential benchmark
			b.Run("Sequential", func(b *testing.B) {
				cfg := Config{}
				// Add Reduce to the sequential pipeline
				p := NewPipeline(cfg).Filter(filterFn).Map(mapFn).Reduce(reduceFn)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					inputCopy := make([]SimpleEvent, len(inputEvents))
					copy(inputCopy, inputEvents)
					_ = executeSequential(p.stages, inputCopy)
				}
			})

			// Concurrent benchmark
			b.Run("Concurrent", func(b *testing.B) {
				cfg := Config{
					MaxWorkersPerStage: 20,
					MaxBatchSize:       100,
					BatchTimeout:       5 * time.Millisecond,
				}
				// Add Reduce to the concurrent pipeline
				p := NewPipeline(cfg).Filter(filterFn).Map(mapFn).Reduce(reduceFn)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					inputCopy := make([]SimpleEvent, len(inputEvents))
					copy(inputCopy, inputEvents)
					_ = p.Execute(inputCopy)
				}
			})
		})
	}
}

