package pipeline

import (
	"fmt"
	"testing"
	"time"
)

func TestPipeline_ConcurrentMapFilter(t *testing.T) {
	t.Log("Testing the concurrent pipelines")

	filterFn := func(e SimpleEvent) bool {
		return e.ID%2 == 0 // keep only even Ids
	}

	mapFn := func(e SimpleEvent) SimpleEvent {
		e.Value = float64(e.ID) * 10.0 // change the value
		return e
	}

	cfg := Config{
		MaxWorkersPerStage: 4,                     // Use multiple workers
		MaxBatchSize:       10,                    // Use small batches to ensure batching logic is tested
		BatchTimeout:       10 * time.Millisecond, // Use a short timeout
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
		// All remaining IDs must be even
		if event.ID%2 != 0 {
			t.Errorf("Found an odd ID %d after filtering, which should not happen", event.ID)
		}
		// The value must be correctly calculated by the Map stage
		expectedValue := float64(event.ID) * 10.0
		if event.Value != expectedValue {
			t.Errorf("For ID %d, expected value %.2f, but got %.2f", event.ID, expectedValue, event.Value)
		}
	}
	t.Log("Correctness test passed!")
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

func BenchmarkPipeline_MapFilter(b *testing.B) {
	eventSizes := []int{10, 100, 1000, 10000, 100000, 1000000}

	filterFn := func(e SimpleEvent) bool { return e.ID%2 == 0 }
	mapFn := func(e SimpleEvent) SimpleEvent {
		time.Sleep(20 * time.Microsecond) // Simulate a network call, DB query, or heavy computation
		e.Value = (float64(e.ID) * 3.14159) / 2.71828
		return e
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
				p := NewPipeline(cfg).Filter(filterFn).Map(mapFn)

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
					MaxWorkersPerStage: 8,
					MaxBatchSize:       100,
					BatchTimeout:       5 * time.Millisecond,
				}
				p := NewPipeline(cfg).Filter(filterFn).Map(mapFn)

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
