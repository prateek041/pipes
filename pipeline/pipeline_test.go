package pipeline

import (
	"fmt"
	"sort"
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
	collector := p.Collect()

	var inputEvents []SimpleEvent
	for i := 1; i <= 100; i++ {
		inputEvents = append(inputEvents, SimpleEvent{ID: i})
	}

	p.Execute(inputEvents)
	results := collector.Results()

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
	collector := p.Collect()

	// Create input data from 1 to 10.
	// Filter will keep [6, 7, 8, 9, 10] (5 events).
	// Map will turn them into 5 events, each with Value = 2.0.
	// Total sum should be 5 * 2.0 = 10.0
	var inputEvents []SimpleEvent
	for i := 1; i <= 10; i++ {
		inputEvents = append(inputEvents, SimpleEvent{ID: i})
	}

	// Execute
	p.Execute(inputEvents)
	results := collector.Results()

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

// Comprehensive Tests for all Pipeline Operations

func TestPipeline_MapOnly(t *testing.T) {
	t.Log("Testing Map stage only")

	mapFn := func(e SimpleEvent) SimpleEvent {
		e.Value = float64(e.ID) * 2.0
		e.Data = fmt.Sprintf("Mapped-%d", e.ID)
		return e
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       5,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg).Map(mapFn)
	collector := p.Collect()

	input := []SimpleEvent{
		{ID: 1, Data: "A", Value: 1.0},
		{ID: 2, Data: "B", Value: 2.0},
		{ID: 3, Data: "C", Value: 3.0},
	}

	p.Execute(input)
	results := collector.Results()

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Sort results by ID for consistent testing
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	for i, result := range results {
		expectedValue := float64(input[i].ID) * 2.0
		expectedData := fmt.Sprintf("Mapped-%d", input[i].ID)

		if result.Value != expectedValue {
			t.Errorf("Event %d: expected value %.2f, got %.2f", result.ID, expectedValue, result.Value)
		}
		if result.Data != expectedData {
			t.Errorf("Event %d: expected data %s, got %s", result.ID, expectedData, result.Data)
		}
	}
	t.Log("Map only test passed!")
}

func TestPipeline_FilterOnly(t *testing.T) {
	t.Log("Testing Filter stage only")

	filterFn := func(e SimpleEvent) bool {
		return e.Value > 5.0
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       5,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg).Filter(filterFn)
	collector := p.Collect()

	input := []SimpleEvent{
		{ID: 1, Value: 3.0},
		{ID: 2, Value: 7.0},
		{ID: 3, Value: 2.0},
		{ID: 4, Value: 10.0},
		{ID: 5, Value: 1.0},
	}

	p.Execute(input)
	results := collector.Results()

	if len(results) != 2 {
		t.Fatalf("Expected 2 results after filtering, got %d", len(results))
	}

	for _, result := range results {
		if result.Value <= 5.0 {
			t.Errorf("Found event with value %.2f, should have been filtered out", result.Value)
		}
	}
	t.Log("Filter only test passed!")
}

func TestPipeline_ReduceOnly(t *testing.T) {
	t.Log("Testing Reduce stage only")

	reduceFn := func(batch []SimpleEvent) SimpleEvent {
		var sum float64
		var count int
		for _, e := range batch {
			sum += e.Value
			count++
		}
		return SimpleEvent{
			ID:    999,
			Data:  fmt.Sprintf("Reduced-%d-events", count),
			Value: sum,
		}
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       3,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg).Reduce(reduceFn)
	collector := p.Collect()

	input := []SimpleEvent{
		{ID: 1, Value: 1.0},
		{ID: 2, Value: 2.0},
		{ID: 3, Value: 3.0},
		{ID: 4, Value: 4.0},
		{ID: 5, Value: 5.0},
	}

	p.Execute(input)
	results := collector.Results()

	// With batch size 3, we should get 2 batches: [1,2,3] and [4,5]
	if len(results) != 2 {
		t.Fatalf("Expected 2 reduced results, got %d", len(results))
	}

	var totalSum float64
	for _, result := range results {
		totalSum += result.Value
	}

	expectedSum := 1.0 + 2.0 + 3.0 + 4.0 + 5.0
	if totalSum != expectedSum {
		t.Errorf("Expected total sum %.2f, got %.2f", expectedSum, totalSum)
	}
	t.Log("Reduce only test passed!")
}

func TestPipeline_CollectOnly(t *testing.T) {
	t.Log("Testing Collect stage only")

	cfg := Config{
		MaxWorkersPerStage: 1,
		MaxBatchSize:       10,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg)
	collector := p.Collect()

	input := []SimpleEvent{
		{ID: 1, Data: "A", Value: 1.0},
		{ID: 2, Data: "B", Value: 2.0},
		{ID: 3, Data: "C", Value: 3.0},
	}

	p.Execute(input)
	results := collector.Results()

	if len(results) != 3 {
		t.Fatalf("Expected 3 results in collector, got %d", len(results))
	}

	// Sort results for consistent testing
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	for i, result := range results {
		if result.ID != input[i].ID || result.Data != input[i].Data || result.Value != input[i].Value {
			t.Errorf("Result %d doesn't match input: expected %+v, got %+v", i, input[i], result)
		}
	}
	t.Log("Collect only test passed!")
}

func TestPipeline_MultipleCollectors(t *testing.T) {
	t.Log("Testing pipeline with multiple collectors")

	mapFn := func(e SimpleEvent) SimpleEvent {
		e.Value = e.Value * 10.0
		return e
	}

	filterFn := func(e SimpleEvent) bool {
		return e.ID%2 == 0
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       5,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg).Map(mapFn)
	collector1 := p.Collect() // Collect after map
	p.Filter(filterFn)
	collector2 := p.Collect() // Collect after filter

	input := []SimpleEvent{
		{ID: 1, Value: 1.0},
		{ID: 2, Value: 2.0},
		{ID: 3, Value: 3.0},
		{ID: 4, Value: 4.0},
	}

	p.Execute(input)

	results1 := collector1.Results()
	results2 := collector2.Results()

	// First collector should have all 4 events (after map)
	if len(results1) != 4 {
		t.Fatalf("Expected 4 results in first collector, got %d", len(results1))
	}

	// Second collector should have only 2 events (after filter)
	if len(results2) != 2 {
		t.Fatalf("Expected 2 results in second collector, got %d", len(results2))
	}

	// Verify values are mapped correctly in first collector
	for _, result := range results1 {
		expectedValue := float64(result.ID) * 10.0
		if result.Value != expectedValue {
			t.Errorf("First collector: expected value %.2f for ID %d, got %.2f", expectedValue, result.ID, result.Value)
		}
	}

	// Verify only even IDs in second collector
	for _, result := range results2 {
		if result.ID%2 != 0 {
			t.Errorf("Second collector: found odd ID %d, should have been filtered", result.ID)
		}
	}
	t.Log("Multiple collectors test passed!")
}

func TestPipeline_ComplexChain(t *testing.T) {
	t.Log("Testing complex pipeline: Map -> Filter -> Map -> Reduce -> Collect")

	mapFn1 := func(e SimpleEvent) SimpleEvent {
		e.Value = e.Value * 2.0
		e.Data = "first-" + e.Data
		return e
	}

	filterFn := func(e SimpleEvent) bool {
		return e.Value > 5.0
	}

	mapFn2 := func(e SimpleEvent) SimpleEvent {
		e.Value = e.Value + 10.0
		e.Data = "second-" + e.Data
		return e
	}

	reduceFn := func(batch []SimpleEvent) SimpleEvent {
		var sum float64
		for _, e := range batch {
			sum += e.Value
		}
		return SimpleEvent{ID: 999, Value: sum, Data: "reduced"}
	}

	cfg := Config{
		MaxWorkersPerStage: 3,
		MaxBatchSize:       2,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg).
		Map(mapFn1).
		Filter(filterFn).
		Map(mapFn2).
		Reduce(reduceFn)
	collector := p.Collect()

	input := []SimpleEvent{
		{ID: 1, Data: "A", Value: 1.0}, // 1*2=2, filtered out (<=5)
		{ID: 2, Data: "B", Value: 3.0}, // 3*2=6, passes filter, 6+10=16
		{ID: 3, Data: "C", Value: 4.0}, // 4*2=8, passes filter, 8+10=18
		{ID: 4, Data: "D", Value: 5.0}, // 5*2=10, passes filter, 10+10=20
	}

	p.Execute(input)
	results := collector.Results()

	// After filtering, we have 3 events. With batch size 2, we get 2 batches: [16,18] and [20]
	if len(results) < 1 || len(results) > 2 {
		t.Fatalf("Expected 1-2 reduced results, got %d", len(results))
	}

	var totalSum float64
	for _, result := range results {
		totalSum += result.Value
		if result.Data != "reduced" {
			t.Errorf("Expected data 'reduced', got %s", result.Data)
		}
	}

	expectedSum := 16.0 + 18.0 + 20.0 // Sum after all transformations
	if totalSum != expectedSum {
		t.Errorf("Expected total sum %.2f, got %.2f", expectedSum, totalSum)
	}
	t.Log("Complex chain test passed!")
}

func TestPipeline_EmptyInput(t *testing.T) {
	t.Log("Testing pipeline with empty input")

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       5,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewPipeline(cfg).
		Map(func(e SimpleEvent) SimpleEvent { return e }).
		Filter(func(e SimpleEvent) bool { return true })
	collector := p.Collect()

	p.Execute([]SimpleEvent{})
	results := collector.Results()

	if len(results) != 0 {
		t.Fatalf("Expected 0 results for empty input, got %d", len(results))
	}
	t.Log("Empty input test passed!")
}

func TestPipeline_LargeInput(t *testing.T) {
	t.Log("Testing pipeline with large input")

	mapFn := func(e SimpleEvent) SimpleEvent {
		e.Value = e.Value * 1.5
		return e
	}

	filterFn := func(e SimpleEvent) bool {
		return e.ID%10 == 0 // Keep every 10th event
	}

	cfg := Config{
		MaxWorkersPerStage: 5,
		MaxBatchSize:       50,
		BatchTimeout:       10 * time.Millisecond,
	}

	p := NewPipeline(cfg).Map(mapFn).Filter(filterFn)
	collector := p.Collect()

	// Create 1000 events
	input := make([]SimpleEvent, 1000)
	for i := 0; i < 1000; i++ {
		input[i] = SimpleEvent{ID: i + 1, Value: float64(i + 1)}
	}

	p.Execute(input)
	results := collector.Results()

	expectedCount := 100 // Every 10th event out of 1000
	if len(results) != expectedCount {
		t.Fatalf("Expected %d results, got %d", expectedCount, len(results))
	}

	// Verify all results are multiples of 10
	for _, result := range results {
		if result.ID%10 != 0 {
			t.Errorf("Found non-multiple of 10: ID %d", result.ID)
		}
		expectedValue := float64(result.ID) * 1.5
		if result.Value != expectedValue {
			t.Errorf("ID %d: expected value %.2f, got %.2f", result.ID, expectedValue, result.Value)
		}
	}
	t.Log("Large input test passed!")
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
				p.Collect() // Add collector to capture results

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					inputCopy := make([]SimpleEvent, len(inputEvents))
					copy(inputCopy, inputEvents)
					p.Execute(inputCopy)
				}
			})
		})
	}
}

// Benchmark different pipeline configurations
func BenchmarkPipeline_CollectorPerformance(b *testing.B) {
	eventSizes := []int{1000, 10000, 100000}

	for _, size := range eventSizes {
		b.Run(fmt.Sprintf("Collector-Events-%d", size), func(b *testing.B) {
			inputEvents := make([]SimpleEvent, size)
			for i := 0; i < size; i++ {
				inputEvents[i] = SimpleEvent{ID: i, Value: float64(i)}
			}

			cfg := Config{
				MaxWorkersPerStage: 10,
				MaxBatchSize:       100,
				BatchTimeout:       5 * time.Millisecond,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := NewPipeline(cfg).
					Map(func(e SimpleEvent) SimpleEvent { e.Value *= 2; return e }).
					Filter(func(e SimpleEvent) bool { return e.ID%2 == 0 })
				collector := p.Collect()

				inputCopy := make([]SimpleEvent, len(inputEvents))
				copy(inputCopy, inputEvents)

				p.Execute(inputCopy)
				_ = collector.Results() // Access results to ensure collection works
			}
		})
	}
}
