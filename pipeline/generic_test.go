package pipeline

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// Test data structures for generic testing
type TestEvent struct {
	ID    int
	Value string
	Score float64
}

type NumberEvent struct {
	Num int
}

// TestGenericPipeline_SimpleEvent tests that the generic pipeline works with SimpleEvent
func TestGenericPipeline_SimpleEvent(t *testing.T) {
	t.Log("Testing generic pipeline with SimpleEvent")

	mapFn := func(e SimpleEvent) SimpleEvent {
		e.Value = e.Value * 2.0
		e.Data = "mapped-" + e.Data
		return e
	}

	filterFn := func(e SimpleEvent) bool {
		return e.ID%2 == 0
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       3,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[SimpleEvent](cfg).
		Map(mapFn).
		Filter(filterFn)
	collector := p.Collect()

	input := []SimpleEvent{
		{ID: 1, Data: "A", Value: 1.0},
		{ID: 2, Data: "B", Value: 2.0},
		{ID: 3, Data: "C", Value: 3.0},
		{ID: 4, Data: "D", Value: 4.0},
	}

	p.Execute(input)
	results := collector.Results()

	// Should have 2 results (ID 2 and 4)
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Sort results by ID for consistent testing
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	// Check first result (ID: 2)
	if results[0].ID != 2 || results[0].Value != 4.0 || results[0].Data != "mapped-B" {
		t.Errorf("First result incorrect: %+v", results[0])
	}

	// Check second result (ID: 4)
	if results[1].ID != 4 || results[1].Value != 8.0 || results[1].Data != "mapped-D" {
		t.Errorf("Second result incorrect: %+v", results[1])
	}

	t.Log("Generic pipeline with SimpleEvent test passed!")
}

// TestGenericPipeline_CustomType tests the generic pipeline with a custom event type
func TestGenericPipeline_CustomType(t *testing.T) {
	t.Log("Testing generic pipeline with custom TestEvent type")

	mapFn := func(e TestEvent) TestEvent {
		e.Score = e.Score * 1.5
		e.Value = "processed-" + e.Value
		return e
	}

	filterFn := func(e TestEvent) bool {
		return e.Score > 10.0
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       2,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[TestEvent](cfg).
		Map(mapFn).
		Filter(filterFn)
	collector := p.Collect()

	input := []TestEvent{
		{ID: 1, Value: "alpha", Score: 5.0},
		{ID: 2, Value: "beta", Score: 10.0},
		{ID: 3, Value: "gamma", Score: 15.0},
		{ID: 4, Value: "delta", Score: 20.0},
	}

	p.Execute(input)
	results := collector.Results()

	// After mapping: scores become [7.5, 15.0, 22.5, 30.0]
	// After filtering: only scores > 10.0 survive -> [15.0, 22.5, 30.0] -> 3 results
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Verify all results have score > 10.0 and are properly processed
	for _, result := range results {
		if result.Score <= 10.0 {
			t.Errorf("Found result with score %.2f, should be > 10.0", result.Score)
		}
		if result.Value[:10] != "processed-" {
			t.Errorf("Value not properly processed: %s", result.Value)
		}
	}

	t.Log("Generic pipeline with custom type test passed!")
}

// TestGenericPipeline_Generate tests the Generate stage
func TestGenericPipeline_Generate(t *testing.T) {
	t.Log("Testing Generate stage")

	generateFn := func(e NumberEvent) NumberEvent {
		return NumberEvent{Num: e.Num * 10} // Generate event with 10x the original number
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       2,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[NumberEvent](cfg).Generate(generateFn)
	collector := p.Collect()

	input := []NumberEvent{
		{Num: 1},
		{Num: 2},
		{Num: 3},
	}

	p.Execute(input)
	results := collector.Results()

	// Should have 6 results: [1, 10, 2, 20, 3, 30] (original + generated for each)
	if len(results) != 6 {
		t.Fatalf("Expected 6 results (3 original + 3 generated), got %d", len(results))
	}

	// Sort results for easier verification
	sort.Slice(results, func(i, j int) bool {
		return results[i].Num < results[j].Num
	})

	expected := []int{1, 2, 3, 10, 20, 30}
	for i, result := range results {
		if result.Num != expected[i] {
			t.Errorf("Result %d: expected %d, got %d", i, expected[i], result.Num)
		}
	}

	t.Log("Generate stage test passed!")
}

// TestGenericPipeline_GenerateWithChain tests Generate in a pipeline chain
func TestGenericPipeline_GenerateWithChain(t *testing.T) {
	t.Log("Testing Generate stage in pipeline chain")

	generateFn := func(e NumberEvent) NumberEvent {
		return NumberEvent{Num: e.Num + 100} // Generate by adding 100
	}

	filterFn := func(e NumberEvent) bool {
		return e.Num < 50 // Keep only numbers < 50
	}

	mapFn := func(e NumberEvent) NumberEvent {
		e.Num = e.Num * 2
		return e
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       3,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[NumberEvent](cfg).
		Generate(generateFn). // [1, 101, 2, 102, 3, 103]
		Filter(filterFn).     // [1, 2, 3] (filter out 101, 102, 103)
		Map(mapFn)            // [2, 4, 6]
	collector := p.Collect()

	input := []NumberEvent{
		{Num: 1},
		{Num: 2},
		{Num: 3},
	}

	p.Execute(input)
	results := collector.Results()

	// Should have 3 results: [2, 4, 6]
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Sort results for easier verification
	sort.Slice(results, func(i, j int) bool {
		return results[i].Num < results[j].Num
	})

	expected := []int{2, 4, 6}
	for i, result := range results {
		if result.Num != expected[i] {
			t.Errorf("Result %d: expected %d, got %d", i, expected[i], result.Num)
		}
	}

	t.Log("Generate with pipeline chain test passed!")
}

// TestGenericPipeline_Reduce tests the Reduce stage
func TestGenericPipeline_Reduce(t *testing.T) {
	t.Log("Testing Reduce stage")

	reduceFn := func(batch []NumberEvent) NumberEvent {
		sum := 0
		for _, e := range batch {
			sum += e.Num
		}
		return NumberEvent{Num: sum}
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       2, // Will create batches of size 2
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[NumberEvent](cfg).Reduce(reduceFn)
	collector := p.Collect()

	input := []NumberEvent{
		{Num: 1},
		{Num: 2},
		{Num: 3},
		{Num: 4},
		{Num: 5},
	}

	p.Execute(input)
	results := collector.Results()

	// With batch size 2: [1,2] -> 3, [3,4] -> 7, [5] -> 5
	// So we should have 3 results
	if len(results) != 3 {
		t.Fatalf("Expected 3 reduced results, got %d", len(results))
	}

	// Calculate total sum to verify correctness
	totalSum := 0
	for _, result := range results {
		totalSum += result.Num
	}

	expectedSum := 1 + 2 + 3 + 4 + 5 // Original sum should be preserved
	if totalSum != expectedSum {
		t.Errorf("Expected total sum %d, got %d", expectedSum, totalSum)
	}

	t.Log("Reduce stage test passed!")
}

// TestGenericPipeline_ComplexChain tests a complex pipeline with multiple stages
func TestGenericPipeline_ComplexChain(t *testing.T) {
	t.Log("Testing complex generic pipeline chain")

	mapFn := func(e TestEvent) TestEvent {
		e.Score = e.Score * 2.0
		return e
	}

	generateFn := func(e TestEvent) TestEvent {
		return TestEvent{
			ID:    e.ID + 1000,
			Value: "gen-" + e.Value,
			Score: e.Score + 5.0,
		}
	}

	filterFn := func(e TestEvent) bool {
		return e.Score > 15.0
	}

	reduceFn := func(batch []TestEvent) TestEvent {
		totalScore := 0.0
		count := len(batch)
		for _, e := range batch {
			totalScore += e.Score
		}
		return TestEvent{
			ID:    999,
			Value: fmt.Sprintf("reduced-%d-events", count),
			Score: totalScore,
		}
	}

	cfg := Config{
		MaxWorkersPerStage: 3,
		MaxBatchSize:       2,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[TestEvent](cfg).
		Map(mapFn).           // Double scores
		Generate(generateFn). // Generate additional events
		Filter(filterFn).     // Keep only score > 15.0
		Reduce(reduceFn)      // Reduce to aggregated events
	collector := p.Collect()

	input := []TestEvent{
		{ID: 1, Value: "A", Score: 5.0},  // -> 10.0, generates {1001, "gen-A", 15.0}
		{ID: 2, Value: "B", Score: 8.0},  // -> 16.0, generates {1002, "gen-B", 21.0}
		{ID: 3, Value: "C", Score: 10.0}, // -> 20.0, generates {1003, "gen-C", 25.0}
	}

	p.Execute(input)
	results := collector.Results()

	// After map: [10.0, 16.0, 20.0]
	// After generate: [10.0, 15.0, 16.0, 21.0, 20.0, 25.0]
	// After filter (>15.0): [16.0, 21.0, 20.0, 25.0] -> 4 events
	// After reduce (batch size 2): 2 reduced events
	if len(results) < 1 || len(results) > 3 {
		t.Fatalf("Expected 1-3 reduced results, got %d", len(results))
	}

	// Verify that all results are reduced events
	for _, result := range results {
		if result.ID != 999 {
			t.Errorf("Expected reduced event ID 999, got %d", result.ID)
		}
		if result.Score <= 0 {
			t.Errorf("Expected positive score in reduced event, got %.2f", result.Score)
		}
	}

	t.Log("Complex generic pipeline chain test passed!")
}

// TestGenericPipeline_MultipleCollectors tests multiple collectors in generic pipeline
func TestGenericPipeline_MultipleCollectors(t *testing.T) {
	t.Log("Testing multiple collectors in generic pipeline")

	mapFn := func(e NumberEvent) NumberEvent {
		e.Num = e.Num * 10
		return e
	}

	filterFn := func(e NumberEvent) bool {
		return e.Num%20 == 0
	}

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       5,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[NumberEvent](cfg).Map(mapFn)
	collector1 := p.Collect() // Collect after map
	p.Filter(filterFn)
	collector2 := p.Collect() // Collect after filter

	input := []NumberEvent{
		{Num: 1}, // -> 10 (filtered out)
		{Num: 2}, // -> 20 (kept)
		{Num: 3}, // -> 30 (filtered out)
		{Num: 4}, // -> 40 (kept)
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

	// Verify values in first collector
	expectedFirst := []int{10, 20, 30, 40}
	sort.Slice(results1, func(i, j int) bool { return results1[i].Num < results1[j].Num })
	for i, result := range results1 {
		if result.Num != expectedFirst[i] {
			t.Errorf("First collector result %d: expected %d, got %d", i, expectedFirst[i], result.Num)
		}
	}

	// Verify values in second collector
	expectedSecond := []int{20, 40}
	sort.Slice(results2, func(i, j int) bool { return results2[i].Num < results2[j].Num })
	for i, result := range results2 {
		if result.Num != expectedSecond[i] {
			t.Errorf("Second collector result %d: expected %d, got %d", i, expectedSecond[i], result.Num)
		}
	}

	t.Log("Multiple collectors test passed!")
}

// TestGenericPipeline_EmptyInput tests generic pipeline with empty input
func TestGenericPipeline_EmptyInput(t *testing.T) {
	t.Log("Testing generic pipeline with empty input")

	cfg := Config{
		MaxWorkersPerStage: 2,
		MaxBatchSize:       5,
		BatchTimeout:       5 * time.Millisecond,
	}

	p := NewGenericPipeline[NumberEvent](cfg).
		Map(func(e NumberEvent) NumberEvent { e.Num *= 2; return e }).
		Filter(func(e NumberEvent) bool { return true }).
		Generate(func(e NumberEvent) NumberEvent { return NumberEvent{Num: e.Num + 1} })
	collector := p.Collect()

	p.Execute([]NumberEvent{})
	results := collector.Results()

	if len(results) != 0 {
		t.Fatalf("Expected 0 results for empty input, got %d", len(results))
	}

	t.Log("Empty input test passed!")
}
