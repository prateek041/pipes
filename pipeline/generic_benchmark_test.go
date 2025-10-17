package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkGenericPipeline_FullChain benchmarks the generic pipeline with different event sizes
func BenchmarkGenericPipeline_FullChain(b *testing.B) {
	eventSizes := []int{10, 100, 1000, 10000, 100000}

	filterFn := func(e NumberEvent) bool { return e.Num%2 == 0 }
	mapFn := func(e NumberEvent) NumberEvent {
		time.Sleep(10 * time.Microsecond) // Simulate work
		e.Num = e.Num * 3
		return e
	}
	reduceFn := func(batch []NumberEvent) NumberEvent {
		sum := 0
		for _, e := range batch {
			sum += e.Num
		}
		time.Sleep(5 * time.Microsecond) // Simulate aggregation work
		return NumberEvent{Num: sum}
	}

	for _, size := range eventSizes {
		b.Run(fmt.Sprintf("Events-%d", size), func(b *testing.B) {
			inputEvents := make([]NumberEvent, size)
			for i := 0; i < size; i++ {
				inputEvents[i] = NumberEvent{Num: i}
			}

			// Sequential benchmark
			b.Run("Sequential", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Sequential processing
					filtered := make([]NumberEvent, 0)
					for _, e := range inputEvents {
						if filterFn(e) {
							filtered = append(filtered, e)
						}
					}

					mapped := make([]NumberEvent, 0)
					for _, e := range filtered {
						mapped = append(mapped, mapFn(e))
					}

					// Simple reduce (all in one batch)
					if len(mapped) > 0 {
						_ = reduceFn(mapped)
					}
				}
			})

			// Concurrent benchmark
			b.Run("Concurrent", func(b *testing.B) {
				cfg := Config{
					MaxWorkersPerStage: 10,
					MaxBatchSize:       100,
					BatchTimeout:       5 * time.Millisecond,
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					p := NewGenericPipeline[NumberEvent](cfg).
						Filter(filterFn).
						Map(mapFn).
						Reduce(reduceFn)
					p.Collect()

					inputCopy := make([]NumberEvent, len(inputEvents))
					copy(inputCopy, inputEvents)
					p.Execute(inputCopy)
				}
			})
		})
	}
}

// BenchmarkGenericPipeline_Generate benchmarks the Generate stage specifically
func BenchmarkGenericPipeline_Generate(b *testing.B) {
	eventSizes := []int{1000, 10000, 100000}

	generateFn := func(e NumberEvent) NumberEvent {
		time.Sleep(5 * time.Microsecond) // Simulate generation work
		return NumberEvent{Num: e.Num + 1000}
	}

	for _, size := range eventSizes {
		b.Run(fmt.Sprintf("Generate-Events-%d", size), func(b *testing.B) {
			inputEvents := make([]NumberEvent, size)
			for i := 0; i < size; i++ {
				inputEvents[i] = NumberEvent{Num: i}
			}

			cfg := Config{
				MaxWorkersPerStage: 8,
				MaxBatchSize:       50,
				BatchTimeout:       5 * time.Millisecond,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := NewGenericPipeline[NumberEvent](cfg).Generate(generateFn)
				collector := p.Collect()

				inputCopy := make([]NumberEvent, len(inputEvents))
				copy(inputCopy, inputEvents)
				p.Execute(inputCopy)

				// Verify we got 2x the input (original + generated)
				results := collector.Results()
				if len(results) != size*2 {
					b.Errorf("Expected %d results, got %d", size*2, len(results))
				}
			}
		})
	}
}

// BenchmarkGenericPipeline_ComplexWorkflow benchmarks a complex workflow with Generate
func BenchmarkGenericPipeline_ComplexWorkflow(b *testing.B) {
	eventSizes := []int{1000, 10000}

	mapFn := func(e TestEvent) TestEvent {
		e.Score = e.Score * 1.5
		return e
	}

	generateFn := func(e TestEvent) TestEvent {
		return TestEvent{
			ID:    e.ID + 10000,
			Value: "gen-" + e.Value,
			Score: e.Score + 50.0,
		}
	}

	filterFn := func(e TestEvent) bool {
		return e.Score > 25.0
	}

	reduceFn := func(batch []TestEvent) TestEvent {
		totalScore := 0.0
		for _, e := range batch {
			totalScore += e.Score
		}
		return TestEvent{
			ID:    999,
			Value: "reduced",
			Score: totalScore,
		}
	}

	for _, size := range eventSizes {
		b.Run(fmt.Sprintf("Complex-Events-%d", size), func(b *testing.B) {
			inputEvents := make([]TestEvent, size)
			for i := 0; i < size; i++ {
				inputEvents[i] = TestEvent{
					ID:    i,
					Value: fmt.Sprintf("event-%d", i),
					Score: float64(i % 100), // Scores 0-99
				}
			}

			cfg := Config{
				MaxWorkersPerStage: 12,
				MaxBatchSize:       80,
				BatchTimeout:       5 * time.Millisecond,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := NewGenericPipeline[TestEvent](cfg).
					Map(mapFn).           // Transform scores
					Generate(generateFn). // Generate additional events
					Filter(filterFn).     // Filter by score
					Reduce(reduceFn)      // Reduce to aggregated results
				collector := p.Collect()

				inputCopy := make([]TestEvent, len(inputEvents))
				copy(inputCopy, inputEvents)
				p.Execute(inputCopy)

				results := collector.Results()
				// Just verify we got some results
				if len(results) == 0 {
					b.Error("Expected some results from complex workflow")
				}
			}
		})
	}
}

// BenchmarkGenericPipeline_TypeComparison compares different types
func BenchmarkGenericPipeline_TypeComparison(b *testing.B) {
	size := 10000

	cfg := Config{
		MaxWorkersPerStage: 8,
		MaxBatchSize:       100,
		BatchTimeout:       5 * time.Millisecond,
	}

	b.Run("SimpleEvent", func(b *testing.B) {
		inputEvents := make([]SimpleEvent, size)
		for i := 0; i < size; i++ {
			inputEvents[i] = SimpleEvent{
				ID:    i,
				Data:  fmt.Sprintf("data-%d", i),
				Value: float64(i),
			}
		}

		mapFn := func(e SimpleEvent) SimpleEvent {
			e.Value = e.Value * 2.0
			return e
		}
		filterFn := func(e SimpleEvent) bool { return e.ID%2 == 0 }

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p := NewGenericPipeline[SimpleEvent](cfg).Map(mapFn).Filter(filterFn)
			p.Collect()
			p.Execute(inputEvents)
		}
	})

	b.Run("NumberEvent", func(b *testing.B) {
		inputEvents := make([]NumberEvent, size)
		for i := 0; i < size; i++ {
			inputEvents[i] = NumberEvent{Num: i}
		}

		mapFn := func(e NumberEvent) NumberEvent {
			e.Num = e.Num * 2
			return e
		}
		filterFn := func(e NumberEvent) bool { return e.Num%2 == 0 }

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p := NewGenericPipeline[NumberEvent](cfg).Map(mapFn).Filter(filterFn)
			p.Collect()
			p.Execute(inputEvents)
		}
	})

	b.Run("TestEvent", func(b *testing.B) {
		inputEvents := make([]TestEvent, size)
		for i := 0; i < size; i++ {
			inputEvents[i] = TestEvent{
				ID:    i,
				Value: fmt.Sprintf("val-%d", i),
				Score: float64(i),
			}
		}

		mapFn := func(e TestEvent) TestEvent {
			e.Score = e.Score * 2.0
			return e
		}
		filterFn := func(e TestEvent) bool { return e.ID%2 == 0 }

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p := NewGenericPipeline[TestEvent](cfg).Map(mapFn).Filter(filterFn)
			p.Collect()
			p.Execute(inputEvents)
		}
	})
}
