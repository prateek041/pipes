# Pipes - Generic Stream Processing Pipeline

A lightweight, high-performance Go library for building concurrent data processing pipelines with built-in observability and type safety.

## Features

- **Generic Type Safety**: Work with any data type using Go generics
- **Concurrent Processing**: Built-in worker pools and batching for optimal throughput
- **Stream Primitives**: Map, Filter, Generate, Collect, and Reduce operations
- **Type Transformation**: Reduce operations that can change element types
- **Observability**: Built-in metrics collection with ClickHouse integration
- **Backpressure Handling**: Automatic batching with configurable timeouts
- **Zero Dependencies**: Core library has no external dependencies (observability optional)

## Installation

```bash
go get github.com/prateek041/pipes
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    
    "github.com/prateek041/pipes/pipeline"
)

func main() {
    // Configure pipeline
    config := pipeline.Config{
        MaxWorkersPerStage: 4,
        MaxBatchSize:      100,
        BatchTimeout:      100 * time.Millisecond,
    }
    
    // Create pipeline
    p := pipeline.NewPipeline[int](config).
        Filter(func(x int) bool { return x%2 == 0 }).  // Keep even numbers
        Map(func(x int) int { return x * x }).          // Square them
        Collect()                                       // Collect results
    
    // Create input channel and send data
    input := make(chan int, 10)
    go func() {
        defer close(input)
        for i := 1; i <= 10; i++ {
            input <- i
        }
    }()
    
    // Execute pipeline
    p.Execute(input)
    
    // Get results
    results := p.Results()
    fmt.Println("Results:", results) // [4, 16, 36, 64, 100]
}
```

## Core Concepts

### Pipeline Stages

The library provides several built-in stage types:

- **Map**: Transform each element `T -> T`
- **Filter**: Keep elements that match a predicate
- **Generate**: Emit original + generated element for each input
- **Collect**: Buffer all elements for later retrieval
- **Reduce**: Aggregate batches and optionally transform types `[]T -> R`

### Configuration

```go
config := pipeline.Config{
    MaxWorkersPerStage: 8,              // Worker goroutines per stage
    MaxBatchSize:      1000,            // Max items per batch
    BatchTimeout:      50 * time.Millisecond, // Max wait time for batch
    Emitter:           emitter,         // Optional: metrics collection
    ExecutionID:       "custom-id",     // Optional: execution tracking
}
```

## Usage Examples

### Basic Pipeline

```go
// Process user events
type UserEvent struct {
    UserID int
    Action string
    Value  float64
}

config := pipeline.Config{
    MaxWorkersPerStage: 4,
    MaxBatchSize:      100,
    BatchTimeout:      100 * time.Millisecond,
}

pipe := pipeline.NewPipeline[UserEvent](config).
    Filter(func(e UserEvent) bool {
        return e.Action == "purchase" && e.Value > 10.0
    }).
    Map(func(e UserEvent) UserEvent {
        e.Value = e.Value * 1.1 // Add 10% tax
        return e
    })

collector := pipe.Collect()
// Execute with your data using pipeline.Execute().
```

### Type-Changing Reduce Pipeline

```go
// Transform individual events into aggregated results
type OrderEvent struct {
    OrderID string
    Amount  float64
    UserID  int
}

type DailySummary struct {
    TotalOrders int
    TotalAmount float64
    UniqueUsers int
}

// Create input channel
	input := make(chan OrderEvent, 1000)

	// Build pipeline with type transformation
	firstPipeline := pipeline.NewPipeline[OrderEvent](config).
		Filter(func(e OrderEvent) bool { return e.Amount > 0 }).
		Map(func(e OrderEvent) OrderEvent {
			// Apply business logic
			return e
		})

	outputChan, cfg := pipeline.ReduceTransformAndStream(firstPipeline, func(batch []OrderEvent) DailySummary {
		// Aggregate batch into summary
		summary := DailySummary{TotalOrders: len(batch)}
		userSet := make(map[int]bool)

		for _, event := range batch {
			summary.TotalAmount += event.Amount
			userSet[event.UserID] = true
		}
		summary.UniqueUsers = len(userSet)

		return summary
	}, input)

	// Consume aggregated results
	for summary := range outputChan {
		fmt.Printf("Summary: %+v\n", summary)
	}
```

### Pipeline Chaining

```go
// Chain multiple pipelines together
input := make(chan RawEvent, 1000)

// First pipeline: Raw events -> Validated events
firstPipe := pipeline.NewPipeline[RawEvent](config).
    Filter(isValid).
    Map(normalize)

// reducerFunc converts RawEvents into ValidatedEvents.
midChannel, config := ReduceTransformAndStream(firstPipe, reducerFunc, input)

// Second pipeline: Validated events -> Enriched events
secondPipe := pipeline.NewPipeline[ValidatedEvent](config).
    Map(enrichWithUserData).
    Map(addGeoLocation)

finalCollector := secondPipe.Collect()
secondPipe.Execute(midChannel)
results := finalCollector.Results()
```

### Generate Stage

```go
// Generate additional events from each input
pipeline.NewPipeline[UserAction](config).
    Generate(func(action UserAction) UserAction {
        // Create audit log entry
        return UserAction{
            UserID: action.UserID,
            Action: "audit_" + action.Action,
            Timestamp: time.Now().Unix(),
        }
    }).
    // Now processing both original and audit events
    Map(processAction)
```

## Observability

Enable metrics collection to monitor pipeline performance:

```go
// Configure ClickHouse observability
observabilityConfig := pipeline.ObservabilityConfig{
    Enabled:       true,
    DatabaseURL:   "tcp://localhost:9000",
    Database:      "pipeline_metrics",
    BufferSize:    100,
    FlushInterval: 5 * time.Second,
    Debug:         true,
}

emitter, err := pipeline.NewClickHouseEmitter(observabilityConfig)
if err != nil {
    panic(err)
}
defer emitter.Close()

config := pipeline.Config{
    MaxWorkersPerStage: 8,
    MaxBatchSize:      100,
    BatchTimeout:      10 * time.Millisecond,
    Emitter:           emitter, // Enable metrics
}
```

### Collected Metrics

The library automatically collects:

- **Pipeline Metrics**: Execution time, throughput, input/output counts
- **Stage Metrics**: Per-stage performance, worker utilization
- **Batch Metrics**: Batch sizes, processing times per worker
- **Error Metrics**: Error tracking with context

## Performance

The library is designed for high-throughput scenarios:

- **Batched Processing**: Reduces goroutine overhead
- **Worker Pools**: Configurable concurrency per stage  
- **Non-blocking**: Stages process independently with buffered channels
- **Memory Efficient**: Streaming design with configurable batch sizes

### Benchmarks

From `cmd/main_test.go`:

```
Events   │ Sequential        │ Concurrent+Pipeline   │ Speedup
---------|-------------------|----------------------|--------
1,000    │ 2.1ms (476k/s)   │ 0.8ms (1.25M/s)     │ 2.6x
10,000   │ 21ms (476k/s)    │ 6ms (1.67M/s)       │ 3.5x
100,000  │ 215ms (465k/s)   │ 58ms (1.72M/s)      │ 3.7x
```

## Architecture

```
Input → [Filter] → [Map] → [Generate] → [Collect] → Output
         ↓          ↓        ↓           ↓
      Workers    Workers   Workers    Single
      (Batch)    (Batch)   (Batch)   (Stream)
         ↓          ↓        ↓           ↓
      Metrics    Metrics   Metrics    Results
```

Each stage:
1. Receives elements via input channel
2. Batches elements (size + timeout based)
3. Processes batches concurrently with worker pool
4. Emits results to next stage
5. Reports metrics to configured emitter

## Error Handling

- **Graceful Degradation**: Stages continue processing on individual errors
- **Metrics Integration**: Errors are captured and reported via emitter
- **Channel Management**: Proper cleanup prevents goroutine leaks

## Development

```bash
# Clone repository
git clone https://github.com/prateek041/pipes
cd pipes

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./cmd

# Lint code
go vet ./...
```
## Roadmap

- [ ] Additional stage types (If, GroupBy, Sort, Distinct)
- [ ] More observability backends (Prometheus)
- [ ] Pipeline visualization and debugging tools
- [ ] Persistent state management for long-running pipelines
- [ ] Built-in retry and circuit breaker patterns