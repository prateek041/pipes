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

## Docker Demo

To run the complete demo with ClickHouse metrics collection and Grafana dashboard:

```bash
# Start all services (ClickHouse, Grafana, and Pipeline application)
docker-compose up

# Or run in background
docker-compose up -d

# View logs
docker-compose logs -f pipeline

# Stop all services
docker-compose down
```

The demo will:

- Start ClickHouse database on port 18123 (HTTP) and 19000 (Native TCP)
- Start Grafana dashboard on port 3000 (admin/admin)
- Run the pipeline application with metrics collection enabled

Access Grafana at <http://localhost:3000> to view pipeline metrics and performance data.

## Command Line Benchmarking Tool

The pipeline library includes a comprehensive CLI tool for benchmarking different event types and configurations.

### Building the CLI

```bash
# Build the CLI executable
go build ./cmd/main.go

# Or build with a custom name
go build -o pipeline-benchmark ./cmd/main.go
```

### CLI Usage

```bash
# Show help and all available options
./main -help

# Run default benchmarks (both SimpleEvent and MableEvent)
./main

# Run specific event type benchmarks
./main -type simple
./main -type mable
./main -type both

# Custom event counts
./main -counts "100,1000,10000,100000"

# Specify number of workers
./main -workers 8

# Custom output file
./main -output my_benchmark_results.csv

# Run without ClickHouse metrics (useful for standalone testing)
./main -metrics=false

# Combined example: MableEvent benchmarks with custom settings
./main -type mable -counts "1000,10000,100000" -workers 16 -output mable_benchmark.csv -metrics=false
```

### Docker CLI Usage

```bash
# Build the benchmark image
docker build -t pipeline-benchmark .

# Run with default settings (shows help)
docker run pipeline-benchmark

# Run benchmarks with volume mounted for output
docker run -v $(pwd)/results:/app/results pipeline-benchmark ./main -output /app/results/docker_benchmark.csv

# Run specific benchmark configurations
docker run pipeline-benchmark ./main -type simple -counts "1000,10000" -metrics=false

# Full benchmark suite with results saved locally
docker run -v $(pwd)/results:/app/results pipeline-benchmark ./main -type both -counts "10,100,1000,10000,100000,1000000" -output /app/results/full_benchmark.csv
```

### CLI Options Reference

| Option     | Type   | Default                            | Description                                                 |
| ---------- | ------ | ---------------------------------- | ----------------------------------------------------------- |
| `-input`   | string | -                                  | Input file path (optional - generates data if not provided) |
| `-output`  | string | `benchmark_results.csv`            | Output CSV file path for results                            |
| `-counts`  | string | `10,100,1000,10000,100000,1000000` | Comma-separated list of event counts                        |
| `-type`    | string | `both`                             | Event type: `simple`, `mable`, or `both`                    |
| `-workers` | int    | CPU cores                          | Number of worker goroutines                                 |
| `-metrics` | bool   | `true`                             | Enable ClickHouse metrics collection                        |
| `-help`    | bool   | `false`                            | Show help message                                           |

### Output Format

The CLI generates CSV files with the following columns:

- **EventType**: Type of event processed (`SimpleEvent` or `MableEvent`)
- **EventCount**: Number of events in the benchmark run
- **Workers**: Number of worker goroutines used
- **ProcessingTimeMs**: Total processing time in milliseconds
- **ThroughputEventsPerSec**: Events processed per second
- **TotalMemoryBytes**: Total memory allocated during processing
- **AllocatedMemoryBytes**: Memory allocated at the end of processing
- **Timestamp**: When the benchmark was run

### Example Output

```csv
EventType,EventCount,Workers,ProcessingTimeMs,ThroughputEventsPerSec,TotalMemoryBytes,AllocatedMemoryBytes,Timestamp
SimpleEvent,10,14,0.68,14737.26,164272,509344,2025-10-22T19:54:13Z
SimpleEvent,100,14,1.14,87812.35,664512,1175232,2025-10-22T19:54:13Z
MableEvent,10,14,0.12,80053.80,121456,487936,2025-10-22T19:54:13Z
MableEvent,100,14,0.17,586940.57,214688,1110048,2025-10-22T19:54:13Z
```

### Running Tests vs CLI Benchmarks

The project includes two types of benchmarking:

1. **Go Test Benchmarks** - Comprehensive, heavy computational benchmarks with detailed analysis comparing sequential and concurrent processing:

   ```bash
   go test -bench=. -v ./cmd/ -benchtime=1s
   ```

2. **CLI Benchmarks** - Simplified, user-friendly benchmarking for demonstrations:

   ```bash
   ./main -type both -counts "1000,10000,100000"
   ```

The Go test benchmarks include heavy operations like JSON parsing, regex validation, SHA-256 hashing, and provide detailed performance analysis. The CLI benchmarks are lightweight and designed for quick demonstrations and CSV output generation.

## Grafana dashboard

Due to time constraints I wasn't able to fully implement beautiful dashboards that are shareable through
a public URL, hence posting a few screenshots of how they looked on my personal computer
along with sample Queries that can be used to create them.

> [!NOTE]
> One thing is a positive that the sample data is already present in clickhouse therefore
> all the remaining work is on the grafana side. You have to install the clickhouse plugin
> for grafana, and connect to it using following entries.
> Server PORT: 8123
> Name: some-clickhouse-server
> Connection type: HTTP

### See the Executions in the table or timeseries view

```sql
SELECT
  toStartOfMinute(start_time) AS minute,
  count() AS executions
FROM pipeline_metrics.pipeline_executions
GROUP BY minute
ORDER BY minute ASC

```

### Pipeline Success Rate

```sql
SELECT
    count() as total_executions,
    countIf(status = 'completed') as successful,
    countIf(status = 'failed') as failed,
    (countIf(status = 'completed') / count()) * 100 as success_rate
FROM pipeline_metrics.pipeline_executions
WHERE timestamp >= now() - INTERVAL 1 HOUR
```

### See Stage Performance bottlenecks

```sql
SELECT
    stage_name,
    avg(duration_ms) as avg_duration_ms,
    quantile(0.95)(duration_ms) as p95_duration_ms,
    count() as execution_count
FROM pipeline_metrics.stage_metrics
WHERE timestamp >= now() - INTERVAL 1 HOUR
    AND status = 'completed'
GROUP BY stage_name
ORDER BY avg_duration_ms DESC
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

