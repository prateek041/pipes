// Package main demonstrates usage examples and a small benchmark runner for
// the pipeline package. It contains helper functions that construct test data,
// run concurrent reduce pipelines, and perform CPU-bound work used by the
// examples. The comments in this file follow Go documentation conventions so
// they can be rendered by godoc.
package main

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/prateek041/pipes/pipeline"
)

// main is the entry point for the example/benchmark runner. It configures
// observability, constructs test data, wires up a pipeline that performs
// filtering, mapping and reducing, and then prints the final results.
// BenchmarkResult represents the results of a pipeline benchmark run
type BenchmarkResult struct {
	EventType      string        `json:"event_type"`
	EventCount     int           `json:"event_count"`
	Workers        int           `json:"workers"`
	ProcessingTime time.Duration `json:"processing_time_ns"`
	Throughput     float64       `json:"throughput_events_per_sec"`
	TotalMemory    uint64        `json:"total_memory_bytes"`
	AllocatedMem   uint64        `json:"allocated_memory_bytes"`
	Timestamp      time.Time     `json:"timestamp"`
}

// CLI flags
var (
	inputFile     = flag.String("input", "", "Input file path (optional - will generate data if not provided)")
	outputFile    = flag.String("output", "benchmark_results.csv", "Output file path for benchmark results")
	eventCounts   = flag.String("counts", "10,100,1000,10000,100000,1000000", "Comma-separated list of event counts to benchmark")
	eventType     = flag.String("type", "both", "Event type to benchmark: 'simple', 'mable', or 'both'")
	workers       = flag.Int("workers", runtime.NumCPU(), "Number of worker goroutines")
	enableMetrics = flag.Bool("metrics", true, "Enable ClickHouse metrics collection")
	help          = flag.Bool("help", false, "Show help message")
)

func main() {
	flag.Parse()

	if *help {
		printUsage()
		return
	}

	// Parse event counts
	counts, err := parseEventCounts(*eventCounts)
	if err != nil {
		fmt.Printf("Error parsing event counts: %v\n", err)
		os.Exit(1)
	}

	// Setup observability if enabled
	var emitter pipeline.Emitter
	if *enableMetrics {
		clickhouseEmitter := setupObservability()
		if clickhouseEmitter != nil {
			emitter = clickhouseEmitter
			defer clickhouseEmitter.Close()
		} else {
			emitter = &pipeline.NoOpEmitter{}
		}
	} else {
		emitter = &pipeline.NoOpEmitter{}
	}

	fmt.Printf("Starting Pipeline Benchmark Suite\n")
	fmt.Printf("Workers: %d\n", *workers)
	fmt.Printf("Event Counts: %v\n", counts)
	fmt.Printf("Event Types: %s\n", *eventType)
	fmt.Printf("Output File: %s\n", *outputFile)
	fmt.Printf("Metrics Enabled: %t\n\n", *enableMetrics)

	var allResults []BenchmarkResult

	// Run benchmarks based on event type
	switch *eventType {
	case "simple":
		allResults = append(allResults, runSimpleEventBenchmarks(counts, *workers, emitter)...)
	case "mable":
		allResults = append(allResults, runMableEventBenchmarks(counts, *workers, emitter)...)
	case "both":
		allResults = append(allResults, runSimpleEventBenchmarks(counts, *workers, emitter)...)
		allResults = append(allResults, runMableEventBenchmarks(counts, *workers, emitter)...)
	default:
		fmt.Printf("Invalid event type: %s. Use 'simple', 'mable', or 'both'\n", *eventType)
		os.Exit(1)
	}

	// Save results to file
	if err := saveBenchmarkResults(allResults, *outputFile); err != nil {
		fmt.Printf("Error saving results: %v\n", err)
		os.Exit(1)
	}

	// Print summary
	printBenchmarkSummary(allResults)
	fmt.Printf("\nBenchmark results saved to: %s\n", *outputFile)
}

func printUsage() {
	fmt.Printf(`
Pipeline Benchmark CLI Tool

USAGE:
    %s [OPTIONS]

OPTIONS:
    -input string
        Input file path (optional - will generate data if not provided)
    
    -output string
        Output file path for benchmark results (default: "benchmark_results.csv")
    
    -counts string
        Comma-separated list of event counts to benchmark (default: "10,100,1000,10000,100000,1000000")
    
    -type string
        Event type to benchmark: 'simple', 'mable', or 'both' (default: "both")
    
    -workers int
        Number of worker goroutines (default: number of CPU cores)
    
    -metrics
        Enable ClickHouse metrics collection (default: true)
    
    -help
        Show this help message

EXAMPLES:
    # Run all benchmarks with default settings
    %s

    # Run only simple event benchmarks with custom event counts
    %s -type simple -counts "100,1000,10000"

    # Run benchmarks with specific output file and 8 workers
    %s -output my_results.csv -workers 8

    # Run without metrics collection
    %s -metrics=false

DOCKER USAGE:
    # Build the image
    docker build -t pipeline-benchmark .

    # Run with output volume mounted
    docker run -v $(pwd)/results:/app/results pipeline-benchmark -output /app/results/benchmark.csv

    # Run specific benchmark
    docker run pipeline-benchmark -type mable -counts "1000,10000"
`, os.Args[0], os.Args[0], os.Args[0], os.Args[0], os.Args[0])
}

// parseEventCounts parses comma-separated string of event counts
func parseEventCounts(countsStr string) ([]int, error) {
	parts := strings.Split(countsStr, ",")
	counts := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		count, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid event count '%s': %w", part, err)
		}
		if count <= 0 {
			return nil, fmt.Errorf("event count must be positive: %d", count)
		}
		counts = append(counts, count)
	}

	if len(counts) == 0 {
		return nil, fmt.Errorf("no valid event counts provided")
	}

	return counts, nil
}

// setupObservability configures ClickHouse metrics collection
func setupObservability() *pipeline.ClickHouseEmitter {
	databaseURL := os.Getenv("CLICKHOUSE_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "localhost:19000" // Use the mapped port for local development
	}

	observabilityConfig := pipeline.ObservabilityConfig{
		Enabled:       true,
		DatabaseURL:   databaseURL,
		Database:      "pipeline_metrics",
		BufferSize:    0,
		FlushInterval: 3 * time.Second,
		Debug:         false, // Reduce noise in CLI mode
	}

	emitter, err := pipeline.NewClickHouseEmitter(observabilityConfig)
	if err != nil {
		fmt.Printf("Warning: Failed to setup ClickHouse metrics: %v\n", err)
		fmt.Printf("Continuing without metrics collection...\n\n")
		return nil
	}

	fmt.Printf("ClickHouse metrics collection enabled\n\n")
	return emitter
}

// runSimpleEventBenchmarks runs benchmarks with SimpleEvent
func runSimpleEventBenchmarks(counts []int, workers int, emitter pipeline.Emitter) []BenchmarkResult {
	var results []BenchmarkResult

	fmt.Printf("Running SimpleEvent Benchmarks\n")

	for _, count := range counts {
		fmt.Printf("Testing %d events... ", count)

		// Generate test data
		events := GenerateComputeTestData(count)

		// Record memory before
		var memBefore runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memBefore)

		// Run benchmark
		start := time.Now()
		processedResults := ConcurrentReduceProcessing(events, workers, emitter)
		duration := time.Since(start)

		// Record memory after
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		throughput := float64(count) / duration.Seconds()

		result := BenchmarkResult{
			EventType:      "SimpleEvent",
			EventCount:     count,
			Workers:        workers,
			ProcessingTime: duration,
			Throughput:     throughput,
			TotalMemory:    memAfter.TotalAlloc - memBefore.TotalAlloc,
			AllocatedMem:   memAfter.Alloc,
			Timestamp:      time.Now(),
		}

		results = append(results, result)

		fmt.Printf("%s (%.2f events/sec)\n", duration, throughput)
		_ = processedResults // Silence unused variable warning
	}

	fmt.Printf("\n")
	return results
}

// runMableEventBenchmarks runs benchmarks with MableEvent using simplified processing
func runMableEventBenchmarks(counts []int, workers int, emitter pipeline.Emitter) []BenchmarkResult {
	var results []BenchmarkResult

	fmt.Printf("Running MableEvent Benchmarks\n")

	for _, count := range counts {
		fmt.Printf("Testing %d events... ", count)

		// Generate test data - create JSON strings manually for CLI
		events := generateMableJSONTestData(count)

		// Record memory before
		var memBefore runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memBefore)

		// Run benchmark using simplified JSON processing
		start := time.Now()
		processedEvents := processJSONEvents(events, workers)
		duration := time.Since(start)

		// Record memory after
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		throughput := float64(count) / duration.Seconds()

		result := BenchmarkResult{
			EventType:      "MableEvent",
			EventCount:     count,
			Workers:        workers,
			ProcessingTime: duration,
			Throughput:     throughput,
			TotalMemory:    memAfter.TotalAlloc - memBefore.TotalAlloc,
			AllocatedMem:   memAfter.Alloc,
			Timestamp:      time.Now(),
		}

		results = append(results, result)

		fmt.Printf("%s (%.2f events/sec)\n", duration, throughput)
		_ = processedEvents // Silence unused variable warning
	}

	fmt.Printf("\n")
	return results
}

// generateMableJSONTestData creates JSON string events for CLI benchmarking
func generateMableJSONTestData(count int) []string {
	events := make([]string, count)

	for i := 0; i < count; i++ {
		jsonStr := fmt.Sprintf(`{
			"bd": {
				"aid": "aid-%d",
				"bsts": %d,
				"cks": {"_fbp": "fb.1.%d"},
				"iqpar": {},
				"ir": "",
				"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
				"ul": "en-GB"
			},
			"cdas": {
				"cc": ["US"],
				"ct": ["New York"],
				"em": ["user%d@example.com"],
				"fn": ["Customer"],
				"id": ["%d"],
				"ln": ["LastName"],
				"street": ["123 Main St"],
				"zip": ["10001"]
			},
			"eid": "event-%d",
			"en": "Order Completed",
			"sd": {
				"ip4": "192.168.1.%d",
				"sid": "session-%d"
			},
			"esd": {
				"cart": {
					"currency": "USD",
					"total": {
						"price": %d
					}
				},
				"oid": "order-%d"
			},
			"ts": %d
		}`, i, time.Now().Unix(), i, i, i, i, i%255, i, 100+i%1000, i, time.Now().Unix())

		events[i] = jsonStr
	}

	return events
}

// processJSONEvents runs a simple pipeline on JSON string events
func processJSONEvents(events []string, workers int) []string {
	config := pipeline.Config{
		MaxWorkersPerStage: workers,
		MaxBatchSize:       1000,
		BatchTimeout:       100 * time.Millisecond,
	}

	// Create pipeline that processes JSON strings
	pipe := pipeline.NewPipeline[string](config)

	// Filter valid JSON events (simple validation)
	pipe = pipe.Filter(func(jsonStr string) bool {
		return len(jsonStr) > 10 && strings.Contains(jsonStr, "eid")
	})

	// Map to add processing marker
	pipe = pipe.Map(func(jsonStr string) string {
		// Simple transformation - add processed timestamp
		processed := strings.Replace(jsonStr, `"ts":`, fmt.Sprintf(`"processed_ts": %d, "ts":`, time.Now().Unix()), 1)
		return processed
	})

	// Add collect stage
	collectStage := pipe.Collect()

	// Create input channel
	inputChan := make(chan string, len(events))

	// Send events to input channel
	go func() {
		defer close(inputChan)
		for _, event := range events {
			inputChan <- event
		}
	}()

	// Execute pipeline
	pipe.Execute(inputChan)

	// Get collected results
	return collectStage.Results()
}

// saveBenchmarkResults saves results to CSV file
func saveBenchmarkResults(results []BenchmarkResult, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"EventType",
		"EventCount",
		"Workers",
		"ProcessingTimeMs",
		"ThroughputEventsPerSec",
		"TotalMemoryBytes",
		"AllocatedMemoryBytes",
		"Timestamp",
	}

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data rows
	for _, result := range results {
		row := []string{
			result.EventType,
			strconv.Itoa(result.EventCount),
			strconv.Itoa(result.Workers),
			strconv.FormatFloat(float64(result.ProcessingTime.Nanoseconds())/1e6, 'f', 2, 64),
			strconv.FormatFloat(result.Throughput, 'f', 2, 64),
			strconv.FormatUint(result.TotalMemory, 10),
			strconv.FormatUint(result.AllocatedMem, 10),
			result.Timestamp.Format(time.RFC3339),
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	return nil
}

// printBenchmarkSummary prints a summary of benchmark results
func printBenchmarkSummary(results []BenchmarkResult) {
	fmt.Printf("Benchmark Summary\n")

	// Group by event type
	simpleResults := []BenchmarkResult{}
	mableResults := []BenchmarkResult{}

	for _, result := range results {
		if result.EventType == "SimpleEvent" {
			simpleResults = append(simpleResults, result)
		} else {
			mableResults = append(mableResults, result)
		}
	}

	if len(simpleResults) > 0 {
		fmt.Printf("SimpleEvent Results:\n")
		printResultTable(simpleResults)
		fmt.Printf("\n")
	}

	if len(mableResults) > 0 {
		fmt.Printf("MableEvent Results:\n")
		printResultTable(mableResults)
	}
}

// printResultTable prints results in a formatted table
func printResultTable(results []BenchmarkResult) {
	fmt.Printf("%-12s %-15s %-12s %-20s\n", "Events", "Time (ms)", "Throughput", "Memory (MB)")
	fmt.Printf("%-12s %-15s %-12s %-20s\n", "------", "--------", "----------", "----------")

	for _, result := range results {
		timeMs := float64(result.ProcessingTime.Nanoseconds()) / 1e6
		memoryMB := float64(result.TotalMemory) / 1e6

		fmt.Printf("%-12d %-15.2f %-12.0f %-20.2f\n",
			result.EventCount,
			timeMs,
			result.Throughput,
			memoryMB)
	}
}

// GenerateComputeTestData creates a slice of ComputeEvent populated with
// synthetic data. The generated events contain payloads and fields that are
// intentionally complex to exercise validation, parsing and hashing in
// example pipelines. The function is deterministic given the same count and
// time may be used only for timestamps.
func GenerateComputeTestData(count int) []pipeline.ComputeEvent {
	events := make([]pipeline.ComputeEvent, count)

	for i := 0; i < count; i++ {
		// Create complex JSON payload for parsing
		payload := fmt.Sprintf(`{
			"metadata": {
				"transaction_id": "tx_%d",
				"user_agent": "Mozilla/5.0 (complex user agent string %d)",
				"session_data": {
					"clicks": %d,
					"duration": %d,
					"pages_visited": ["/page1", "/page2", "/checkout"]
				}
			},
			"cart_items": [
				{"item_id": %d, "price": %.2f, "quantity": %d}
			]
		}`, i+1, i+1, (i%50)+1, (i%3600)+60, i+1, float64(10+i%90)+0.99, (i%5)+1)

		events[i] = pipeline.ComputeEvent{
			ID:          i + 1,
			Email:       fmt.Sprintf("user%d@example%d.com", i+1, (i%10)+1),
			JSONPayload: payload,
			IPAddress:   fmt.Sprintf("192.168.%d.%d", (i%255)+1, (i%254)+1),
			Value:       float64(100+(i%500)) + 0.99,
			Timestamp:   time.Now().Unix() + int64(i),
		}
	}

	return events
}

// ConcurrentReduceProcessing creates and runs a pipeline that reads
// ComputeEvent values, applies filtration and mapping (heavy compute), then
// reduces and streams aggregated results. The function returns the collected
// AggregatedResult values produced by the reduce stage.
func ConcurrentReduceProcessing(events []pipeline.ComputeEvent, workerCount int, emitter pipeline.Emitter) []pipeline.AggregatedResult {
	cfg := pipeline.Config{
		MaxWorkersPerStage: workerCount,
		MaxBatchSize:       80,
		BatchTimeout:       3 * time.Second,
		Emitter:            emitter,
	}

	// Create input channel
	inputChan := make(chan pipeline.ComputeEvent, len(events))

	// First pipeline: Filter -> Map with heavy computation
	firstPipeline := pipeline.NewPipeline[pipeline.ComputeEvent](cfg).
		Filter(func(event pipeline.ComputeEvent) bool {
			// Heavy validation and parsing
			localEvent := pipeline.ComputeEvent{
				ID: event.ID, Email: event.Email, JSONPayload: event.JSONPayload,
				IPAddress: event.IPAddress, Value: event.Value, Timestamp: event.Timestamp,
			}
			_, valid := ValidateAndParseJSON(localEvent)
			return valid && event.Value > 150.0
		}).
		Map(func(event pipeline.ComputeEvent) pipeline.ComputeEvent {
			// Heavy hashing computation
			localEvent := pipeline.ComputeEvent{
				ID: event.ID, Email: event.Email, JSONPayload: event.JSONPayload,
				IPAddress: event.IPAddress, Value: event.Value, Timestamp: event.Timestamp,
			}
			processed := ComputeHashes(localEvent)
			return pipeline.ComputeEvent{
				ID: processed.ID, Email: processed.Email, JSONPayload: processed.JSONPayload,
				IPAddress: processed.IPAddress, Value: processed.Value, Timestamp: processed.Timestamp,
			}
		})

	// Use ReduceTransformAndStream to connect pipelines
	reducedChan, cfg := pipeline.ReduceTransformAndStream(firstPipeline,
		func(batch []pipeline.ComputeEvent) pipeline.AggregatedResult {
			// Convert to ComputeEvent for processing
			convertedBatch := make([]pipeline.ComputeEvent, len(batch))
			for i, event := range batch {
				convertedBatch[i] = pipeline.ComputeEvent{
					ID: event.ID, Email: event.Email, JSONPayload: event.JSONPayload,
					IPAddress: event.IPAddress, Value: event.Value, Timestamp: event.Timestamp,
				}
			}
			return HeavyReduceFunction(convertedBatch)
		}, inputChan)
	//
	// Collect results directly without additional transformation
	var finalResults []pipeline.AggregatedResult

	// Send events in goroutine
	go func() {
		defer close(inputChan)
		for _, event := range events {
			inputChan <- pipeline.ComputeEvent{
				ID: event.ID, Email: event.Email, JSONPayload: event.JSONPayload,
				IPAddress: event.IPAddress, Value: event.Value, Timestamp: event.Timestamp,
			}
		}
	}()

	// Collect all results from the reduce stream
	for result := range reducedChan {
		finalResults = append(finalResults, result)
	}

	return finalResults
}

// ValidateAndParseJSON validates a ComputeEvent's Email against a regular
// expression and attempts to unmarshal the JSONPayload. The parsed JSON map
// and a boolean success flag are returned. The function returns (nil, false)
// when validation or parsing fails.
func ValidateAndParseJSON(event pipeline.ComputeEvent) (map[string]interface{}, bool) {
	// Regex validation for email
	if !EmailRegex.MatchString(event.Email) {
		return nil, false
	}

	// Heavy JSON parsing
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(event.JSONPayload), &parsed); err != nil {
		return nil, false
	}

	return parsed, true
}

// EmailRegex is a precompiled regular expression used to validate email
// addresses in example data. It follows a permissive pattern suitable for
// demonstration purposes and is not intended as a production-grade validator.
var EmailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

// ComputeHashes applies multiple rounds of SHA-256 hashing to selected
// fields of a ComputeEvent to emulate CPU-bound work. The function truncates
// hashed outputs for readability in examples.
func ComputeHashes(event pipeline.ComputeEvent) pipeline.ComputeEvent {
	// Hash email multiple times for increased computational load
	hashedEmail := HashString(event.Email)
	for i := 0; i < 3; i++ { // Multiple rounds of hashing
		hashedEmail = HashString(hashedEmail)
	}

	// Hash IP address
	hashedIP := HashString(event.IPAddress)
	for i := 0; i < 3; i++ { // Multiple rounds of hashing
		hashedIP = HashString(hashedIP)
	}

	// Combine hashes as new data field
	event.Email = hashedEmail[:16] // Truncate for readability
	event.IPAddress = hashedIP[:16]

	return event
}

// HashString returns the hexadecimal encoding of the SHA-256 digest for the
// provided input string.
func HashString(input string) string {
	hash := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", hash)
}

// HeavyReduceFunction reduces a batch of ComputeEvent values into a single
// AggregatedResult. The function demonstrates an expensive reduce step that
// performs hashing and regex checks; it returns zero-value AggregatedResult
// when the input batch is empty.
func HeavyReduceFunction(batch []pipeline.ComputeEvent) pipeline.AggregatedResult {
	if len(batch) == 0 {
		return pipeline.AggregatedResult{}
	}

	var sum float64
	var emailHashes []string

	for _, event := range batch {
		sum += event.Value

		// Perform heavy computation for each event in reduce
		hashedEmail := HashString(fmt.Sprintf("%s_%d", event.Email, event.ID))

		// Additional regex processing
		ipPattern := regexp.MustCompile(`\d+\.\d+\.\d+\.\d+`)
		if ipPattern.MatchString(event.IPAddress) {
			emailHashes = append(emailHashes, hashedEmail[:8])
		}
	}

	// Heavy string operations
	combinedHashes := strings.Join(emailHashes, "|")
	finalHash := HashString(combinedHashes)

	return pipeline.AggregatedResult{
		BatchSum:       sum,
		AverageValue:   sum / float64(len(batch)),
		ProcessedCount: len(batch),
		HashedEmails:   finalHash[:16],
		ProcessedAt:    time.Now().Unix(),
	}
}
