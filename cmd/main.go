// Package main demonstrates usage examples and a small benchmark runner for
// the pipeline package. It contains helper functions that construct test data,
// run concurrent reduce pipelines, and perform CPU-bound work used by the
// examples. The comments in this file follow Go documentation conventions so
// they can be rendered by godoc.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/prateek041/pipes/pipeline"
)

// filterFn reports whether the provided SimpleEvent should be kept. The
// function returns true when the event's Value is greater than 50.0.
func filterFn(e pipeline.SimpleEvent) bool {
	return e.Value > 50.0
}

// mapFn transforms a SimpleEvent by offsetting its ID and prefixing Data
// with "PROCESSED: ". The mapping is intentionally simple and used for
// demonstration purposes in examples and benchmarks.
func mapFn(e pipeline.SimpleEvent) pipeline.SimpleEvent {
	e.ID = e.ID + 1000
	e.Data = "PROCESSED: " + e.Data
	return e
}

// avgFunction computes a simple aggregate value over a batch of
// SimpleEvent. When the batch is empty it returns 0.0. The function prints
// intermediate values for visibility in example runs but can be removed in
// production code.
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

// main is the entry point for the example/benchmark runner. It configures
// observability, constructs test data, wires up a pipeline that performs
// filtering, mapping and reducing, and then prints the final results.
func main() {

	observabilityConfig := pipeline.ObservabilityConfig{
		Enabled:       true,
		DatabaseURL:   "tcp://localhost:9000",
		Database:      "pipeline_metrics",
		BufferSize:    0,
		FlushInterval: 3 * time.Second,
		Debug:         true,
	}

	emitter, err := pipeline.NewClickHouseEmitter(observabilityConfig)
	if err != nil {
		panic(err)
	}
	defer emitter.Close()

	// eventCounts := []int{100, 1000, 5000, 10000, 100000, 1000000}
	eventCounts := []int{100}

	// Results storage for comparison table
	type ReduceBenchResult struct {
		Events         int
		SeqTime        time.Duration
		ConcTime       time.Duration
		SeqThroughput  float64
		ConcThroughput float64
	}

	var results []ReduceBenchResult
	workers := runtime.NumCPU()

	for _, count := range eventCounts {
		events := GenerateComputeTestData(count)
		result := ReduceBenchResult{Events: count}

		processedResults := ConcurrentReduceProcessing(events, workers, emitter)
		_ = processedResults
		result.ConcThroughput = float64(count) / result.ConcTime.Seconds()

		results = append(results, result)
	}

	fmt.Println("final result", results)
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
