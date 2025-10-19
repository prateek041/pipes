package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prateek041/pipes/pipeline"
)

// MableEvent represents the complex event structure from your example
type MableEvent struct {
	BD struct {
		AID   string                 `json:"aid"`
		BSTS  int64                  `json:"bsts"`
		CKS   map[string]interface{} `json:"cks"`
		IQPAR map[string]interface{} `json:"iqpar"`
		IR    string                 `json:"ir"`
		UA    string                 `json:"ua"`
		UL    string                 `json:"ul"`
	} `json:"bd"`
	CDAS struct {
		CC     []string `json:"cc"`
		CT     []string `json:"ct"`
		EM     []string `json:"em"`
		FN     []string `json:"fn"`
		ID     []string `json:"id"`
		LN     []string `json:"ln"`
		STREET []string `json:"street"`
		ZIP    []string `json:"zip"`
	} `json:"cdas"`
	EID string `json:"eid"`
	EN  string `json:"en"`
	SD  struct {
		IP4 string `json:"ip4"`
		SID string `json:"sid"`
	} `json:"sd"`
	ESD struct {
		Cart struct {
			Currency string `json:"currency"`
			Total    struct {
				Price float64 `json:"price"`
			} `json:"total"`
		} `json:"cart"`
		OID string `json:"oid"`
	} `json:"esd"`
	TS int64 `json:"ts"`
}

// ProcessedEvent represents the simplified processed event
type ProcessedEvent struct {
	OrderID     string  `json:"order_id"`
	HashedEmail string  `json:"hashed_email"`
	HashedIP    string  `json:"hashed_ip"`
	CartTotal   float64 `json:"cart_total"`
	EventName   string  `json:"event_name"`
	ProcessedAt int64   `json:"processed_at"`
}

var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

// generateMableTestData creates test Mable events as JSON strings
func generateMableTestData(count int) []string {
	events := make([]string, count)

	sampleEvent := `{
		"bd": {
			"aid": "01979c1b-364b-700c-a60b-b7146f2e7457",
			"bsts": 1750670719192,
			"cks": {"_fbp": "fb.1.1750670807500.411873176"},
			"iqpar": {},
			"ir": "",
			"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
			"ul": "en-GB"
		},
		"cdas": {
			"cc": ["DE"],
			"ct": ["Bremen"],
			"em": ["user%d@example.com"],
			"fn": ["Customer"],
			"id": ["%d"],
			"ln": ["LastName"],
			"street": ["Bremen Airport 123"],
			"zip": ["28199"]
		},
		"eid": "0197a599-ef42-7bb8-99e7-70e57cef4627",
		"en": "Order Completed",
		"sd": {
			"ip4": "185.132.133.%d",
			"sid": "session-id-123"
		},
		"esd": {
			"cart": {
				"currency": "EUR",
				"total": {
					"price": %.2f
				}
			},
			"oid": "order-%d"
		},
		"ts": %d
	}`

	for i := 0; i < count; i++ {
		ip := (i % 254) + 1                  // Generate IPs like 185.132.133.1-254
		price := float64(100+(i%900)) + 0.99 // Prices from 100.99 to 999.99
		timestamp := time.Now().Unix() + int64(i)

		eventJSON := fmt.Sprintf(sampleEvent, i+1, i+1, ip, price, i+1, timestamp)
		events[i] = eventJSON
	}

	return events
}

// hashString creates a SHA-256 hash of the input string
func hashString(input string) string {
	hash := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", hash)
}

// validateEmail checks if the email is valid using regex
func validateEmail(email string) bool {
	return emailRegex.MatchString(email)
}

// parseAndValidate performs heavy JSON parsing and validation
func parseAndValidate(jsonStr string) (*MableEvent, bool) {
	var event MableEvent

	// JSON unmarshaling using reflection
	if err := json.Unmarshal([]byte(jsonStr), &event); err != nil {
		return nil, false
	}

	// validation: check email format if present
	if len(event.CDAS.EM) > 0 && event.CDAS.EM[0] != "" {
		if !validateEmail(event.CDAS.EM[0]) {
			return nil, false
		}
	}

	// Additional validation: check if required fields exist
	if event.ESD.OID == "" || event.ESD.Cart.Total.Price <= 0 {
		return nil, false
	}

	return &event, true
}

// enrichAndTransform performs heavy cryptographic operations
func enrichAndTransform(event *MableEvent) *ProcessedEvent {
	processed := &ProcessedEvent{
		OrderID:     event.ESD.OID,
		CartTotal:   event.ESD.Cart.Total.Price,
		EventName:   event.EN,
		ProcessedAt: time.Now().Unix(),
	}

	// cryptographic hashing for email
	if len(event.CDAS.EM) > 0 && event.CDAS.EM[0] != "" {
		processed.HashedEmail = hashString(event.CDAS.EM[0])
	}

	// cryptographic hashing for IP
	if event.SD.IP4 != "" {
		processed.HashedIP = hashString(event.SD.IP4)
	}

	return processed
}

// serializeResult performs heavy JSON marshaling
func serializeResult(processed *ProcessedEvent) string {
	// JSON marshaling
	jsonBytes, _ := json.Marshal(processed)
	return string(jsonBytes)
}

// sequentialProcessing implements Parse -> Enrich -> Serialize sequentially
func sequentialProcessing(events []string) []string {
	var results []string

	for _, eventJSON := range events {
		// Parse & Validate
		parsed, valid := parseAndValidate(eventJSON)
		if !valid {
			continue
		}

		// Enrich & Transform
		enriched := enrichAndTransform(parsed)

		// Serialize
		serialized := serializeResult(enriched)

		results = append(results, serialized)
	}

	return results
}

// concurrentProcessing implements Parse -> Enrich -> Serialize using single pipeline
func concurrentProcessing(events []string, workerCount int) []string {
	cfg := pipeline.Config{
		MaxWorkersPerStage: workerCount,
		MaxBatchSize:       80,
		BatchTimeout:       2 * time.Second,
	}

	// Single pipeline: Parse & Validate -> Enrich & Transform -> Serialize
	pipe := pipeline.NewPipeline[string](cfg).
		Filter(func(jsonStr string) bool {
			// JSON parsing and validation
			_, valid := parseAndValidate(jsonStr)
			return valid
		}).
		Map(func(jsonStr string) string {
			// Parse again for enrichment (could be optimized but keeping consistent with original)
			parsed, _ := parseAndValidate(jsonStr)

			// Enrich & Transform
			enriched := enrichAndTransform(parsed)

			// Serialize result
			return serializeResult(enriched)
		})

	// Single collector for the entire pipeline
	collector := pipe.Collect()

	// Convert slice to channel for execution
	inputChan := make(chan string, len(events))
	go func() {
		defer close(inputChan)
		for _, event := range events {
			inputChan <- event
		}
	}()

	// Execute single pipeline - no blocking waits between stages
	pipe.Execute(inputChan)
	return collector.Results()
}

// Benchmark function that compares sequential vs concurrent performance
func Benchmark(b *testing.B) {
	eventCounts := []int{10, 100, 1000, 10000, 100000, 1000000}

	// Print header
	b.Log("")
	b.Log(strings.Repeat("=", 120))
	b.Log("HEAVY COMPUTATIONAL PIPELINE: Parse & Validate -> Enrich & Transform -> Serialize")
	b.Log("JSON Parsing + Regex Validation + SHA-256 Hashing + JSON Marshaling")
	b.Log(strings.Repeat("=", 120))
	b.Log("")
	b.Log("METRICS EXPLANATION:")
	b.Log("  • ns/op     = Nanoseconds per operation (LOWER is better)")
	b.Log("  • events/s  = Events processed per second (HIGHER is better)")
	b.Log("  • Heavy operations: JSON parsing (reflection), Regex validation, SHA-256 hashing, JSON marshaling")
	b.Log("")

	// Results storage for comparison table
	type BenchResult struct {
		Events         int
		SeqTime        time.Duration
		ConcTime       time.Duration
		SeqThroughput  float64
		ConcThroughput float64
	}

	var results []BenchResult
	workers := runtime.NumCPU()

	for _, count := range eventCounts {
		events := generateMableTestData(count)
		result := BenchResult{Events: count}

		// Sequential benchmark
		b.Run(fmt.Sprintf("Sequential_%d", count), func(b *testing.B) {
			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				processedResults := sequentialProcessing(events)
				_ = processedResults
			}
			result.SeqTime = time.Since(start) / time.Duration(b.N)
			result.SeqThroughput = float64(count) / result.SeqTime.Seconds()

			b.ReportMetric(result.SeqThroughput, "events/sec")
		})

		// Concurrent benchmark
		b.Run(fmt.Sprintf("Concurrent_%d", count), func(b *testing.B) {
			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				processedResults := concurrentProcessing(events, workers) // Use workers as the number of CPUs in the system..
				_ = processedResults
			}
			result.ConcTime = time.Since(start) / time.Duration(b.N)
			result.ConcThroughput = float64(count) / result.ConcTime.Seconds()

			b.ReportMetric(result.ConcThroughput, "events/sec")
		})

		results = append(results, result)
	}

	// Print comparison table
	b.Log("")
	b.Log("HEAVY COMPUTATION PERFORMANCE COMPARISON")
	b.Log(fmt.Sprintf("%-12s │ %-30s │ %-30s │ %-12s │ %-15s", "Events", "Sequential", "Concurrent", "Winner", "Speedup"))
	b.Log(strings.Repeat("─", 12) + "┼" + strings.Repeat("─", 30) + "┼" + strings.Repeat("─", 30) + "┼" + strings.Repeat("─", 12) + "┼" + strings.Repeat("─", 15))

	for _, r := range results {
		winner := "Sequential"
		speedup := r.SeqTime.Seconds() / r.ConcTime.Seconds()
		if r.ConcThroughput > r.SeqThroughput {
			winner = "Concurrent"
		} else {
			speedup = r.ConcTime.Seconds() / r.SeqTime.Seconds()
		}

		seqStr := fmt.Sprintf("%.0f ns/op, %.0f e/s", float64(r.SeqTime.Nanoseconds()), r.SeqThroughput)
		concStr := fmt.Sprintf("%.0f ns/op, %.0f e/s", float64(r.ConcTime.Nanoseconds()), r.ConcThroughput)
		speedupStr := fmt.Sprintf("%.2fx", speedup)

		b.Log(fmt.Sprintf("%-12d │ %-30s │ %-30s │ %-12s │ %-15s",
			r.Events, seqStr, concStr, winner, speedupStr))
	}

	b.Log("")
	b.Log("COMPUTATIONAL LOAD ANALYSIS:")
	b.Log("JSON Parsing: Uses Go reflection - CPU intensive")
	b.Log("Regex Validation: Email pattern matching - CPU intensive")
	b.Log("SHA-256 Hashing: Cryptographic operations - CPU intensive")
	b.Log("JSON Marshaling: Struct serialization - CPU intensive")
	b.Log("")
	b.Log("ANALYSIS SUMMARY:")
	b.Log(strings.Repeat("=", 120))
}

// ComputeEvent represents an event with heavy computational requirements
type ComputeEvent struct {
	ID          int     `json:"id"`
	Email       string  `json:"email"`
	JSONPayload string  `json:"payload"`
	IPAddress   string  `json:"ip_address"`
	Value       float64 `json:"value"`
	Timestamp   int64   `json:"timestamp"`
}

// AggregatedResult represents the result after reduce operation
type AggregatedResult struct {
	BatchSum       float64 `json:"batch_sum"`
	AverageValue   float64 `json:"average_value"`
	ProcessedCount int     `json:"processed_count"`
	HashedEmails   string  `json:"hashed_emails"`
	ProcessedAt    int64   `json:"processed_at"`
}

// generateComputeTestData creates test events with heavy computation requirements
func generateComputeTestData(count int) []ComputeEvent {
	events := make([]ComputeEvent, count)

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

		events[i] = ComputeEvent{
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

// Heavy computational functions

// validateAndParseJSON performs heavy JSON parsing and validation
func validateAndParseJSON(event ComputeEvent) (map[string]interface{}, bool) {
	// Regex validation for email
	if !emailRegex.MatchString(event.Email) {
		return nil, false
	}

	// Heavy JSON parsing
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(event.JSONPayload), &parsed); err != nil {
		return nil, false
	}

	return parsed, true
}

// computeHashes performs multiple SHA-256 hashing operations
func computeHashes(event ComputeEvent) ComputeEvent {
	// Hash email multiple times for increased computational load
	hashedEmail := hashString(event.Email)
	for i := 0; i < 3; i++ { // Multiple rounds of hashing
		hashedEmail = hashString(hashedEmail)
	}

	// Hash IP address
	hashedIP := hashString(event.IPAddress)
	for i := 0; i < 3; i++ { // Multiple rounds of hashing
		hashedIP = hashString(hashedIP)
	}

	// Combine hashes as new data field
	event.Email = hashedEmail[:16] // Truncate for readability
	event.IPAddress = hashedIP[:16]

	return event
}

// heavyReduceFunction performs aggregation with heavy computation
func heavyReduceFunction(batch []ComputeEvent) AggregatedResult {
	if len(batch) == 0 {
		return AggregatedResult{}
	}

	var sum float64
	var emailHashes []string

	for _, event := range batch {
		sum += event.Value

		// Perform heavy computation for each event in reduce
		hashedEmail := hashString(fmt.Sprintf("%s_%d", event.Email, event.ID))

		// Additional regex processing
		ipPattern := regexp.MustCompile(`\d+\.\d+\.\d+\.\d+`)
		if ipPattern.MatchString(event.IPAddress) {
			emailHashes = append(emailHashes, hashedEmail[:8])
		}
	}

	// Heavy string operations
	combinedHashes := strings.Join(emailHashes, "|")
	finalHash := hashString(combinedHashes)

	return AggregatedResult{
		BatchSum:       sum,
		AverageValue:   sum / float64(len(batch)),
		ProcessedCount: len(batch),
		HashedEmails:   finalHash[:16],
		ProcessedAt:    time.Now().Unix(),
	}
}

// Sequential processing with reduce streaming pattern
func sequentialReduceProcessing(events []ComputeEvent) []AggregatedResult {
	var results []AggregatedResult

	// Process in batches like the pipeline would
	batchSize := 10
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]

		// Filter and map operations
		var filteredBatch []ComputeEvent
		for _, event := range batch {
			// Heavy validation
			if _, valid := validateAndParseJSON(event); valid && event.Value > 150.0 {
				// Heavy computation
				processed := computeHashes(event)
				filteredBatch = append(filteredBatch, processed)
			}
		}

		// Reduce operation - directly append the result instead of transforming to string
		if len(filteredBatch) > 0 {
			reduced := heavyReduceFunction(filteredBatch)
			results = append(results, reduced)
		}
	}

	return results
}

// Concurrent processing using pipeline with reduce streaming
func concurrentReduceProcessing(events []ComputeEvent, workerCount int) []AggregatedResult {
	cfg := pipeline.Config{
		MaxWorkersPerStage: workerCount,
		MaxBatchSize:       80,
		BatchTimeout:       3 * time.Second,
	}

	// Create input channel
	inputChan := make(chan pipeline.ComputeEvent, len(events))

	// First pipeline: Filter -> Map with heavy computation
	firstPipeline := pipeline.NewPipeline[pipeline.ComputeEvent](cfg).
		Filter(func(event pipeline.ComputeEvent) bool {
			// Heavy validation and parsing
			localEvent := ComputeEvent{
				ID: event.ID, Email: event.Email, JSONPayload: event.JSONPayload,
				IPAddress: event.IPAddress, Value: event.Value, Timestamp: event.Timestamp,
			}
			_, valid := validateAndParseJSON(localEvent)
			return valid && event.Value > 150.0
		}).
		Map(func(event pipeline.ComputeEvent) pipeline.ComputeEvent {
			// Heavy hashing computation
			localEvent := ComputeEvent{
				ID: event.ID, Email: event.Email, JSONPayload: event.JSONPayload,
				IPAddress: event.IPAddress, Value: event.Value, Timestamp: event.Timestamp,
			}
			processed := computeHashes(localEvent)
			return pipeline.ComputeEvent{
				ID: processed.ID, Email: processed.Email, JSONPayload: processed.JSONPayload,
				IPAddress: processed.IPAddress, Value: processed.Value, Timestamp: processed.Timestamp,
			}
		})

	// Use ReduceTransformAndStream to connect pipelines
	reducedChan := pipeline.ReduceTransformAndStream(firstPipeline,
		func(batch []pipeline.ComputeEvent) AggregatedResult {
			// Convert to ComputeEvent for processing
			convertedBatch := make([]ComputeEvent, len(batch))
			for i, event := range batch {
				convertedBatch[i] = ComputeEvent{
					ID: event.ID, Email: event.Email, JSONPayload: event.JSONPayload,
					IPAddress: event.IPAddress, Value: event.Value, Timestamp: event.Timestamp,
				}
			}
			return heavyReduceFunction(convertedBatch)
		}, inputChan)

	// Collect results directly without additional transformation
	var finalResults []AggregatedResult

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

// BenchmarkReduceStreaming benchmarks the reduce streaming functionality
func BenchmarkReduceStreaming(b *testing.B) {
	eventCounts := []int{100, 1000, 5000, 10000, 100000, 1000000}

	b.Log("")
	b.Log(strings.Repeat("=", 120))
	b.Log("REDUCE STREAMING PIPELINE: Filter -> Map -> Reduce -> Stream")
	b.Log("Heavy Ops: JSON Parsing + Email Regex + Multi-round SHA-256 + Reduce Aggregation")
	b.Log(strings.Repeat("=", 120))
	b.Log("")
	b.Log("PIPELINE PATTERN:")
	b.Log("Pipeline 1: Filter(validate+parse) -> Map(multi-hash) -> Reduce(aggregate)")
	b.Log("Removed single-goroutine final transformation bottleneck")
	b.Log("Direct AggregatedResult comparison for fair benchmarking")
	b.Log("")

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
		events := generateComputeTestData(count)
		result := ReduceBenchResult{Events: count}

		// Sequential benchmark
		b.Run(fmt.Sprintf("ReduceSeq_%d", count), func(b *testing.B) {
			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				processedResults := sequentialReduceProcessing(events)
				_ = processedResults
			}
			result.SeqTime = time.Since(start) / time.Duration(b.N)
			result.SeqThroughput = float64(count) / result.SeqTime.Seconds()

			b.ReportMetric(result.SeqThroughput, "events/sec")
		})

		// Concurrent benchmark with reduce streaming
		b.Run(fmt.Sprintf("ReduceConc_%d", count), func(b *testing.B) {
			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				processedResults := concurrentReduceProcessing(events, workers)
				_ = processedResults
			}
			result.ConcTime = time.Since(start) / time.Duration(b.N)
			result.ConcThroughput = float64(count) / result.ConcTime.Seconds()

			b.ReportMetric(result.ConcThroughput, "events/sec")
		})

		results = append(results, result)
	}

	// Print comparison table
	b.Log("")
	b.Log("REDUCE STREAMING PERFORMANCE COMPARISON")
	b.Log(fmt.Sprintf("%-12s │ %-30s │ %-30s │ %-12s │ %-15s", "Events", "Sequential", "Concurrent+Reduce", "Winner", "Speedup"))
	b.Log(strings.Repeat("─", 12) + "┼" + strings.Repeat("─", 30) + "┼" + strings.Repeat("─", 30) + "┼" + strings.Repeat("─", 12) + "┼" + strings.Repeat("─", 15))

	for _, r := range results {
		winner := "Sequential"
		speedup := r.SeqTime.Seconds() / r.ConcTime.Seconds()
		if r.ConcThroughput > r.SeqThroughput {
			winner = "Concurrent"
		} else {
			speedup = r.ConcTime.Seconds() / r.SeqTime.Seconds()
		}

		seqStr := fmt.Sprintf("%.0f ns/op, %.0f e/s", float64(r.SeqTime.Nanoseconds()), r.SeqThroughput)
		concStr := fmt.Sprintf("%.0f ns/op, %.0f e/s", float64(r.ConcTime.Nanoseconds()), r.ConcThroughput)
		speedupStr := fmt.Sprintf("%.2fx", speedup)

		b.Log(fmt.Sprintf("%-12d │ %-30s │ %-30s │ %-12s │ %-15s",
			r.Events, seqStr, concStr, winner, speedupStr))
	}

	b.Log("")
	b.Log("REDUCE STREAMING ANALYSIS:")
	b.Log("Multi-round SHA-256 hashing (3 rounds per email/IP)")
	b.Log("Heavy JSON parsing with reflection")
	b.Log("Email regex validation per event")
	b.Log("Reduce aggregation with string operations")
	b.Log("Type transformation via ReduceTransformAndStream")
	b.Log("")
}
