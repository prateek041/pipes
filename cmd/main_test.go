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

// concurrentProcessing implements Parse -> Enrich -> Serialize using pipeline
func concurrentProcessing(events []string, workerCount int) []string {
	cfg := pipeline.Config{
		MaxWorkersPerStage: workerCount,
		MaxBatchSize:       50,
	}

	// Parse & Validate Pipeline
	parsePipeline := pipeline.NewPipeline[string](cfg).
		Filter(func(jsonStr string) bool {
			// JSON parsing and validation
			_, valid := parseAndValidate(jsonStr)
			return valid
		}).
		Map(func(jsonStr string) string {
			// Return the JSON string for next stage
			return jsonStr
		})

	// Enrich & Transform Pipeline
	enrichPipeline := pipeline.NewPipeline[string](cfg).
		Map(func(jsonStr string) string {
			parsed, _ := parseAndValidate(jsonStr)

			enriched := enrichAndTransform(parsed)

			return serializeResult(enriched)
		})

	// Connect pipelines
	parseCollector := parsePipeline.Collect()

	// Convert slice to channel for execution
	inputChan := make(chan string, len(events))
	go func() {
		defer close(inputChan)
		for _, event := range events {
			inputChan <- event
		}
	}()

	// Execute parse pipeline
	parsePipeline.Execute(inputChan)
	parsedResults := parseCollector.Results()

	// Execute enrich pipeline
	enrichCollector := enrichPipeline.Collect()

	enrichChan := make(chan string, len(parsedResults))
	go func() {
		defer close(enrichChan)
		for _, result := range parsedResults {
			enrichChan <- result
		}
	}()

	enrichPipeline.Execute(enrichChan)
	return enrichCollector.Results()
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
