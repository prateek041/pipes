package pipeline

import "time"

// SimpleEvent is a small concrete event type used by examples and tests.
// It is provided for convenience when working with the pipeline primitives
// without defining custom types.
type SimpleEvent struct {
	ID    int
	Data  string
	Value float64
}

// ComputeEvent is a richer event structure used in heavier example
// workloads. It includes fields for payload, email and timestamps to
// demonstrate validation, parsing and hashing within pipeline stages.
type ComputeEvent struct {
	ID          int     `json:"id"`
	Email       string  `json:"email"`
	JSONPayload string  `json:"payload"`
	IPAddress   string  `json:"ip_address"`
	Value       float64 `json:"value"`
	Timestamp   int64   `json:"timestamp"`
}

// EventPaid models a pair of original and transformed SimpleEvent values.
// It is used by the Generate primitive to return both the source and
// generated events when needed.
type EventPaid struct {
	Original SimpleEvent
	New      SimpleEvent
}

// Config controls pipeline execution parameters such as batching and
// worker pool sizing. Most fields have sensible defaults and the
// NewPipeline helper will assign a NoOpEmitter if Emitter is nil.
type Config struct {
	MaxWorkersPerStage int
	MaxBatchSize       int
	BatchTimeout       time.Duration

	Emitter     Emitter
	ExecutionID string // Set automatically if not provided
}

// AggregatedResult represents the output of a reduce operation and is
// included as a convenience type used by example code in the repository.
type AggregatedResult struct {
	BatchSum       float64 `json:"batch_sum"`
	AverageValue   float64 `json:"average_value"`
	ProcessedCount int     `json:"processed_count"`
	HashedEmails   string  `json:"hashed_emails"`
	ProcessedAt    int64   `json:"processed_at"`
}
