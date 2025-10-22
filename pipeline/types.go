package pipeline

import "time"

// SimpleEvent is non generic struct that we use to
// implement the pipeline logic first.
type SimpleEvent struct {
	ID    int
	Data  string
	Value float64
}

type ComputeEvent struct {
	ID          int     `json:"id"`
	Email       string  `json:"email"`
	JSONPayload string  `json:"payload"`
	IPAddress   string  `json:"ip_address"`
	Value       float64 `json:"value"`
	Timestamp   int64   `json:"timestamp"`
}

type EventPaid struct {
	Original SimpleEvent
	New      SimpleEvent
}

type Config struct {
	MaxWorkersPerStage int
	MaxBatchSize       int
	BatchTimeout       time.Duration

	Emitter     Emitter
	ExecutionID string // Set automatically if not provided
}

type AggregatedResult struct {
	BatchSum       float64 `json:"batch_sum"`
	AverageValue   float64 `json:"average_value"`
	ProcessedCount int     `json:"processed_count"`
	HashedEmails   string  `json:"hashed_emails"`
	ProcessedAt    int64   `json:"processed_at"`
}
