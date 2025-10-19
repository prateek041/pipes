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
}
