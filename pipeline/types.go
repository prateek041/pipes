package pipeline

import "time"

// SimpleEvent is non generic struct that we use to
// implement the pipeline logic first.
type SimpleEvent struct {
	ID    int
	Data  string
	Value float64
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
