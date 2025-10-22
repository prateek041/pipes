package pipeline

import "time"

// PipelineMetric captures high level metrics for a pipeline execution. It is
// primarily intended for shipping to external systems via an Emitter.
type PipelineMetric struct {
	ExecutionID     string     `json:"execution_id"`
	PipelineType    string     `json:"pipeline_type"`
	StartTime       time.Time  `json:"start_time"`
	EndTime         *time.Time `json:"end_time,omitempty"`
	DurationMs      *uint64    `json:"duration_ms,omitempty"`
	InputCount      *uint64    `json:"input_count,omitempty"`
	OutputCount     *uint64    `json:"output_count,omitempty"`
	ConfigWorkers   uint32     `json:"config_workers"`
	ConfigBatchSize uint32     `json:"config_batch_size"`
	ConfigTimeoutMs uint64     `json:"config_timeout_ms"`
	Status          string     `json:"status"` // "started", "completed", "failed"
}

// StageMetric describes metrics collected for an individual stage during
// pipeline execution.
type StageMetric struct {
	ExecutionID   string     `json:"execution_id"`
	StageName     string     `json:"stage_name"`
	StageIndex    uint32     `json:"stage_index"`
	StartTime     time.Time  `json:"start_time"`
	EndTime       *time.Time `json:"end_time,omitempty"`
	DurationMs    *uint64    `json:"duration_ms,omitempty"`
	InputEvents   *uint64    `json:"input_events,omitempty"`
	OutputEvents  *uint64    `json:"output_events,omitempty"`
	ThroughputEPS *float64   `json:"throughput_eps,omitempty"`
	ActiveWorkers uint32     `json:"active_workers"`
	Status        string     `json:"status"` // "started", "completed", "failed"
}

// BatchMetric contains metrics for a processed batch within a stage.
type BatchMetric struct {
	ExecutionID      string    `json:"execution_id"`
	StageName        string    `json:"stage_name"`
	BatchID          string    `json:"batch_id"`
	WorkerID         string    `json:"worker_id"`
	BatchSize        uint32    `json:"batch_size"`
	ProcessingTimeMs uint64    `json:"processing_time_ms"`
	Timestamp        time.Time `json:"timestamp"`
}

// ErrorMetric represents an error event captured during pipeline execution.
type ErrorMetric struct {
	ExecutionID string    `json:"execution_id"`
	StageName   string    `json:"stage_name"`
	ErrorMsg    string    `json:"error_msg"`
	Timestamp   time.Time `json:"timestamp"`
}

// MetricBatch is a container used to flush multiple metric types in a
// single request to the backend.
type MetricBatch struct {
	PipelineMetrics []PipelineMetric `json:"pipeline_metrics"`
	StageMetrics    []StageMetric    `json:"stage_metrics"`
	BatchMetrics    []BatchMetric    `json:"batch_metrics"`
	ErrorMetrics    []ErrorMetric    `json:"error_metrics"`
}
