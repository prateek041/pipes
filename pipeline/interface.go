package pipeline

import (
	"sync"
	"time"
)

// Emitter interface for observability.
type Emitter interface {
	// Pipeline level metrics
	EmitPipelineStart(executionID string, pipelineType string, config Config)
	EmitPipelineEnd(executionID string, inputCount, outputCount uint64, duration time.Duration)

	// Stage level metrics
	EmitStageStart(executionID, stageName string, stageIndex uint32)
	EmitStageEnd(executionID, stageName string, inputCount, outputCount uint64, duration time.Duration)

	// Batch Level Events
	EmitBatchMetrics(executionID, stageName, batchID, workerID string, batchSize uint32, processingTime time.Duration)

	// Error tracking
	EmitError(executionID, stageName, errorMsg string)

	// Lifecycle
	Close() error
}

// NoOpEmitter for when observability is disabled
type NoOpEmitter struct{}

func (n *NoOpEmitter) EmitPipelineStart(string, string, Config)                               {}
func (n *NoOpEmitter) EmitPipelineEnd(string, uint64, uint64, time.Duration)                  {}
func (n *NoOpEmitter) EmitStageStart(string, string, uint32)                                  {}
func (n *NoOpEmitter) EmitStageEnd(string, string, uint64, uint64, time.Duration)             {}
func (n *NoOpEmitter) EmitBatchMetrics(string, string, string, string, uint32, time.Duration) {}
func (n *NoOpEmitter) EmitError(string, string, string)                                       {}
func (n *NoOpEmitter) Close() error                                                           { return nil }

// Generic Stage interface that can work with any type T
type Stage[T any] interface {
	// ProcessBatch is the method that runs inside a Worker Goroutine.
	ProcessBatch(batch []T) ([]T, error)

	// Connect sets up the plumbing (channels and goroutines) for this stage.
	// Waitgroup to signal when it's finished processing.
	Connect(wg *sync.WaitGroup, inChan <-chan T, outChan chan<- T, emitter Emitter, executionID string, stageIndex uint32) error

	// Name returns the stage name for observability and logging.
	Name() string
}

// TransformStage is for stages that can transform from type TIn to type TOut
// This is useful for Reduce operations where input and output types differ
type TransformStage[TIn, TOut any] interface {
	ProcessBatch(batch []TIn) ([]TOut, error)
	Connect(wg *sync.WaitGroup, inChan <-chan TIn, outChan chan<- TOut, emitter Emitter) error
	Name() string
}

// Type aliases for backward compatibility with SimpleEvent
type SimpleEventStage = Stage[SimpleEvent]
