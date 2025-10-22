package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type ObservabilityConfig struct {
	Enabled       bool
	DatabaseURL   string
	Database      string
	BufferSize    int
	FlushInterval time.Duration
	MaxRetries    int
	Debug         bool
}

type ClickHouseEmitter struct {
	config ObservabilityConfig
	conn   driver.Conn

	// Async batching channels
	pipelineChan chan PipelineMetric
	stageChan    chan StageMetric
	batchChan    chan BatchMetric
	errorChan    chan ErrorMetric

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
	mu     sync.Mutex
}

func connect() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"localhost:19000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "changeme",
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},
			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
		})
	)

	if err != nil {
		fmt.Println("Error in getting the connection", err)
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func NewClickHouseEmitter(config ObservabilityConfig) (*ClickHouseEmitter, error) {
	if !config.Enabled {
		return &ClickHouseEmitter{config: config, closed: true}, nil
	}

	// Set defaults
	if config.BufferSize == 0 {
		config.BufferSize = 1000
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = 5 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}

	// Connect to ClickHouse
	conn, err := connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	emitter := &ClickHouseEmitter{
		config:       config,
		conn:         conn,
		pipelineChan: make(chan PipelineMetric, config.BufferSize),
		stageChan:    make(chan StageMetric, config.BufferSize),
		batchChan:    make(chan BatchMetric, config.BufferSize),
		errorChan:    make(chan ErrorMetric, config.BufferSize),
		ctx:          ctx,
		cancel:       cancel,
	}

	rows, err := conn.Query(ctx, "SELECT name, toString(uuid) as uuid_str FROM system.tables LIMIT 5")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var name, uuid string
		if err := rows.Scan(&name, &uuid); err != nil {
			log.Fatal(err)
		}
		log.Printf("name: %s, uuid: %s", name, uuid)
	}

	// Start background processors
	emitter.startBackgroundProcessors()

	return emitter, nil
}

func (e *ClickHouseEmitter) startBackgroundProcessors() {
	// Pipeline metrics processor
	e.wg.Add(1)
	go e.processPipelineMetrics()

	// Stage metrics processor
	e.wg.Add(1)
	go e.processStageMetrics()

	// Batch metrics processor
	e.wg.Add(1)
	go e.processBatchMetrics()

	// Error metrics processor
	e.wg.Add(1)
	go e.processErrorMetrics()
}

// Non-blocking emission methods
func (e *ClickHouseEmitter) EmitPipelineStart(executionID, pipelineType string, config Config) {
	if e.closed {
		return
	}

	metric := PipelineMetric{
		ExecutionID:     executionID,
		PipelineType:    pipelineType,
		StartTime:       time.Now(),
		ConfigWorkers:   uint32(config.MaxWorkersPerStage),
		ConfigBatchSize: uint32(config.MaxBatchSize),
		ConfigTimeoutMs: uint64(config.BatchTimeout.Milliseconds()),
		Status:          "started",
	}

	select {
	case e.pipelineChan <- metric:
	default:
		if e.config.Debug {
			log.Printf("Pipeline metric channel full, dropping metric")
		}
	}
}

func (e *ClickHouseEmitter) EmitPipelineEnd(executionID string, inputCount, outputCount uint64, duration time.Duration) {
	if e.closed {
		return
	}

	endTime := time.Now()
	durationMs := uint64(duration.Milliseconds())

	metric := PipelineMetric{
		ExecutionID: executionID,
		EndTime:     &endTime,
		DurationMs:  &durationMs,
		InputCount:  &inputCount,
		OutputCount: &outputCount,
		Status:      "completed",
	}

	select {
	case e.pipelineChan <- metric:
	default:
		if e.config.Debug {
			log.Printf("Pipeline metric channel full, dropping metric")
		}
	}
}

func (e *ClickHouseEmitter) EmitStageStart(executionID, stageName string, stageIndex uint32) {
	if e.closed {
		return
	}

	metric := StageMetric{
		ExecutionID:   executionID,
		StageName:     stageName,
		StageIndex:    stageIndex,
		StartTime:     time.Now(),
		ActiveWorkers: 0, // Will be updated in Connect
		Status:        "started",
	}

	select {
	case e.stageChan <- metric:
	default:
		if e.config.Debug {
			log.Printf("Stage metric channel full, dropping metric")
		}
	}
}

func (e *ClickHouseEmitter) EmitStageEnd(executionID, stageName string, inputCount, outputCount uint64, duration time.Duration) {
	if e.closed {
		return
	}

	endTime := time.Now()
	durationMs := uint64(duration.Milliseconds())
	throughput := float64(outputCount) / duration.Seconds()

	metric := StageMetric{
		ExecutionID:   executionID,
		StageName:     stageName,
		EndTime:       &endTime,
		DurationMs:    &durationMs,
		InputEvents:   &inputCount,
		OutputEvents:  &outputCount,
		ThroughputEPS: &throughput,
		Status:        "completed",
	}

	select {
	case e.stageChan <- metric:
	default:
		if e.config.Debug {
			log.Printf("Stage metric channel full, dropping metric")
		}
	}
}

func (e *ClickHouseEmitter) EmitBatchMetrics(executionID, stageName, batchID, workerID string, batchSize uint32, processingTime time.Duration) {
	if e.closed {
		return
	}

	metric := BatchMetric{
		ExecutionID:      executionID,
		StageName:        stageName,
		BatchID:          batchID,
		WorkerID:         workerID,
		BatchSize:        batchSize,
		ProcessingTimeMs: uint64(processingTime.Milliseconds()),
		Timestamp:        time.Now(),
	}

	select {
	case e.batchChan <- metric:
	default:
		if e.config.Debug {
			log.Printf("Batch metric channel full, dropping metric")
		}
	}
}

func (e *ClickHouseEmitter) EmitError(executionID, stageName, errorMsg string) {
	if e.closed {
		return
	}

	metric := ErrorMetric{
		ExecutionID: executionID,
		StageName:   stageName,
		ErrorMsg:    errorMsg,
		Timestamp:   time.Now(),
	}

	select {
	case e.errorChan <- metric:
	default:
		if e.config.Debug {
			log.Printf("Error metric channel full, dropping metric")
		}
	}
}

// Background processors with batching
func (e *ClickHouseEmitter) processPipelineMetrics() {
	defer e.wg.Done()

	var batch []PipelineMetric
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case metric := <-e.pipelineChan:
			batch = append(batch, metric)
			if len(batch) >= e.config.BufferSize {
				e.flushPipelineMetrics(batch)
				batch = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				e.flushPipelineMetrics(batch)
				batch = nil
			}

		case <-e.ctx.Done():
			if len(batch) > 0 {
				e.flushPipelineMetrics(batch)
			}
			return
		}
	}
}

func (e *ClickHouseEmitter) flushPipelineMetrics(metrics []PipelineMetric) {
	ctx := context.Background()
	if e.config.Debug {
		log.Printf("Flushing %d pipeline metrics to ClickHouse", len(metrics))
	}

	batch, err := e.conn.PrepareBatch(ctx, `INSERT INTO pipeline_metrics.pipeline_executions (
	        execution_id, pipeline_type, start_time, end_time, duration_ms,
	        input_count, output_count, config_workers, config_batch_size,
	        config_timeout_ms, status
	    ) VALUES`)

	if err != nil {
		log.Printf("Failed to prepare pipeline metrics statement: %v", err)
		return
	}

	defer batch.Close()

	for _, metric := range metrics {

		err := batch.Append(
			metric.ExecutionID,
			metric.PipelineType,
			metric.StartTime,
			metric.EndTime,
			metric.DurationMs,
			metric.InputCount,
			metric.OutputCount,
			metric.ConfigWorkers,
			metric.ConfigBatchSize,
			metric.ConfigTimeoutMs,
			metric.Status,
		)

		if err != nil {
			log.Printf("Failed to insert pipeline metric: %v", err)
		}
	}

	err = batch.Send()
	if err != nil {
		log.Println("Error sending pipeline metric", err)
	}

	log.Println("pipeline metrics is prepped and sent", metrics)
}

func (e *ClickHouseEmitter) flushStageMetrics(metrics []StageMetric) {
	ctx := context.Background()
	if e.config.Debug {
		log.Printf("Flushing %d stage metrics to ClickHouse", len(metrics))
	}

	batch, err := e.conn.PrepareBatch(ctx, `INSERT INTO pipeline_metrics.stage_metrics (
        execution_id, stage_name, stage_index, start_time, end_time,
        duration_ms, input_events, output_events, throughput_eps,
        active_workers, status
    ) VALUES`)
	if err != nil {
		log.Printf("Failed to prepare stage metrics statement: %v", err)
		return
	}
	defer batch.Close()

	for _, m := range metrics {
		if err := batch.Append(
			m.ExecutionID,
			m.StageName,
			m.StageIndex,
			m.StartTime,
			m.EndTime,
			m.DurationMs,
			m.InputEvents,
			m.OutputEvents,
			m.ThroughputEPS,
			m.ActiveWorkers,
			m.Status,
		); err != nil {
			log.Printf("Failed to insert stage metric: %v", err)
		}
	}

	err = batch.Send()
	if err != nil {
		log.Println("Error sending stage metrics metric", err)
	}

	log.Println("stage metrics prepped and sent", metrics)
}

// Similar implementations for other metric types...
func (e *ClickHouseEmitter) processStageMetrics() {
	defer e.wg.Done()

	var batch []StageMetric
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case metric := <-e.stageChan:
			batch = append(batch, metric)
			if len(batch) >= e.config.BufferSize {
				e.flushStageMetrics(batch)
				batch = nil
			}

		case <-ticker.C:
			fmt.Println("ticker expired")
			if len(batch) > 0 {
				e.flushStageMetrics(batch)
				batch = nil
			}

		case <-e.ctx.Done():
			if len(batch) > 0 {
				e.flushStageMetrics(batch)
			}
			return
		}
	}
}

func (e *ClickHouseEmitter) processBatchMetrics() {
	defer e.wg.Done()

	var batch []BatchMetric
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case metric := <-e.batchChan:
			batch = append(batch, metric)
			if len(batch) >= e.config.BufferSize {
				e.flushBatchMetrics(batch)
				batch = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				e.flushBatchMetrics(batch)
				batch = nil
			}

		case <-e.ctx.Done():
			if len(batch) > 0 {
				e.flushBatchMetrics(batch)
			}
			return
		}
	}
}

func (e *ClickHouseEmitter) flushBatchMetrics(metrics []BatchMetric) {
	ctx := context.Background()

	if e.config.Debug {
		log.Printf("Flushing %d batch metrics to ClickHouse", len(metrics))
	}

	batch, err := e.conn.PrepareBatch(ctx, `INSERT INTO pipeline_metrics.batch_metrics (
        execution_id, stage_name, batch_id, worker_id, batch_size, processing_time_ms, timestamp
    ) VALUES`)

	if err != nil {
		log.Printf("Failed to prepare stage metrics statement: %v", err)
		return
	}

	defer batch.Close()

	for _, m := range metrics {
		if err := batch.Append(
			m.ExecutionID,
			m.StageName,
			m.BatchID,
			m.WorkerID,
			m.BatchSize,
			m.ProcessingTimeMs,
			m.Timestamp,
		); err != nil {
			log.Printf("Failed to insert stage metric: %v", err)
		}
	}

	err = batch.Send()
	if err != nil {
		log.Println("Error sending batch metrics metric", err)
	}

	log.Println("Batch metrics is prepped and sent", metrics)
}

func (e *ClickHouseEmitter) flushErrorMetrics(metrics []ErrorMetric) {
	ctx := context.Background()

	if e.config.Debug {
		log.Printf("Flushing %d error metrics to ClickHouse", len(metrics))
	}

	batch, err := e.conn.PrepareBatch(ctx, `INSERT INTO pipeline_metrics.error_metrics (
        execution_id, stage_name, error_message, timestamp
    ) VALUES`)

	if err != nil {
		log.Printf("Failed to prepare stage metrics statement: %v", err)
		return
	}

	defer batch.Close()

	for _, m := range metrics {
		if err := batch.Append(
			m.ExecutionID,
			m.StageName,
			m.ErrorMsg,
			m.Timestamp,
		); err != nil {
			log.Printf("Failed to insert stage metric: %v", err)
		}
	}

	err = batch.Send()
	if err != nil {
		log.Println("Error sending batch metrics metric", err)
	}

	log.Println("Batch metrics is prepped and sent", metrics)
}

func (e *ClickHouseEmitter) processErrorMetrics() {
	defer e.wg.Done()

	var batch []ErrorMetric
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case metric := <-e.errorChan:
			batch = append(batch, metric)
			if len(batch) >= e.config.BufferSize {
				e.flushErrorMetrics(batch)
				batch = nil
			}

		case <-ticker.C:
			if len(batch) > 0 {
				e.flushErrorMetrics(batch)
				batch = nil
			}

		case <-e.ctx.Done():
			if len(batch) > 0 {
				e.flushErrorMetrics(batch)
			}
			return
		}
	}
}

func (e *ClickHouseEmitter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true
	e.cancel()

	// Close channels
	close(e.pipelineChan)
	close(e.stageChan)
	close(e.batchChan)
	close(e.errorChan)

	// Wait for background processors to finish
	e.wg.Wait()

	// Close database connection
	if e.conn != nil {
		return e.conn.Close()
	}

	return nil
}

// Utility function for generating IDs
func GenerateExecutionID() string {
	return uuid.New().String()
}

func GenerateBatchID() string {
	return uuid.New().String()[:8] // Shorter for batch IDs
}

func getValidDuration(duration int64) uint64 {
	if duration < 0 {
		return 0
	}
	var durationUint uint64
	durationUint = uint64(duration)
	return durationUint
}
