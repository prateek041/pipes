CREATE DATABASE IF NOT EXISTS pipeline_metrics;

CREATE TABLE IF NOT EXISTS pipeline_metrics.pipeline_executions (
    execution_id String,
    pipeline_type String,
    start_time DateTime64(3),
    end_time Nullable(DateTime64(3)),
    duration_ms Nullable(UInt64),
    input_count Nullable(UInt64),
    output_count Nullable(UInt64),
    config_workers UInt32,
    config_batch_size UInt32,
    config_timeout_ms UInt64,
    status String,
    timestamp DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE IF NOT EXISTS pipeline_metrics.stage_metrics (
    execution_id String,
    stage_name String,
    stage_index UInt32,
    start_time DateTime64(3),
    end_time Nullable(DateTime64(3)),
    duration_ms Nullable(UInt64),
    input_events Nullable(UInt64),
    output_events Nullable(UInt64),
    throughput_eps Nullable(Float64),
    active_workers UInt32,
    status String,
    timestamp DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (execution_id, timestamp);

CREATE TABLE IF NOT EXISTS pipeline_metrics.batch_metrics (
    execution_id String,
    stage_name String,
    batch_id String,
    worker_id String,
    batch_size UInt32,
    processing_time_ms UInt64,
    timestamp DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (execution_id, timestamp);

CREATE TABLE IF NOT EXISTS pipeline_metrics.error_metrics (
    execution_id String,
    stage_name String,
    error_msg String,
    timestamp DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
ORDER BY timestamp;

