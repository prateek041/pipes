# Pipeline Execution Flow Diagrams

This document contains detailed sequence diagrams showing the execution flow of the Pipes library.

## Pipeline Execution Sequence

```mermaid
sequenceDiagram
    participant User
    participant Pipeline
    participant FilterStage
    participant MapStage
    participant CollectStage
    participant WorkerPool1
    participant WorkerPool2
    participant Emitter
    participant ClickHouse

    %% Pipeline Setup Phase
    User->>Pipeline: NewPipeline[T](config)
    Pipeline->>Pipeline: Initialize execution ID
    User->>Pipeline: Filter(func(T) bool)
    Pipeline->>FilterStage: Create filter stage
    User->>Pipeline: Map(func(T) T)
    Pipeline->>MapStage: Create map stage
    User->>Pipeline: Collect()
    Pipeline->>CollectStage: Create collect stage

    %% Execution Phase
    User->>Pipeline: Execute(inputChan)
    
    rect rgb(200, 220, 255)
    note right of Pipeline: Pipeline Start Metrics
    Pipeline->>Emitter: EmitPipelineMetric(start)
    Emitter-->>ClickHouse: Async store metrics
    end

    %% Stage Connection Phase
    par FilterStage Connection
        Pipeline->>FilterStage: Connect(wg, inChan, outChan1, emitter)
        FilterStage->>WorkerPool1: Spawn 4 workers
        loop For each worker
            WorkerPool1->>WorkerPool1: Start goroutine
        end
    and MapStage Connection
        Pipeline->>MapStage: Connect(wg, inChan1, outChan2, emitter)
        MapStage->>WorkerPool2: Spawn 4 workers
        loop For each worker
            WorkerPool2->>WorkerPool2: Start goroutine
        end
    and CollectStage Connection
        Pipeline->>CollectStage: Connect(wg, inChan2, nil, emitter)
    end

    %% Processing Phase
    rect rgb(255, 240, 200)
    note over User, ClickHouse: Data Processing Phase
    
    User->>FilterStage: Send batch of T elements
    
    loop Process Filter Batches
        FilterStage->>WorkerPool1: Batch processing
        activate WorkerPool1
        WorkerPool1->>WorkerPool1: Apply filter func
        
        rect rgb(200, 255, 200)
        WorkerPool1->>Emitter: EmitBatchMetric(processing_time, batch_size)
        Emitter-->>ClickHouse: Async store batch metrics
        end
        
        WorkerPool1->>MapStage: Send filtered elements
        deactivate WorkerPool1
        
        MapStage->>WorkerPool2: Batch processing
        activate WorkerPool2
        WorkerPool2->>WorkerPool2: Apply map func
        
        rect rgb(200, 255, 200)
        WorkerPool2->>Emitter: EmitBatchMetric(processing_time, batch_size)
        Emitter-->>ClickHouse: Async store batch metrics
        end
        
        WorkerPool2->>CollectStage: Send mapped elements
        deactivate WorkerPool2
        
        CollectStage->>CollectStage: Store in buffer (mutex protected)
    end
    end

    %% Completion Phase
    rect rgb(255, 200, 200)
    note right of Pipeline: Pipeline Completion
    FilterStage->>Emitter: EmitStageMetric(filter_performance)
    MapStage->>Emitter: EmitStageMetric(map_performance)
    CollectStage->>Emitter: EmitStageMetric(collect_performance)
    
    Pipeline->>Emitter: EmitPipelineMetric(completion)
    Emitter-->>ClickHouse: Async store all metrics
    end

    Pipeline-->>User: Execution complete
    User->>CollectStage: Results()
    CollectStage-->>User: Return processed elements []T
```

## Type Transformation Sequence

```mermaid
sequenceDiagram
    participant User
    participant FirstPipeline as Pipeline[T]
    participant ReduceTransform as ReduceTransformAndStream
    participant SecondPipeline as Pipeline[R]
    participant CollectorT as Collector[T]
    participant CollectorR as Collector[R]
    participant OutputChannel as chan R

    %% First Pipeline Setup
    User->>FirstPipeline: NewPipeline[ComputeEvent](config)
    User->>FirstPipeline: Filter(validationFunc)
    User->>FirstPipeline: Map(enrichmentFunc)
    FirstPipeline->>CollectorT: Collect()

    %% Type Transformation Setup
    User->>ReduceTransform: ReduceTransformAndStream(firstPipeline, reduceFunc, inputChan)
    
    rect rgb(220, 220, 255)
    note over ReduceTransform: Type Transformation: []ComputeEvent -> AggregatedResult
    ReduceTransform->>OutputChannel: Create output chan R
    ReduceTransform->>FirstPipeline: Execute(inputChan)
    end

    %% First Pipeline Execution
    par First Pipeline Processing
        FirstPipeline->>FirstPipeline: Process ComputeEvent elements
        FirstPipeline->>CollectorT: Collect results []ComputeEvent
    and Async Transform
        ReduceTransform->>ReduceTransform: Start goroutine for transformation
    end

    %% Reduction and Streaming
    rect rgb(255, 220, 220)
    note over ReduceTransform: Reduce and Stream Phase
    FirstPipeline-->>ReduceTransform: Processing complete
    ReduceTransform->>CollectorT: Results()
    CollectorT-->>ReduceTransform: Return []ComputeEvent
    ReduceTransform->>ReduceTransform: Apply reduceFunc([]ComputeEvent) -> AggregatedResult
    ReduceTransform->>OutputChannel: Send AggregatedResult
    ReduceTransform-->>User: Return (chan R, Config)
    end

    %% Second Pipeline Setup and Execution
    User->>SecondPipeline: NewPipeline[AggregatedResult](config)
    User->>SecondPipeline: Map(furtherProcessing)
    SecondPipeline->>CollectorR: Collect()
    
    User->>SecondPipeline: Execute(outputChannel)
    SecondPipeline->>SecondPipeline: Process AggregatedResult elements
    SecondPipeline->>CollectorR: Collect final results
    
    SecondPipeline-->>User: Execution complete
    User->>CollectorR: Results()
    CollectorR-->>User: Return []AggregatedResult
```

## Error Handling and Observability Flow

```mermaid
sequenceDiagram
    participant Stage
    participant Worker
    participant Emitter
    participant ErrorBuffer
    participant MetricsBuffer
    participant ClickHouse
    participant FlushScheduler

    %% Normal Processing with Metrics
    Stage->>Worker: Process batch
    activate Worker
    Worker->>Worker: Apply processing function
    
    alt Success Case
        Worker->>Stage: Return processed elements
        rect rgb(200, 255, 200)
        Worker->>Emitter: EmitBatchMetric(success)
        Emitter->>MetricsBuffer: Buffer success metric
        end
    else Error Case
        Worker->>Worker: Handle processing error
        rect rgb(255, 200, 200)
        Worker->>Emitter: EmitErrorMetric(error_details)
        Worker->>Emitter: EmitBatchMetric(failure)
        Emitter->>ErrorBuffer: Buffer error metric
        Emitter->>MetricsBuffer: Buffer failure metric
        end
        Worker->>Stage: Return partial/empty results
    end
    deactivate Worker

    %% Async Metrics Processing
    par Buffer Management
        loop Buffer Monitoring
            FlushScheduler->>FlushScheduler: Check buffer size/timeout
            alt Buffer full or timeout reached
                FlushScheduler->>MetricsBuffer: Flush metrics
                FlushScheduler->>ErrorBuffer: Flush errors
                rect rgb(220, 220, 255)
                MetricsBuffer->>ClickHouse: Batch insert metrics
                ErrorBuffer->>ClickHouse: Batch insert errors
                end
            end
        end
    and Retry Logic
        alt Database connection failed
            ClickHouse--xMetricsBuffer: Connection error
            MetricsBuffer->>MetricsBuffer: Retry with exponential backoff
            MetricsBuffer->>ClickHouse: Retry batch insert
        end
    end

    %% Configuration and Monitoring
    note over Emitter, ClickHouse: Observability Configuration<br/>• Buffer Size: 100-1000<br/>• Flush Interval: 1-30s<br/>• Max Retries: 3<br/>• Retry Backoff: Exponential
```

## Concurrent Processing Pattern

```mermaid
sequenceDiagram
    participant InputChannel as Input chan T
    participant BatchFormer
    participant WorkerPool
    participant Worker1
    participant Worker2
    participant Worker3
    participant Worker4
    participant OutputChannel as Output chan T
    participant Emitter

    %% Batch Formation
    InputChannel->>BatchFormer: Stream of elements
    
    rect rgb(240, 240, 255)
    note over BatchFormer: Batch Formation Logic<br/>Size: 100-1000 elements<br/>Timeout: 50-500ms
    loop Batch Creation
        alt Batch size reached
            BatchFormer->>BatchFormer: Create batch (size limit)
        else Timeout reached
            BatchFormer->>BatchFormer: Create batch (timeout)
        else Input closed
            BatchFormer->>BatchFormer: Create final batch
        end
    end
    end

    %% Worker Pool Distribution
    BatchFormer->>WorkerPool: Distribute batches
    
    par Worker 1 Processing
        WorkerPool->>Worker1: Batch A
        activate Worker1
        Worker1->>Worker1: Process batch A
        Worker1->>Emitter: Batch metrics A
        Worker1->>OutputChannel: Results A
        deactivate Worker1
    and Worker 2 Processing
        WorkerPool->>Worker2: Batch B
        activate Worker2
        Worker2->>Worker2: Process batch B
        Worker2->>Emitter: Batch metrics B
        Worker2->>OutputChannel: Results B
        deactivate Worker2
    and Worker 3 Processing
        WorkerPool->>Worker3: Batch C
        activate Worker3
        Worker3->>Worker3: Process batch C
        Worker3->>Emitter: Batch metrics C
        Worker3->>OutputChannel: Results C
        deactivate Worker3
    and Worker 4 Processing
        WorkerPool->>Worker4: Batch D
        activate Worker4
        Worker4->>Worker4: Process batch D
        Worker4->>Emitter: Batch metrics D
        Worker4->>OutputChannel: Results D
        deactivate Worker4
    end

    %% Completion Synchronization
    rect rgb(200, 255, 200)
    note over Worker1, OutputChannel: Synchronization<br/>WaitGroup ensures all workers complete<br/>before closing output channel
    Worker1->>WorkerPool: Signal completion
    Worker2->>WorkerPool: Signal completion  
    Worker3->>WorkerPool: Signal completion
    Worker4->>WorkerPool: Signal completion
    WorkerPool->>OutputChannel: Close channel
    end
```