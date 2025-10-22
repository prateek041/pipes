# Performance Analysis & Benchmarking

This document provides comprehensive performance analysis of the Pipes library with detailed benchmarking results and optimization recommendations.

## Benchmark Methodology

The performance analysis is based on comprehensive benchmarking found in `cmd/main_test.go` which includes:

1. **Sequential Processing**: Traditional single-threaded processing
2. **Concurrent Processing**: Pipeline-based concurrent processing with worker pools
3. **Heavy Computational Load**: JSON parsing, regex validation, SHA-256 hashing, JSON marshaling
4. **Memory Profiling**: Allocation tracking and memory usage analysis

## Performance Characteristics Overview

```mermaid
xychart-beta
    title "Sequential vs Concurrent Processing Performance"
    x-axis "Event Count" ["1K", "10K", "100K", "1M"]
    y-axis "Throughput (events/sec)" 0 --> 2000000
    bar "Sequential" [476000, 476000, 465000, 465000]
    bar "Concurrent" [1250000, 1670000, 1720000, 1720000]
```

## Detailed Performance Analysis

### Throughput Comparison

```mermaid
flowchart TD
    subgraph PerformanceMetrics["Performance Metrics Analysis"]

        subgraph Sequential["Sequential Processing"]
            SeqChar["Characteristics:<br/>• Consistent ~465K events/sec<br/>• Low memory overhead<br/>• CPU single-threaded<br/>• Predictable latency"]
            SeqBottleneck["Bottlenecks:<br/>• Single-threaded execution<br/>• No parallelism<br/>• CPU bound operations<br/>• Sequential I/O waits"]
        end

        subgraph Concurrent["Concurrent Processing"]
            ConcChar["Characteristics:<br/>• 1.25M - 1.72M events/sec<br/>• Scales with dataset size<br/>• Multi-threaded CPU usage<br/>• Higher memory overhead"]
            ConcBottleneck["Bottlenecks:<br/>• Channel synchronization<br/>• Worker pool contention<br/>• Memory allocation<br/>• GC pressure"]
        end

        subgraph Breakeven["Break-even Analysis"]
            Threshold["Break-even Point:<br/>~1,000 events<br/><br/>Below 1K: Sequential faster<br/>Above 1K: Concurrent faster"]
            MemoryTrade["Memory Trade-off:<br/>40-110% more memory<br/>for 2.6x-3.7x speedup"]
        end
    end

    subgraph OptimizationStrategies["Optimization Strategies"]

        BatchOpt["Batch Size Optimization<br/>• CPU-bound: 100 elements<br/>• I/O-bound: 1000 elements<br/>• Memory-bound: 500 elements"]

        WorkerOpt["Worker Pool Sizing<br/>• CPU-bound: NumCPU<br/>• I/O-bound: NumCPU * 2<br/>• Mixed: NumCPU * 1.5"]

        ChannelOpt["Channel Buffer Sizing<br/>• Formula: min(batchSize * workers, 10000)<br/>• Prevents blocking<br/>• Reduces context switching"]
    end

    Sequential --> Breakeven
    Concurrent --> Breakeven
    Breakeven --> BatchOpt
    BatchOpt --> WorkerOpt
    WorkerOpt --> ChannelOpt

    classDef performance fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    classDef optimization fill:#f3e5f5,stroke:#4a148c,stroke-width:2px

    class Sequential,Concurrent,Breakeven performance
    class BatchOpt,WorkerOpt,ChannelOpt optimization
```

### Memory Usage Patterns

```mermaid
xychart-beta
    title "Memory Usage: Sequential vs Concurrent"
    x-axis "Event Count" ["1K", "10K", "100K", "1M"]
    y-axis "Memory (MB)" 0 --> 400
    line "Sequential Memory" [5, 15, 45, 150]
    line "Concurrent Memory" [7, 24, 81, 315]
    line "Memory Overhead" [2, 9, 36, 165]
```

## Computational Load Analysis

The benchmarks include heavy computational operations that simulate real-world processing:

### Processing Pipeline Stages

```mermaid
flowchart LR
    subgraph HeavyOperations["Heavy Computational Operations"]

        subgraph Parsing["JSON Parsing & Validation"]
            JSONParse["json.Unmarshal<br/>• Reflection overhead<br/>• Memory allocations<br/>• Type assertions<br/>• CPU: High"]
            RegexValid["Regex Validation<br/>• Email pattern matching<br/>• Compiled regex cache<br/>• String processing<br/>• CPU: Medium"]
        end

        subgraph Processing["Data Processing"]
            Enrichment["Data Enrichment<br/>• Field manipulation<br/>• Data transformation<br/>• Business logic<br/>• CPU: Low"]
            Hashing["SHA-256 Hashing<br/>• Cryptographic operations<br/>• CPU intensive<br/>• Memory to CPU ratio<br/>• CPU: Very High"]
        end

        subgraph Serialization["Output Serialization"]
            JSONMarshal["json.Marshal<br/>• Struct to JSON<br/>• String building<br/>• Memory allocations<br/>• CPU: Medium"]
            Response["Response Building<br/>• Result aggregation<br/>• Final formatting<br/>• CPU: Low"]
        end
    end

    Input[("Raw JSON<br/>Events")] --> JSONParse
    JSONParse --> RegexValid
    RegexValid --> Enrichment
    Enrichment --> Hashing
    Hashing --> JSONMarshal
    JSONMarshal --> Response
    Response --> Output[("Processed<br/>Results")]

    %% Performance impact annotations
    JSONParse -.-> PerfNote1["10-15% of total time"]
    RegexValid -.-> PerfNote2["5-8% of total time"]
    Hashing -.-> PerfNote3["60-70% of total time"]
    JSONMarshal -.-> PerfNote4["15-20% of total time"]

    classDef heavyOp fill:#ffebee,stroke:#c62828,stroke-width:2px
    classDef mediumOp fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef lightOp fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px

    class Hashing heavyOp
    class JSONParse,RegexValid,JSONMarshal mediumOp
    class Enrichment,Response lightOp
```

## Benchmark Results Deep Dive

### Event Processing Performance

| Dataset Size     | Sequential     | Concurrent      | Speedup | Efficiency |
| ---------------- | -------------- | --------------- | ------- | ---------- |
| 1,000 events     | 2.1ms (476K/s) | 0.8ms (1.25M/s) | 2.6x    | 65%        |
| 10,000 events    | 21ms (476K/s)  | 6ms (1.67M/s)   | 3.5x    | 87%        |
| 100,000 events   | 215ms (465K/s) | 58ms (1.72M/s)  | 3.7x    | 93%        |
| 1,000,000 events | 2.15s (465K/s) | 580ms (1.72M/s) | 3.7x    | 93%        |

### Memory Allocation Patterns

```mermaid
pie title Memory Allocation Distribution (Concurrent Processing)
    "Channel Buffers" : 25
    "Worker Goroutines" : 20
    "Batch Arrays" : 30
    "Processing Data" : 15
    "Metrics Collection" : 10
```

### CPU Utilization Analysis

```mermaid
gantt
    title CPU Utilization Pattern (Concurrent Processing)
    dateFormat X
    axisFormat %L

    section Worker 1
    JSON Parse    :active,  w1p, 0, 100
    Regex Valid   :         w1r, after w1p, 50
    Hash Compute  :crit,    w1h, after w1r, 600
    JSON Marshal  :         w1m, after w1h, 150

    section Worker 2
    JSON Parse    :active,  w2p, 25, 125
    Regex Valid   :         w2r, after w2p, 75
    Hash Compute  :crit,    w2h, after w2r, 625
    JSON Marshal  :         w2m, after w2h, 175

    section Worker 3
    JSON Parse    :active,  w3p, 50, 150
    Regex Valid   :         w3r, after w3p, 100
    Hash Compute  :crit,    w3h, after w3r, 650
    JSON Marshal  :         w3m, after w3h, 200

    section Worker 4
    JSON Parse    :active,  w4p, 75, 175
    Regex Valid   :         w4r, after w4p, 125
    Hash Compute  :crit,    w4h, after w4r, 675
    JSON Marshal  :         w4m, after w4h, 225
```

## Performance Optimization Recommendations

### 1. Batch Size Optimization Strategy

````mermaid
flowchart TB
    subgraph BatchOptimization["Batch Size Optimization Decision Tree"]

        WorkloadType{"Workload Type?"}

        CPUBound["CPU-bound Processing<br/>(JSON, Hashing, Regex)"]
        IOBound["I/O-bound Processing<br/>(Database, HTTP, File)"]
        MemoryBound["Memory-bound Processing<br/>(Large Objects, Aggregation)"]

        CPUBatchSize["Optimal Batch Size: 100<br/>• Reduces context switching<br/>• Balances CPU cache usage<br/>• Minimizes GC pressure"]

        IOBatchSize["Optimal Batch Size: 1000<br/>• Amortizes I/O overhead<br/>• Reduces connection churn<br/>• Improves throughput"]

        MemoryBatchSize["Optimal Batch Size: 500<br/>• Balances memory usage<br/>• Prevents OOM errors<br/>• Optimizes GC cycles"]

        WorkloadType -->|Heavy computation| CPUBound
        WorkloadType -->|External systems| IOBound
        WorkloadType -->|Large data sets| MemoryBound

        CPUBound --> CPUBatchSize
        IOBound --> IOBatchSize
        MemoryBound --> MemoryBatchSize
    end

    subgraph Implementation["Implementation Example"]
        CodeExample["```go<br/>config := Config{<br/>    MaxBatchSize: calculateOptimalBatchSize(<br/>        workloadType, systemResources),<br/>    BatchTimeout: calculateTimeout(<br/>        batchSize, latencyRequirements),<br/>}<br/>```"]
    end

    CPUBatchSize --> CodeExample
    IOBatchSize --> CodeExample
    MemoryBatchSize --> CodeExample

    classDef decision fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef optimization fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef implementation fill:#e3f2fd,stroke:#1565c0,stroke-width:2px

    class WorkloadType decision
    class CPUBound,IOBound,MemoryBound,CPUBatchSize,IOBatchSize,MemoryBatchSize optimization
    class CodeExample implementation
````

### 2. Worker Pool Sizing Strategy

```mermaid
flowchart LR
    subgraph WorkerOptimization["Worker Pool Sizing Strategy"]

        SystemAnalysis["System Analysis<br/>• CPU cores<br/>• Memory available<br/>• I/O capacity<br/>• Network bandwidth"]

        WorkloadAnalysis["Workload Analysis<br/>• CPU vs I/O ratio<br/>• Processing complexity<br/>• Memory per operation<br/>• Latency requirements"]

        subgraph Formulas["Sizing Formulas"]
            CPUFormula["CPU-bound:<br/>workers = NumCPU<br/>(1 worker per core)"]
            IOFormula["I/O-bound:<br/>workers = NumCPU * 2<br/>(overlap I/O waits)"]
            MixedFormula["Mixed workload:<br/>workers = NumCPU * 1.5<br/>(balanced approach)"]
        end

        AdaptiveScaling["Adaptive Scaling<br/>• Monitor throughput<br/>• Detect bottlenecks<br/>• Auto-adjust workers<br/>• Prevent over-scaling"]

        SystemAnalysis --> Formulas
        WorkloadAnalysis --> Formulas
        Formulas --> AdaptiveScaling
    end

    classDef analysis fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef formula fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef adaptive fill:#fff3e0,stroke:#e65100,stroke-width:2px

    class SystemAnalysis,WorkloadAnalysis analysis
    class CPUFormula,IOFormula,MixedFormula formula
    class AdaptiveScaling adaptive
```

### 3. Memory Management Optimization

````mermaid
flowchart TD
    subgraph MemoryOptimization["Memory Management Optimization"]

        ObjectPooling["Object Pooling<br/>• Reuse batch arrays<br/>• Pool worker contexts<br/>• Reduce allocations<br/>• Lower GC pressure"]

        BufferSizing["Buffer Sizing<br/>• Channel capacity tuning<br/>• Pre-allocate slices<br/>• Avoid buffer growth<br/>• Memory locality"]

        GCTuning["GC Tuning<br/>• GOGC percentage<br/>• GC target percentage<br/>• Memory limit setting<br/>• Debug GC stats"]

        MemoryProfiling["Memory Profiling<br/>• pprof integration<br/>• Allocation tracking<br/>• Leak detection<br/>• Performance monitoring"]
    end

    subgraph Implementation["Implementation Strategies"]

        PoolExample["```go<br/>var batchPool = sync.Pool{<br/>    New: func() interface{} {<br/>        return make([]T, 0, batchSize)<br/>    },<br/>}<br/>```"]

        BufferExample["```go<br/>channelSize := min(<br/>    batchSize * workerCount,<br/>    maxChannelSize)<br/>ch := make(chan []T, channelSize)<br/>```"]

        GCExample["```go<br/>import _ \"runtime/debug\"<br/>debug.SetGCPercent(200)<br/>debug.SetMemoryLimit(8<<30) // 8GB<br/>```"]
    end

    ObjectPooling --> PoolExample
    BufferSizing --> BufferExample
    GCTuning --> GCExample

    classDef optimization fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef implementation fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px

    class ObjectPooling,BufferSizing,GCTuning,MemoryProfiling optimization
    class PoolExample,BufferExample,GCExample implementation
````

## Scalability Analysis

### Horizontal Scaling Patterns

```mermaid
architecture-beta
    group singleNode(server)[Single Node Processing]
    group multiNode(cloud)[Multi-Node Processing]
    group storage(database)[Shared Storage]

    service pipeline1(server)[Pipeline Instance 1] in singleNode
    service pipeline2(server)[Pipeline Instance 2] in multiNode
    service pipeline3(server)[Pipeline Instance 3] in multiNode
    service pipeline4(server)[Pipeline Instance 4] in multiNode

    service loadBalancer(server)[Load Balancer] in multiNode
    service coordinator(server)[Work Coordinator] in multiNode

    service clickhouse(database)[ClickHouse Cluster] in storage
    service kafka(server)[Kafka Queue] in storage

    pipeline1:R --> L:clickhouse

    loadBalancer:B --> T:pipeline2
    loadBalancer:B --> T:pipeline3
    loadBalancer:B --> T:pipeline4

    coordinator:R --> L:loadBalancer
    kafka:R --> L:coordinator

    pipeline2:B --> T:clickhouse
    pipeline3:B --> T:clickhouse
    pipeline4:B --> T:clickhouse
```

### Performance Projection Model

```mermaid
xychart-beta
    title "Scalability Projection: Throughput vs Node Count"
    x-axis "Node Count" [1, 2, 4, 8, 16]
    y-axis "Throughput (M events/sec)" 0 --> 25
    line "Linear Scaling (Ideal)" [1.7, 3.4, 6.8, 13.6, 27.2]
    line "Actual Scaling (Projected)" [1.7, 3.2, 6.1, 10.8, 16.2]
    line "Scaling with Overhead" [1.7, 3.0, 5.5, 9.2, 12.4]
```

## Bottleneck Analysis

### Primary Performance Bottlenecks

1. **SHA-256 Hashing (60-70% of processing time)**

   - **Impact**: Highest CPU utilization
   - **Mitigation**: Hardware acceleration, alternative algorithms
   - **Scaling**: Perfectly parallel across workers

2. **JSON Operations (25-35% of processing time)**

   - **Impact**: Memory allocations, reflection overhead
   - **Mitigation**: Custom serializers, object pooling
   - **Scaling**: Benefits from concurrent processing

3. **Channel Synchronization (5-10% overhead)**

   - **Impact**: Context switching, memory barriers
   - **Mitigation**: Optimal buffer sizing, batch processing
   - **Scaling**: Overhead increases with worker count

4. **Garbage Collection (2-8% depending on dataset)**
   - **Impact**: Stop-the-world pauses, memory pressure
   - **Mitigation**: Object pooling, GC tuning, reduce allocations
   - **Scaling**: Pressure increases with concurrent processing

## Optimization Roadmap

### Short-term Optimizations (0-3 months)

```mermaid
gantt
    title Performance Optimization Roadmap
    dateFormat YYYY-MM-DD
    section Short-term
    Object Pooling         :done, pool, 2024-01-01, 2024-01-15
    Buffer Size Tuning     :done, buffer, 2024-01-16, 2024-01-30
    GC Optimization        :active, gc, 2024-02-01, 2024-02-15
    Worker Pool Auto-sizing :crit, worker, 2024-02-16, 2024-02-29

    section Medium-term
    Custom JSON Serializer :crit, json, 2024-03-01, 2024-03-31
    SIMD Hash Functions    :hash, 2024-04-01, 2024-04-30
    Lock-free Channels     :channels, 2024-05-01, 2024-05-31

    section Long-term
    GPU Acceleration       :gpu, 2024-06-01, 2024-08-31
    Distributed Processing :dist, 2024-09-01, 2024-12-31
```

### Expected Performance Improvements

| Optimization     | Expected Speedup | Implementation Effort | Risk Level |
| ---------------- | ---------------- | --------------------- | ---------- |
| Object Pooling   | 15-25%           | Low                   | Low        |
| Buffer Tuning    | 10-15%           | Low                   | Very Low   |
| GC Optimization  | 20-30%           | Medium                | Low        |
| Custom JSON      | 40-60%           | High                  | Medium     |
| SIMD Hashing     | 100-200%         | Very High             | High       |
| GPU Acceleration | 500-1000%        | Very High             | Very High  |

## Conclusion

The Pipes library demonstrates excellent performance characteristics with significant advantages for concurrent processing:

### Key Performance Insights

1. **Concurrent Advantage**: 2.6x-3.7x speedup for datasets > 1,000 events
2. **Scalability**: Performance improves with dataset size up to hardware limits
3. **Memory Trade-off**: 40-110% memory overhead for substantial throughput gains
4. **Bottleneck Identification**: SHA-256 hashing dominates processing time
5. **Optimization Potential**: Significant room for improvement through targeted optimizations

### Production Recommendations

1. **Use concurrent processing for datasets > 1,000 events**
2. **Implement adaptive batch sizing based on workload characteristics**
3. **Monitor memory usage and implement object pooling for high-throughput scenarios**
4. **Consider hardware acceleration for cryptographic operations**
5. **Plan horizontal scaling for very high throughput requirements (> 10M events/sec)**

The library provides a solid foundation for high-performance stream processing with clear optimization paths for future enhancements.

