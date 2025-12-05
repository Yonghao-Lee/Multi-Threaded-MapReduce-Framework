# Multi-Threaded MapReduce Framework

A C++ implementation of the MapReduce paradigm with multi-threading support for parallel data processing.

## Overview

This framework provides a thread-safe MapReduce implementation that allows clients to process large datasets in parallel across multiple stages: Map → Shuffle → Reduce.

## Features

- **Multi-threaded execution** with configurable thread count
- **Automatic shuffling** and sorting of intermediate results
- **Thread-safe operations** with atomic counters and mutexes
- **Progress tracking** via job state queries
- **Barrier synchronization** between phases

## Building

Using CMake:
```bash
cmake -B build
cmake --build build
```

Using provided Makefile:
```bash
make
```

## Usage

### 1. Implement the Client Interface

```cpp
class MyClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* value, void* context) const override {
        // Your map logic
        emit2(k2, v2, context);
    }

    void reduce(const IntermediateVec* pairs, void* context) const override {
        // Your reduce logic
        emit3(k3, v3, context);
    }
};
```

### 2. Define Key-Value Types

```cpp
class MyK1 : public K1 {
    bool operator<(const K1 &other) const override { /* ... */ }
};
// Implement V1, K2, V2, K3, V3...
```

### 3. Run the Job

```cpp
MyClient client;
InputVec inputVec = /* your input data */;
OutputVec outputVec;

JobHandle job = startMapReduceJob(client, inputVec, outputVec, 4);
waitForJob(job);
closeJobHandle(job);
```

## API Reference

### Core Functions

```cpp
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel);
```
Starts a MapReduce job with specified number of threads.

```cpp
void waitForJob(JobHandle job);
```
Blocks until the job completes.

```cpp
void getJobState(JobHandle job, JobState* state);
```
Queries current job progress (stage and percentage).

```cpp
void closeJobHandle(JobHandle job);
```
Cleans up job resources. Waits for completion if not already done.

### Emit Functions

```cpp
void emit2(K2* key, V2* value, void* context);  // Call in map()
void emit3(K3* key, V3* value, void* context);  // Call in reduce()
```

## Architecture

```
┌─────────────────────────────────────────┐
│         MapReduce Framework             │
├─────────────────────────────────────────┤
│  Input Phase                            │
│    └─ Distribute across threads         │
├─────────────────────────────────────────┤
│  MAP Phase (parallel)                   │
│    └─ Each thread processes inputs      │
│    └─ Emits intermediate (K2,V2) pairs  │
├─────────────────────────────────────────┤
│  SORT Phase (parallel)                  │
│    └─ Each thread sorts its results     │
├─────────────────────────────────────────┤
│  SHUFFLE Phase (single thread)          │
│    └─ Merge-sort all intermediate data  │
│    └─ Group by K2 keys                  │
├─────────────────────────────────────────┤
│  REDUCE Phase (parallel)                │
│    └─ Process each K2 group             │
│    └─ Emits final (K3,V3) pairs         │
└─────────────────────────────────────────┘
```

## Implementation Details

- **Atomic operations** for thread-safe counters
- **Barrier synchronization** between Map/Sort/Shuffle/Reduce phases
- **Per-thread intermediate storage** to minimize lock contention
- **Single atomic variable** for job state (stage + progress)
- **Mutex-protected output** to prevent race conditions

## Files

| File | Purpose |
|------|---------|
| `MapReduceFramework.cpp/h` | Main framework implementation |
| `MapReduceClient.h` | Client interface and type definitions |
| `Barrier.cpp/h` | Thread barrier synchronization |
| `CMakeLists.txt` | CMake build configuration |
| `Makefile` | Alternative build system |

## Thread Safety

- Map phase: Lock-free (per-thread vectors)
- Sort phase: Lock-free (independent sorting)
- Shuffle phase: Single-threaded (thread 0 only)
- Reduce phase: Lock-free reads, mutex-protected output writes

## Requirements

- C++14 or later
- pthreads support
- CMake 3.1+ (for CMake build)

## Testing

Optional sanitizer flags in `CMakeLists.txt`:
- ThreadSanitizer: Detects data races
- AddressSanitizer: Detects memory errors

Uncomment the appropriate lines to enable.

## Author

**Yonghao Lee** 

---

*Part of Operating Systems course project*
