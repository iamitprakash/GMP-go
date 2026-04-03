# GMP Scheduler in Go

A high-performance, custom user-space task scheduler written in Go that simulates the core mechanics of Go's built-in GMP (Goroutine, Machine, Processor) concurrency model.

This project was built with a focus on strict thread-safety, modular design, and robust data structures utilizing modern Go Generics.

## Architecture

The project maps closely to the design philosophy of the Go runtime:
- **G (Goroutines -> Tasks)**: The fundamental unit of execution wrapping the user's workload logic (`pkg/gmp/task.go`).
- **P (Processor)**: A logical context containing a local thread-safe **bounded queue** (`pkg/gmp/processor.go`). Processors serve as lightweight localized execution buffers to minimize global lock contention.
- **M (Machine -> Thread)**: System-level OS threads (simulated via goroutines) (`pkg/gmp/machine.go`). The `M` binds to a `P` to pick up local tasks.
- **Scheduler**: Orchestrates the initialization, teardown, and lifecycle management of Processors and Machines, including managing idle machine sleeping via synchronized condition variables (`sync.Cond`).

### Work Stealing

This scheduler guarantees active distribution of load via **work stealing**. Once an `M` exhausts its bound `P`'s localized run queue, it follows a strict algorithm to recover work:
1.  **Anti-Starvation Check**: Every 61 actions, the `M` will peek at the unbounded Global Run Queue.
2.  **Local Check**: Drain the immediate `P` local queue.
3.  **Global Fallback**: Attempt to extract the task from the Global Run Queue.
4.  **Work Stealing**: If all else fails, pick a random `P` from the peer pool and **steal half of its queued workload** to re-balance execution traffic quickly without starving single nodes.

## Directory Structure

```text
/pkg
  /gmp         # Core scheduling mechanics (Task, Machine, Processor, Scheduler)
  /queue       # Generic data structures (Bounded & Unbounded queues)
/examples
  main.go      # Throughput integration load tester
```

## Getting Started

### Prerequisites
- Go 1.18+ (Utilizes Generics natively)

### Testing
Run the comprehensive suite of work-stealing, unit, race-condition, and integration tests directly:

```bash
go test -v -race ./...
```

### Running the Example
A heavy-throughput test validating scale processing is provided in `examples/main.go`. It spins up logical cores equal to your hardware and processes 1,000,000 tasks dynamically:

```bash
go run examples/main.go
```
