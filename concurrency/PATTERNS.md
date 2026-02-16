# 1️⃣ Pipeline Pattern Problem

## Problem: Streaming Log Processing System

You are building a service that processes a continuous stream of log entries coming from a TCP connection.

Each log entry must go through these stages:

1. Parse raw bytes into structured log objects
2. Filter out logs below a severity threshold
3. Enrich logs with metadata (IP lookup simulation — add 10ms delay per entry)
4. Serialize to JSON
5. Send to an output writer

Constraints:

- Logs arrive continuously (unbounded stream).
- The enrichment stage is slower than the others.
- The system must stop cleanly if the context is canceled.
- No stage may leak goroutines.
- No stage may buffer unbounded memory.

Design the pipeline so:

- Each stage is isolated.
- Backpressure propagates correctly.
- Cancellation shuts down all stages.

Do not implement it yet — just design the structure.


# 2️⃣ Fan-Out / Fan-In Problem

## Problem: Parallel Image Hashing

You are given 50,000 image file paths.

You must:

- Read each file
- Compute a SHA256 hash
- Return a slice of results

Constraints:

- File I/O is moderately slow.
- You have 8 CPU cores.
- Order of results does not matter.
- The system must shut down cleanly if the context is canceled.
- The output channel must close correctly without panic.

Design:

- How many workers?
- How do you distribute work?
- How do you merge results?
- How do you know when you’re done?

Avoid:

- Spawning 50,000 goroutines at once.
- Data races on shared slices.


# 3️⃣ Bounded Concurrency / Backpressure Problem

## Problem: HTTP Fan-Out Service

Your service receives HTTP requests.

For each incoming request:

- It must call up to 20 downstream services.
- Aggregate their responses.
- Return a combined result.

Constraints:

- You must limit total concurrent outbound calls across the entire service to 100.
- If downstream services slow down, your system must not explode in goroutines.
- Each request has a timeout.
- If the client disconnects, all downstream work must stop.

Design:

- How do you enforce the global concurrency limit?
- How do you prevent unbounded goroutine growth?
- Where does backpressure occur?
- How does cancellation propagate?


# 4️⃣ Worker Pool Problem

## Problem: Job Queue Processor

You have a channel that receives jobs from an external queue.

Each job:

- Takes between 50ms and 2s.
- May fail and need retry.
- Should not block the system indefinitely.

Constraints:

- You want at most 10 jobs executing at once.
- Failed jobs should be retried up to 3 times.
- The system must shut down gracefully on context cancellation.
- No job should be lost.
- No goroutine leaks.

Design:

- Worker lifecycle.
- Shutdown sequence.
- Retry strategy.
- Where and when channels are closed.
