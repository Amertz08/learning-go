# Net projects

Had ChatGPT generate some projects to refine my networking knowledge

# 🧩 1. TCP Echo Server + Client

**Goal:** Learn the basics of `net.Conn`

## Prompt
Build a TCP server that:
- Listens on a port
- Accepts multiple clients concurrently
- Echoes back whatever the client sends

Then build a client that:
- Connects to the server
- Sends messages
- Prints responses

## Focus
- `net.Listen`, `net.Dial`
- goroutines per connection
- basic I/O (`Read`, `Write`)

---

# 🧩 2. Line-Based Chat Server

**Goal:** Introduce shared state + broadcasting

## Prompt
Build a TCP chat server where:
- Multiple clients can connect
- Each message is broadcast to all connected clients
- Each client has a username

## Constraints
- Use channels for communication
- No global locks if possible

## Focus
- connection registry
- fan-out pattern
- synchronization

---

# 🧩 3. Concurrent Log Processing Server (Pipeline)

**Goal:** Combine networking + your pipeline knowledge

## Prompt
Build a TCP server that:
- Accepts log lines from clients
- Processes them through a pipeline:
    - parse → filter (ERROR only) → transform
- Returns processed results to the client

## Requirements
- Each stage is a goroutine
- Use channels between stages
- Support cancellation

## Focus
- backpressure
- pipeline composition
- streaming data over TCP

---

# 🧩 4. Rate-Limited API Gateway

**Goal:** Introduce control + resource management

## Prompt
Build a TCP or HTTP service that:
- Accepts requests
- Limits concurrent processing (e.g., max 10 in-flight)
- Rejects or queues excess requests

## Constraints
- Use a semaphore pattern
- Add timeouts

## Focus
- concurrency limiting
- fairness
- system stability under load

---

# 🧩 5. Simple HTTP Server (from scratch)

**Goal:** Understand protocols over TCP

## Prompt
Using only `net`, implement a minimal HTTP server:
- Parse raw HTTP requests manually
- Return basic responses

## Bonus
- Support multiple routes
- Handle headers

## Focus
- protocol parsing
- request framing
- string/byte processing

---

# 🧩 6. TCP Proxy (Man-in-the-Middle)

**Goal:** Learn bidirectional streaming

## Prompt
Build a proxy that:
- Accepts client connections
- Forwards traffic to another server
- Relays responses back

## Requirements
- Full duplex communication
- Handle disconnects gracefully

## Focus
- `io.Copy`
- stream forwarding
- lifecycle management

---

# 🧩 7. Connection Pool

**Goal:** Resource reuse

## Prompt
Build a connection pool for TCP clients:
- Reuse connections instead of dialing each time
- Limit max connections
- Handle stale connections

## Focus
- pooling patterns
- synchronization
- cleanup

---

# 🧩 8. Distributed Worker System

**Goal:** Real-world system design

## Prompt
Build:
- A server that distributes jobs
- Multiple worker clients that:
    - connect to server
    - receive jobs
    - send results back

## Requirements
- retry failed jobs
- handle worker disconnects

## Focus
- message protocols
- reliability
- coordination

---

# 🧩 9. File Transfer Protocol

**Goal:** Large data + streaming

## Prompt
Build a client/server system that:
- uploads/downloads files over TCP
- supports large files (streaming, not full memory)

## Bonus
- resume interrupted transfers

## Focus
- chunking
- buffering
- throughput

---

# 🧩 10. Observability + Profiling

**Goal:** Production readiness

## Prompt
Instrument one of your servers:
- expose `pprof`
- measure latency
- track memory usage

## Focus
- performance tuning
- bottleneck identification

---

# 🧠 How to Approach These

For each project, ask yourself:

- Where can this block?
- Where do I need backpressure?
- What happens on failure?
- How do I shut this down cleanly?

---

# 🔥 Suggested Order (based on your level)

Given what you’ve already been doing:

1. Echo server
2. Chat server
3. Pipeline server ⭐
4. Proxy
5. Worker system  