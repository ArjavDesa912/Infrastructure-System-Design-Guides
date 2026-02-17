# Part 1: Async Processing, Resilience & API Design

> **Scope:** This guide consolidates three core infrastructure topics — distributed async task queues, retry/DLQ/circuit-breaker infrastructure, and idempotent API gateways — into one reference. It also covers HTTP status codes, retriable vs. non-retriable errors, exponential backoff with jitter, and real-world candidate feedback on deploying production services in the cloud.

---

## Candidate Feedback: What Interviewers Actually Ask

A former candidate's debrief on the **"Deploy a Service in the Cloud"** round:

> *"This was more infrastructure-oriented than a classic system design. I was asked to lay out how a production-grade service would function once live. The interviewer expected me to think aloud while building a service that could: scale under heavy load (horizontal scaling, auto-scaling groups, etc.), use reliable storage and durable queues for async operations, handle failures gracefully with retries, timeouts, and circuit breakers, and be observable with logs, metrics, alerts, dashboards. What mattered most wasn't the completeness, but the tradeoffs I made, my prioritization, and how I adapted when new constraints were introduced (like cost-efficiency, high availability, or multi-region support)."*

**Key takeaway:** The interviewer wants you to demonstrate **tradeoff thinking** — not a perfect architecture. Below, every section is structured so you can adapt on the fly when the interviewer shifts constraints (cost → availability → multi-region).

---

# Section A: Distributed Async Task Queue

> **Interview Prompt:** "SnowConvert receives thousands of legacy SQL files per hour. Each file needs AST parsing, semantic analysis, and transpilation — jobs that take 5 seconds to 15 minutes each. Design the async task queue infrastructure that powers this."

---

## A.1 Requirements

### Functional
- Accept file conversion jobs via API and enqueue them for async processing.
- Support heterogeneous job durations (5s syntax check → 15min stored procedure with 4,000 lines of PL/SQL).
- Provide real-time job status (PENDING → RUNNING → COMPLETED/FAILED) via polling or push.
- Support job prioritization (enterprise SLA tier vs. free-tier).
- Guarantee at-least-once execution with idempotent workers.
- Allow job cancellation and timeout enforcement.

### Non-Functional
- **Throughput:** 10,000 jobs/minute at peak.
- **Latency:** P99 enqueue < 50ms; P99 time-to-pickup < 2s for high-priority jobs.
- **Durability:** Zero job loss — a job accepted is a job that will eventually run.
- **Availability:** 99.95% uptime on the control plane.
- **Scalability:** Horizontal scaling from 10 to 500 workers on GKE.

### Capacity Estimation
```
Peak load:         10,000 jobs/min ≈ 167 jobs/sec
Avg job payload:   50 KB (SQL file + metadata)
Avg job duration:  30 seconds
Concurrent jobs:   167 × 30 = ~5,000 concurrent jobs
Worker pool:       5,000 / 1 job-per-worker = 5,000 pods (or fewer with faster jobs)
                   Realistic: 80% of jobs < 10s → ~500-1,000 pods at peak
Queue throughput:  167 writes/sec + 167 reads/sec = ~334 ops/sec on broker
Storage (24hr):    10K jobs/min × 60 × 24 × 50KB ≈ 720 GB/day (job payloads in object storage)
```

---

## A.2 API Design

### Submit Job
```
POST /v1/jobs
Content-Type: application/json
X-Idempotency-Key: <client-generated-uuid>

{
  "source_dialect": "oracle_plsql",
  "target_dialect": "snowflake",
  "file_ref": "gs://uploads/tenant-42/batch-7/proc_get_orders.sql",
  "priority": "high",
  "callback_url": "https://client.example.com/hooks/conversion",
  "metadata": {
    "tenant_id": "tenant-42",
    "batch_id": "batch-7",
    "original_filename": "proc_get_orders.sql"
  }
}

Response 202 Accepted:
{
  "job_id": "job-a1b2c3d4",
  "status": "PENDING",
  "estimated_wait_seconds": 12,
  "status_url": "/v1/jobs/job-a1b2c3d4"
}
```

### Poll Job Status
```
GET /v1/jobs/{job_id}

Response 200:
{
  "job_id": "job-a1b2c3d4",
  "status": "COMPLETED",
  "result": {
    "output_ref": "gs://outputs/tenant-42/batch-7/proc_get_orders.sf.sql",
    "warnings": 3,
    "errors": 0,
    "conversion_score": 0.97
  },
  "timings": {
    "enqueued_at": "2025-01-15T10:00:00Z",
    "started_at": "2025-01-15T10:00:02Z",
    "completed_at": "2025-01-15T10:00:34Z",
    "wall_time_seconds": 32
  }
}
```

### Cancel Job
```
DELETE /v1/jobs/{job_id}

Response 200:
{ "job_id": "job-a1b2c3d4", "status": "CANCELLED" }
```

---

## A.3 High-Level Architecture

```
                         ┌──────────────────────────────────────┐
                         │          API Gateway (Envoy)          │
                         │  Rate limiting, auth, idempotency     │
                         └──────────────┬───────────────────────┘
                                        │
                         ┌──────────────▼───────────────────────┐
                         │       Job Submission Service          │
                         │  - Validates payload                  │
                         │  - Writes job record to PostgreSQL    │
                         │  - Publishes to Pub/Sub topic         │
                         └──────────┬───────────┬───────────────┘
                                    │           │
                    ┌───────────────▼──┐   ┌────▼─────────────────┐
                    │   PostgreSQL      │   │  Google Cloud Pub/Sub │
                    │   (Job Registry)  │   │  (Durable Queue)      │
                    │   - job_id        │   │                       │
                    │   - status        │   │  Topics:              │
                    │   - priority      │   │   priority-high       │
                    │   - timestamps    │   │   priority-normal     │
                    │   - result_ref    │   │   priority-low        │
                    └──────────────────┘   └────┬──────────────────┘
                                                │
                         ┌──────────────────────▼───────────────┐
                         │       Worker Pool (GKE)               │
                         │  ┌─────────┐ ┌─────────┐ ┌─────────┐│
                         │  │Worker-1 │ │Worker-2 │ │Worker-N ││
                         │  │  AST    │ │  AST    │ │  AST    ││
                         │  │ Parser  │ │ Parser  │ │ Parser  ││
                         │  └────┬────┘ └────┬────┘ └────┬────┘│
                         │       │           │           │      │
                         │  HPA auto-scales based on     │      │
                         │  Pub/Sub subscription backlog │      │
                         └──────────────────────────────────────┘
                                        │
                         ┌──────────────▼───────────────────────┐
                         │       GCS (Output Storage)            │
                         │  - Converted Snowflake SQL files      │
                         │  - Conversion reports & diffs          │
                         └──────────────────────────────────────┘
```

### Component Responsibilities

| Component | Technology | Role |
|-----------|-----------|------|
| API Gateway | Envoy on GKE | Rate limiting, TLS termination, idempotency key dedup |
| Job Submission | Go microservice | Validation, persistence, enqueue |
| Job Registry | Cloud SQL (PostgreSQL) | Source of truth for job state |
| Durable Queue | Google Cloud Pub/Sub | Decouples submission from processing, at-least-once delivery |
| Worker Pool | GKE pods (Rust/Python) | AST parsing, transpilation, validation |
| Output Storage | GCS | Immutable output artifacts |
| Autoscaler | GKE HPA + custom metrics | Scales workers based on queue depth |

---

## A.4 Deep Dive: Critical Design Decisions

### A.4.1 Why Pub/Sub Over Self-Hosted Kafka?

| Factor | Pub/Sub | Self-Hosted Kafka |
|--------|---------|-------------------|
| Ops burden | Zero (fully managed) | Broker management, ZK, upgrades |
| Scaling | Automatic | Manual partition rebalancing |
| At-least-once | Built-in with ack deadlines | Requires careful offset management |
| Cost at 10K msg/min | ~$40/month | 3-node cluster ~$600/month |
| Ordering | Per-key ordering available | Per-partition FIFO |
| Replay | Seek to timestamp | Offset reset |

**Trade-off:** We sacrifice some control over partitioning for zero operational overhead. For a migration product where engineering focus should be on the parser, not the queue, this is the right call.

> *From my experience deploying enterprise microservices on GKE, operational simplicity of managed services drastically reduces the blast radius of infrastructure incidents.*

### A.4.2 Priority Queue Design

```
Three Pub/Sub topics with priority-based subscription:

  priority-high:    ──── Worker pulls from this FIRST
  priority-normal:  ──── Workers check this when high is empty
  priority-low:     ──── Processed only when normal is drained

Worker pull logic (pseudocode):
  loop:
    msg = pull(priority-high, timeout=100ms)
    if msg == nil:
      msg = pull(priority-normal, timeout=100ms)
    if msg == nil:
      msg = pull(priority-low, timeout=500ms)
    if msg != nil:
      process(msg)
```

**Why not a single topic with priority field?** Pub/Sub doesn't natively support priority ordering within a topic. Using separate topics gives us hard priority lanes — a burst of 10,000 free-tier jobs won't delay enterprise SLA jobs.

### A.4.3 Worker Lifecycle & Heartbeats

```
Worker Process:
  1. Pull message from Pub/Sub (ack deadline = 10 minutes)
  2. Write status = RUNNING to PostgreSQL
  3. Begin AST parsing + transpilation
  4. Every 60 seconds: extend ack deadline by 5 minutes (heartbeat)
  5. On success:
     a. Upload output to GCS
     b. Write status = COMPLETED + result_ref to PostgreSQL
     c. ACK the Pub/Sub message
  6. On failure:
     a. Write status = FAILED + error details
     b. NACK the message (returns to queue for retry)
  7. On timeout (ack deadline expires):
     → Pub/Sub automatically redelivers to another worker
```

**Why extend ack deadline instead of setting a very long one?** If a worker OOMs or gets evicted mid-job, we want fast redelivery. A 10-minute initial deadline means worst-case redelivery latency is 10 minutes for a dead worker. The heartbeat extension handles legitimately long jobs.

> *This is analogous to how I designed heartbeat mechanisms in VibeDB — the storage engine uses a similar lease-renewal pattern for distributed lock coordination to prevent stale locks from blocking writes.*

### A.4.4 Autoscaling Strategy on GKE

```yaml
# HPA with custom Pub/Sub metric
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: conversion-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: conversion-worker
  minReplicas: 5
  maxReplicas: 500
  metrics:
  - type: External
    external:
      metric:
        name: pubsub.googleapis.com|subscription|num_undelivered_messages
        selector:
          matchLabels:
            resource.labels.subscription_id: conversion-jobs-sub
      target:
        type: AverageValue
        averageValue: "10"   # Scale up when > 10 msgs per worker
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 30
      policies:
      - type: Percent
        value: 100           # Double the fleet every 30s if needed
        periodSeconds: 30
    scaleDown:
      stabilizationWindowSeconds: 300   # Wait 5 min before scaling down
      policies:
      - type: Percent
        value: 10            # Remove 10% of fleet per minute (graceful)
        periodSeconds: 60
```

**Key decisions:**
- **Aggressive scale-up, conservative scale-down** — prevents flapping during bursty enterprise batch uploads.
- **Target: 10 messages per worker** — keeps queue depth manageable while avoiding over-provisioning.
- **Min 5 replicas** — ensures sub-2s pickup time even during low traffic.

### A.4.5 Job State Machine

```
                    ┌───────────┐
                    │  PENDING   │
                    └─────┬─────┘
                          │ Worker picks up
                    ┌─────▼─────┐
          ┌────────│  RUNNING   │────────┐
          │         └─────┬─────┘         │
          │ Timeout/OOM   │ Success       │ Unrecoverable
          │               │               │ error
    ┌─────▼─────┐   ┌────▼──────┐  ┌─────▼─────┐
    │ RETRYING   │   │ COMPLETED │  │  FAILED    │
    └─────┬─────┘   └───────────┘  └───────────┘
          │ Back to queue                  ▲
          │ (max 3 retries)                │
          └────────────────────────────────┘
                    After max retries
```

### A.4.6 Idempotent Job Execution

A Pub/Sub NACK or ack-deadline expiry causes redelivery. Workers must be idempotent:

```
Worker.process(job):
  // 1. Check if already completed (idempotent guard)
  existing = db.get_job(job.id)
  if existing.status == COMPLETED:
    ack(message)  // Already done, skip
    return

  // 2. Generate deterministic output path
  output_path = f"gs://outputs/{job.tenant_id}/{job.batch_id}/{job.file_hash}.sf.sql"

  // 3. Parse and convert (pure function — same input = same output)
  result = transpile(job.source_file, job.source_dialect, job.target_dialect)

  // 4. Upload output (GCS upload is idempotent — overwrites with same content)
  gcs.upload(output_path, result)

  // 5. Update job status atomically
  db.update_job(job.id, status=COMPLETED, output_ref=output_path)

  // 6. ACK
  ack(message)
```

---

## A.5 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Worker OOM on large file** | Job stalls, ack deadline expires | Memory limits per pod + GKE eviction → message redelivered to different worker with more headroom |
| **PostgreSQL down** | Can't update job status | Workers retry DB writes with exponential backoff; job status eventually consistent |
| **Pub/Sub outage** | Can't enqueue/dequeue | API returns 503, client retries; SLA: Pub/Sub has 99.95% availability |
| **GCS upload fails** | Output lost | Retry upload 3x; on persistent failure, mark job FAILED with clear error |
| **Poison message (unparseable SQL)** | Worker crashes repeatedly | After 3 NACK cycles, route to Dead Letter Topic for manual review |
| **Node preemption (spot instances)** | Worker killed mid-job | Graceful shutdown handler: NACK current message before exit → redelivery |
| **Thundering herd (batch upload)** | 50K jobs at once | Priority queues absorb burst; HPA scales workers; rate limit per tenant at API gateway |

---

## A.6 Trade-offs & Design Decisions

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Pub/Sub over Kafka | Self-hosted Kafka on GKE | Zero ops overhead; Kafka only justified at >100K msg/sec sustained |
| PostgreSQL for job state | Redis | Jobs must survive restarts; Redis persistence adds complexity for no gain |
| Separate priority topics | Single topic + priority field | Hard isolation prevents starvation of high-priority jobs |
| Pull-based workers | Push-based delivery | Workers control concurrency; avoids overwhelming slow workers |
| GCS for outputs | Inline in DB | Outputs can be 100MB+; GCS is cheaper and supports streaming reads |

---

## A.7 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not use a simple database queue with SELECT FOR UPDATE?" | "Database queues work at low scale but create lock contention under high throughput. At 167 jobs/sec, the polling overhead and row-level locking would make PostgreSQL the bottleneck. A dedicated message broker gives us push-based delivery, built-in dead-lettering, and decouples producers from consumers." |
| "What if Pub/Sub loses messages?" | "Pub/Sub guarantees at-least-once delivery with message persistence to multiple zones. The real risk isn't message loss — it's message duplication. That's why workers are idempotent: same input always produces same output, and the DB status check prevents double-processing." |
| "How do you handle a 15-minute job? The ack deadline will expire." | "Workers extend the ack deadline every 60 seconds via a heartbeat goroutine. If the worker dies and stops heartbeating, the deadline expires and Pub/Sub redelivers the message within 10 minutes. This balances fast recovery from dead workers with support for legitimately long jobs." |
| "500 pods on GKE sounds expensive." | "We use spot/preemptible VMs for the worker pool — 60-80% cost reduction. Since workers are stateless and idempotent, preemption is safe. We NACK the current message on SIGTERM and another worker picks it up. The fleet auto-scales down to 5 pods during off-peak hours." |
| "What about exactly-once processing?" | "Exactly-once is an illusion in distributed systems. We implement effectively-once via idempotent workers: deterministic output paths, DB status checks, and GCS overwrite semantics. The cost of a duplicate execution is a few seconds of wasted compute, which is far cheaper than building a distributed transaction coordinator." |

---

# Section B: Dead Letter Queue, Retry Manager & Circuit Breakers

> **Interview Prompt:** "A SnowConvert worker fails to parse a 3,000-line Oracle stored procedure. The job has already been retried 5 times. Design the retry infrastructure — including exponential backoff, jitter, circuit breakers, and a dead letter queue — that ensures no job is silently lost and transient failures self-heal."

---

## B.1 Requirements

### Functional
- Automatically retry failed conversion jobs with configurable retry policies.
- Implement exponential backoff with full jitter to prevent thundering herds.
- Route permanently failed jobs to a Dead Letter Queue (DLQ) with full diagnostic context.
- Provide a circuit breaker that halts retries when a downstream dependency is unhealthy.
- Expose a DLQ management UI: inspect, replay, purge, and bulk-retry.
- Support per-tenant and per-error-class retry policies.

### Non-Functional
- **Zero silent job loss:** Every accepted job must reach COMPLETED or land in the DLQ.
- **Retry latency budget:** Total retry window ≤ 30 minutes per job before DLQ routing.
- **Observability:** Every retry attempt is logged with structured metadata.
- **DLQ throughput:** Support replaying 50,000 DLQ items/hour during batch recovery.

### Capacity Estimation
```
Job failure rate:     ~5% of 10K jobs/min = 500 failures/min
Avg retries/failure:  3 attempts
Total retry volume:   500 × 3 = 1,500 retry events/min
DLQ ingest rate:      ~2% of failures exhaust retries = 10 jobs/min → DLQ
DLQ storage (30 days): 10/min × 60 × 24 × 30 × 10KB = ~4.3 GB
Circuit breaker evaluations: 167 checks/sec (every job checks circuit state)
```

---

## B.2 API Design

### Retry Policy Configuration
```
PUT /v1/retry-policies/{policy_id}
{
  "max_retries": 5,
  "base_delay_ms": 1000,
  "max_delay_ms": 60000,
  "backoff_multiplier": 2.0,
  "jitter_strategy": "full",       // "full" | "equal" | "decorrelated"
  "retriable_error_classes": [
    "TRANSIENT_NETWORK",
    "WORKER_OOM",
    "DEPENDENCY_TIMEOUT"
  ],
  "non_retriable_error_classes": [
    "INVALID_SQL_SYNTAX",
    "UNSUPPORTED_DIALECT",
    "AUTH_FAILURE"
  ]
}
```

### DLQ Management
```
GET  /v1/dlq/items?tenant_id=42&status=PENDING&limit=50
POST /v1/dlq/items/{item_id}/replay       // Replay a single item
POST /v1/dlq/bulk-replay                   // Replay filtered batch
     { "filter": { "error_class": "WORKER_OOM", "created_after": "2025-01-14T00:00:00Z" } }
DELETE /v1/dlq/items/{item_id}             // Purge (acknowledge as unrecoverable)
```

### Circuit Breaker Status
```
GET /v1/circuit-breakers

Response:
{
  "circuits": [
    {
      "name": "ast-parser-oracle",
      "state": "HALF_OPEN",
      "failure_count": 47,
      "last_failure": "2025-01-15T10:05:32Z",
      "next_probe_at": "2025-01-15T10:06:02Z"
    }
  ]
}
```

---

## B.3 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Job Processing Pipeline                      │
│                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │  Pub/Sub  │───▶│    Worker     │───▶│   Result Handler      │  │
│  │  (Main Q) │    │              │    │   - Success → DB      │  │
│  └──────────┘    │  ┌────────┐  │    │   - Failure → Retry   │  │
│       ▲          │  │Circuit │  │    │     Manager            │  │
│       │          │  │Breaker │  │    └───────────┬───────────┘  │
│       │          │  │ Check  │  │                │              │
│       │          │  └────────┘  │                │              │
│       │          └──────────────┘                │              │
│       │                                          │              │
│       │          ┌──────────────────────────────▼────────────┐  │
│       │          │           Retry Manager                    │  │
│       │          │                                            │  │
│       │          │  1. Classify error (retriable?)            │  │
│       │          │  2. Check retry count < max?               │  │
│       │          │  3. Calculate delay (exp backoff + jitter) │  │
│       │          │  4. Check circuit breaker state            │  │
│       │          │                                            │  │
│       │          │  ┌─────────┐   ┌────────────┐             │  │
│       │          │  │ Retriable│   │Non-Retriable│             │  │
│       │          │  │ & Under  │   │ OR Max      │             │  │
│       │          │  │ Max      │   │ Retries Hit │             │  │
│       │          │  └────┬────┘   └──────┬─────┘             │  │
│       │          └───────│───────────────│────────────────────┘  │
│       │                  │               │                       │
│       │    ┌─────────────▼──┐   ┌───────▼──────────────┐       │
│       └────│ Cloud Tasks     │   │   Dead Letter Queue   │       │
│            │ (Delayed Retry) │   │   (Cloud Pub/Sub DLT) │       │
│            │                 │   │                       │       │
│            │ Schedule msg    │   │ Full diagnostic ctx:  │       │
│            │ with delay      │   │ - Original payload    │       │
│            └─────────────────┘   │ - All retry attempts  │       │
│                                  │ - Stack traces        │       │
│                                  │ - Circuit breaker st. │       │
│                                  └───────┬───────────────┘       │
│                                          │                       │
│                               ┌──────────▼───────────┐          │
│                               │   DLQ Management API  │          │
│                               │   (Inspect / Replay)  │          │
│                               └──────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

---

## B.4 Deep Dive: Exponential Backoff & Jitter

### B.4.1 Why Backoff Matters

**Naive retry** (immediate, fixed-interval) causes retry storms — if 100 requests all fail at the same time, retrying them all simultaneously doubles the load on an already struggling service.

### B.4.2 Exponential Backoff (Naive)

```
delay = base × 2^attempt

  Attempt 0: 1s, Attempt 1: 2s, Attempt 2: 4s, Attempt 3: 8s
  Problem: 100 workers all retry at exactly 1s, 2s, 4s... → synchronized waves
```

### B.4.3 Full Jitter (Recommended)

Randomizes the delay uniformly between 0 and the exponential ceiling:

```
Full Jitter:  delay = random(0, min(cap, base × 2^attempt))

  Attempt 0: random(0, 1s)    → e.g., 0.7s
  Attempt 1: random(0, 2s)    → e.g., 1.3s
  Attempt 2: random(0, 4s)    → e.g., 0.5s
  Attempt 3: random(0, 8s)    → e.g., 6.2s
  Attempt 4: random(0, 16s)   → e.g., 11.8s
  Attempt 5: random(0, 32s)   → e.g., 28.1s
  Cap:       random(0, 60s)   → bounded maximum
```

**Why full jitter over equal jitter?**
- **Equal jitter:** `delay = base × 2^attempt / 2 + random(0, base × 2^attempt / 2)` — still clusters retries in the upper half.
- **Full jitter:** Uniformly spreads retries across the entire window. AWS's research shows full jitter completes work with the fewest total calls under contention.

```python
import random

def calculate_retry_delay(attempt: int, base_ms: int = 1000, cap_ms: int = 60000) -> int:
    """Full jitter exponential backoff per AWS architecture blog."""
    exp_delay = min(cap_ms, base_ms * (2 ** attempt))
    return random.randint(0, exp_delay)
```

### B.4.4 Decorrelated Jitter (Alternative)

```
Decorrelated:  delay = min(cap, random(base, prev_delay × 3))

  More aggressive spread than full jitter.
  Each delay is based on the PREVIOUS delay, not the attempt number.
  Better for systems where retry timing matters more than ordering.
```

### B.4.5 When the Interviewer Asks About Jitter

> "Why not just retry immediately?"
> → "Immediate retries amplify the problem. If a service is overloaded and 100 requests fail, retrying all 100 immediately doubles the load. Exponential backoff gives the service breathing room, and jitter ensures retries don't arrive in synchronized waves. This is critical for production resilience."

---

## B.5 Deep Dive: HTTP Status Codes — Retriable vs. Non-Retriable

This is a frequently asked topic. Know it cold:

### Retriable Status Codes (client SHOULD retry with backoff)

| Code | Name | Meaning | Retry Strategy |
|------|------|---------|----------------|
| **429** | Too Many Requests | Rate limited | Respect `Retry-After` header; exponential backoff |
| **500** | Internal Server Error | Transient server bug | Retry with backoff; likely fixes itself |
| **502** | Bad Gateway | Upstream server crashed | Retry after brief delay |
| **503** | Service Unavailable | Overloaded / maintenance | Retry with longer backoff; check `Retry-After` |
| **504** | Gateway Timeout | Upstream too slow | Retry; possibly increase timeout |

### Non-Retriable Status Codes (client MUST NOT retry with same request)

| Code | Name | Meaning | Why Not Retry? |
|------|------|---------|----------------|
| **400** | Bad Request | Invalid payload | Same input → same error. Fix the request. |
| **401** | Unauthorized | Bad/missing credentials | Same creds → same failure. Re-authenticate. |
| **403** | Forbidden | No permission | Same identity → same denial. |
| **404** | Not Found | Wrong URL/resource gone | Resource doesn't exist. |
| **409** | Conflict | Logical conflict | State-dependent; resolve conflict first. |
| **413** | Payload Too Large | Body exceeds limit | Same payload → same rejection. |
| **422** | Unprocessable Entity | Semantic validation failure | Structurally valid but logically wrong. |

### The Key Principle

**Deterministic outcomes should not be retried.** 4xx errors are deterministic — the same input will always produce the same error. 5xx errors are non-deterministic — the same input might succeed on retry because the server-side condition may have cleared.

> **Interview tip:** Be ready to explain what `Retry-After` is — it's an HTTP response header (returned with 429 or 503) that tells the client how many seconds to wait before retrying. A well-behaved client respects this header instead of using its own backoff calculation.

---

## B.6 Deep Dive: Error Classification Engine

Not all errors deserve retries. Retrying a syntax error 5 times wastes compute:

```
Error Classification Matrix:

┌─────────────────────────┬──────────┬──────────┬────────────────┐
│ Error Class             │ Retriable│ Max Retry│ Notes          │
├─────────────────────────┼──────────┼──────────┼────────────────┤
│ TRANSIENT_NETWORK       │ ✅ Yes   │ 5        │ DNS, TCP reset │
│ WORKER_OOM              │ ✅ Yes   │ 2        │ Route to larger│
│                         │          │          │ worker pool    │
│ DEPENDENCY_TIMEOUT      │ ✅ Yes   │ 3        │ Parser service │
│                         │          │          │ overloaded     │
│ RATE_LIMITED (429)      │ ✅ Yes   │ 5        │ Use Retry-After│
│                         │          │          │ header         │
│ INTERNAL_SERVER (500)   │ ✅ Yes   │ 3        │ Transient bug  │
├─────────────────────────┼──────────┼──────────┼────────────────┤
│ INVALID_SQL_SYNTAX      │ ❌ No    │ 0        │ Straight to DLQ│
│ UNSUPPORTED_DIALECT     │ ❌ No    │ 0        │ Feature gap    │
│ AUTH_FAILURE (401/403)  │ ❌ No    │ 0        │ Bad credentials│
│ PAYLOAD_TOO_LARGE (413) │ ❌ No    │ 0        │ File too big   │
│ VALIDATION_ERROR (400)  │ ❌ No    │ 0        │ Bad input      │
└─────────────────────────┴──────────┴──────────┴────────────────┘
```

> *This classification mirrors how I built the retry logic in Praesidium's compliance agents — accounting validation errors are non-retriable (the data is fundamentally wrong), but API timeouts are always retriable. Mixing them up causes either wasted compute or silent data loss.*

---

## B.7 Deep Dive: Circuit Breaker

### B.7.1 Three-State Circuit Breaker

```
                    ┌──────────────────────────┐
        Success     │                          │    Failure threshold
     ┌─────────────▶│         CLOSED            │───────────────┐
     │              │   (Normal operation)      │               │
     │              │   Failure counter < N     │               │
     │              └──────────────────────────┘               │
     │                                                          │
     │              ┌──────────────────────────┐               │
     │              │                          │◀──────────────┘
     │              │          OPEN             │
     │              │   (All requests fail-fast)│
     │              │   Timer running           │──── Timeout ──┐
     │              └──────────────────────────┘               │
     │                                                          │
     │              ┌──────────────────────────┐               │
     │              │                          │◀──────────────┘
     └──────────────│       HALF-OPEN           │
        Probe       │   (Allow 1 probe request) │
        success     │   If fail → back to OPEN  │
                    └──────────────────────────┘
```

### B.7.2 Configuration

```python
class CircuitBreakerConfig:
    failure_threshold: int = 10       # Failures to trip OPEN
    success_threshold: int = 3        # Successes in HALF-OPEN to close
    timeout_seconds: int = 30         # Time in OPEN before probing
    window_seconds: int = 60          # Sliding window for failure counting
    half_open_max_concurrent: int = 1 # Probes allowed simultaneously
```

### B.7.3 Per-Dependency Circuit Breakers

```
Circuit Breakers (one per downstream dependency):

  ┌──────────────────────────────────────────┐
  │ ast-parser-oracle        │ CLOSED  │ 0/10│ ← Healthy
  │ ast-parser-teradata      │ OPEN    │ 10/10│ ← Teradata parser down
  │ ast-parser-tsql          │ HALF_OPEN│ 8/10│ ← Recovering
  │ snowflake-validator      │ CLOSED  │ 2/10│ ← Healthy
  │ gcs-output-writer        │ CLOSED  │ 0/10│ ← Healthy
  └──────────────────────────────────────────┘

When ast-parser-teradata circuit is OPEN:
  → All Teradata conversion jobs fail-fast with CIRCUIT_OPEN error
  → Jobs are NOT retried (pointless to retry against a down service)
  → Jobs are held in a "parking" queue until circuit closes
  → Alert fires to PagerDuty
```

**Why per-dependency, not global?** If the Teradata parser is down, Oracle conversions should continue unaffected. A global circuit breaker would halt all work.

### B.7.4 Circuit Breaker + Retry Interaction

```
Worker receives job:
  1. Check circuit breaker for target dependency
     - OPEN → Don't even attempt. Re-enqueue with delay = circuit_timeout.
     - CLOSED/HALF_OPEN → Proceed to step 2.
  
  2. Execute the job
  
  3. On failure:
     a. Record failure in circuit breaker
     b. If circuit trips OPEN → log event, alert
     c. Classify error
     d. If retriable AND circuit still CLOSED:
        → Schedule retry with backoff + jitter
     e. If circuit is now OPEN:
        → Hold job (don't burn retry count against a known-down service)
  
  4. On success:
     a. Record success in circuit breaker
     b. If was HALF_OPEN and success_count >= threshold → transition to CLOSED
```

---

## B.8 Deep Dive: Dead Letter Queue

### B.8.1 DLQ Entry Schema

```json
{
  "dlq_id": "dlq-5f8a9b2c",
  "original_job_id": "job-a1b2c3d4",
  "tenant_id": "tenant-42",
  "error_class": "WORKER_OOM",
  "final_error_message": "Container killed: OOMKilled (limit: 4Gi, usage: 4.1Gi)",
  "retry_history": [
    {
      "attempt": 1,
      "timestamp": "2025-01-15T10:00:05Z",
      "delay_ms": 700,
      "error": "OOMKilled",
      "worker_id": "worker-pod-abc123"
    },
    {
      "attempt": 2,
      "timestamp": "2025-01-15T10:00:08Z",
      "delay_ms": 1300,
      "error": "OOMKilled",
      "worker_id": "worker-pod-def456"
    }
  ],
  "original_payload": {
    "file_ref": "gs://uploads/tenant-42/batch-7/massive_proc.sql",
    "source_dialect": "oracle_plsql",
    "file_size_bytes": 2500000
  },
  "circuit_breaker_state_at_failure": "CLOSED",
  "created_at": "2025-01-15T10:00:12Z",
  "status": "PENDING_REVIEW"
}
```

### B.8.2 DLQ Processing Workflow

```
DLQ Item Lifecycle:

  PENDING_REVIEW ──→ REPLAYING ──→ (back to main queue)
       │                               │
       │                          Success → DELETE from DLQ
       │                          Failure → back to PENDING_REVIEW
       │
       └──→ PURGED (acknowledged as unrecoverable)
```

**Replay strategies:**
1. **Single replay:** Ops engineer inspects one item and replays after fixing the root cause.
2. **Bulk replay by error class:** "Replay all WORKER_OOM items" after scaling up worker memory limits.
3. **Bulk replay by time window:** "Replay everything that failed during the 10:00-10:30 parser outage."
4. **Automated replay on circuit close:** When a circuit breaker transitions from OPEN → CLOSED, automatically replay parked jobs.

### B.8.3 DLQ Alerting

```
Alert Rules:
  - DLQ ingest rate > 50 items/min           → PagerDuty Critical
  - DLQ depth > 1,000 items                  → Slack Warning
  - Single error class > 80% of DLQ items    → PagerDuty (systemic failure)
  - DLQ item age > 24 hours                  → Slack Reminder
  - DLQ replay failure rate > 50%            → PagerDuty (root cause not fixed)
```

---

## B.9 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Retry storm after outage** | Recovered service re-overwhelmed | Full jitter spreads retries; circuit breaker limits concurrent probes |
| **DLQ overflow** | Storage fills up | 30-day TTL on DLQ items; alert at 80% capacity; auto-purge items older than TTL |
| **Incorrect error classification** | Retriable error treated as non-retriable → premature DLQ | Default to retriable for unknown errors; classification is a config, not code |
| **Circuit breaker flapping** | Rapidly opens/closes | Hysteresis: require 3 consecutive successes to close, not just 1 |
| **DLQ replay causes cascading failure** | Replaying 10K items floods the system | Replay with rate limiting (e.g., 100 items/min); respect circuit breaker state during replay |

---

## B.10 Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Full jitter over equal jitter | Equal jitter or no jitter | Full jitter minimizes total completion time under contention (AWS research) |
| Per-dependency circuit breakers | Global breaker | Fault isolation — one bad parser shouldn't block all dialects |
| Cloud Tasks for delayed retry | Sleep in worker, or Pub/Sub scheduling | Workers shouldn't block on sleep; Cloud Tasks gives exactly-once delayed delivery |
| Error classification as config | Hardcoded in worker code | New error types appear constantly; config-driven lets ops adjust without deploys |
| 30-min total retry budget | Unlimited retries | Bounded retry window prevents resource waste; DLQ catches the rest |

---

## B.11 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Isn't a DLQ just sweeping failures under the rug?" | "The opposite — a DLQ makes failures visible and actionable. Without a DLQ, failed jobs disappear silently. With one, every failure is preserved with full context (retry history, error traces, payload), alerting fires, and ops can replay when the root cause is fixed." |
| "Circuit breakers seem complex. Why not just retry more?" | "Retrying against a dead service is worse than not retrying at all — it wastes compute, burns retry budgets, and adds load to the failing service. A circuit breaker is a fast-fail mechanism. When the Teradata parser is down, we fail-fast in microseconds instead of waiting 30 seconds for a timeout, saving thousands of pod-seconds per minute." |
| "How do you test all these failure modes?" | "Chaos engineering. We inject failures in staging: kill the parser pod, simulate OOM with memory limits, add artificial latency with Istio fault injection, and trigger circuit breakers intentionally. We verify that retries backoff correctly, circuits trip at the right threshold, and DLQ entries have complete context." |
| "What about exactly-once processing on replay?" | "Replay is idempotent by design. Each job uses a deterministic output path and checks for existing completion status before processing. Replaying a completed job is a no-op." |

---

# Section C: Idempotent API Gateway

> **Interview Prompt:** "A client submits a 500-file batch conversion request. The network drops mid-response. The client retries. How do you ensure you don't process 500 files twice? Design an API gateway that handles retriable vs. non-retriable HTTP codes without duplicating data."

---

## C.1 Requirements

### Functional
- Accept an `X-Idempotency-Key` header on all mutating requests (POST, PUT, DELETE).
- Return the cached response for duplicate requests with the same idempotency key.
- Distinguish between retriable (5xx, 429, network errors) and non-retriable (4xx) responses.
- Support key expiration (idempotency keys valid for 24 hours).
- Prevent concurrent duplicate requests (two retries hitting different gateway pods simultaneously).

### Non-Functional
- **Latency overhead:** < 5ms P99 for idempotency check.
- **Throughput:** 5,000 requests/sec across the gateway fleet.
- **Consistency:** Linearizable idempotency guarantees — no duplicate side effects under any race condition.

### Capacity Estimation
```
Request rate:          5,000 req/sec
Mutating requests:     ~40% = 2,000 req/sec need idempotency
Idempotency record:    ~500 bytes (key + status + response hash + timestamps)
Records per 24hr TTL:  2,000 × 86,400 = 172.8M records
Storage:               172.8M × 500B = ~86 GB
Redis memory (hot):    Store last 1 hour = 7.2M × 500B = ~3.6 GB
```

---

## C.2 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Client                                 │
│  (includes idempotency key in X-Idempotency-Key header)      │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────────┐
│                   API Gateway (Envoy + Filter)                │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐    │
│  │              Idempotency Middleware                    │    │
│  │                                                       │    │
│  │  1. Extract X-Idempotency-Key from header             │    │
│  │  2. Lookup key in Redis                               │    │
│  │     ├─ KEY NOT FOUND → acquire lock, proceed          │    │
│  │     ├─ KEY FOUND, status=IN_PROGRESS → return 409     │    │
│  │     └─ KEY FOUND, status=COMPLETED → return cached    │    │
│  │  3. After upstream responds:                          │    │
│  │     ├─ 2xx → store response, mark COMPLETED          │    │
│  │     ├─ 4xx → store response, mark COMPLETED (final)  │    │
│  │     └─ 5xx → delete key (allow retry)                 │    │
│  └──────────────────────────────────────────────────────┘    │
│                           │                                   │
│  ┌────────────────────────▼─────────────────────────────┐    │
│  │              Redis Cluster (Idempotency Store)        │    │
│  │                                                       │    │
│  │  Key: "idempotency:{tenant_id}:{idempotency_key}"    │    │
│  │  Value: {                                             │    │
│  │    "status": "COMPLETED",                             │    │
│  │    "response_code": 202,                              │    │
│  │    "response_body_hash": "sha256:abc...",             │    │
│  │    "response_body": "{...}",                          │    │
│  │    "created_at": "2025-01-15T10:00:00Z"              │    │
│  │  }                                                    │    │
│  │  TTL: 24 hours                                        │    │
│  └──────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────┘
```

---

## C.3 Deep Dive: Idempotency Protocol

### C.3.1 The Race Condition Problem

Without locking, two retries can hit different gateway pods simultaneously:

```
Time    Pod A                           Pod B
────    ─────                           ─────
T1      Lookup key → NOT FOUND          
T2                                      Lookup key → NOT FOUND
T3      Forward to upstream             Forward to upstream
T4      Upstream creates batch-99       Upstream creates batch-100 ← DUPLICATE!
```

### C.3.2 Solution: Redis Distributed Lock + Status

```python
def handle_request(key: str, request):
    full_key = f"idempotency:{request.tenant_id}:{key}"
    
    # Step 1: Atomic SET-IF-NOT-EXISTS with lock
    acquired = redis.set(
        full_key,
        json.dumps({"status": "IN_PROGRESS", "locked_by": pod_id, "locked_at": now()}),
        nx=True,      # Only set if not exists
        ex=300         # 5-minute lock timeout (prevents dead locks)
    )
    
    if acquired:
        # Step 2a: We own the lock — forward to upstream
        try:
            response = forward_to_upstream(request)
            
            if response.status_code < 500:
                # 2xx or 4xx → Cache the response (final answer)
                redis.set(full_key, json.dumps({
                    "status": "COMPLETED",
                    "response_code": response.status_code,
                    "response_body": response.body,
                    "completed_at": now()
                }), ex=86400)  # 24-hour TTL
            else:
                # 5xx → Delete the key (allow client to retry)
                redis.delete(full_key)
            
            return response
        except Exception:
            # Upstream unreachable → delete key, allow retry
            redis.delete(full_key)
            raise
    
    else:
        # Step 2b: Key exists — check status
        existing = json.loads(redis.get(full_key))
        
        if existing["status"] == "IN_PROGRESS":
            # Another pod is processing this right now
            return Response(409, "Request is already being processed. Retry later.")
        
        elif existing["status"] == "COMPLETED":
            # Return the cached response
            return Response(
                existing["response_code"],
                existing["response_body"],
                headers={"X-Idempotency-Applied": "true"}
            )
```

### C.3.3 Critical: Why 5xx Deletes the Key

| Status Code | Action on Idempotency Key | Rationale |
|-------------|---------------------------|-----------|
| 2xx | Cache response, mark COMPLETED | Success — return same result on retry |
| 400 | Cache response, mark COMPLETED | Client error — retrying with same payload will always fail |
| 401/403 | Cache response, mark COMPLETED | Auth error — same creds will always fail |
| 409 | Cache response, mark COMPLETED | Conflict is deterministic for same input |
| 429 | **DELETE the key** | Transient — should succeed after backoff |
| 500 | **DELETE the key** | Transient — upstream may recover |
| 502/503/504 | **DELETE the key** | Transient — upstream may recover |

**The key insight:** We cache responses for *deterministic* outcomes. 4xx are deterministic (same input → same error). 5xx are *non-deterministic* (same input might succeed next time). Caching a 500 would permanently block the client from retrying successfully.

> *This is a lesson from building Praesidium's compliance validation agents — a validation error (400) is informational and permanent. A timeout hitting the IRS API (504) is transient. Mixing them up means either blocking legitimate retries or caching garbage.*

### C.3.4 Handling Missing Idempotency Keys

```
Policy per endpoint:

POST /v1/conversion/batches       → REQUIRED (reject 400 if missing)
POST /v1/jobs                     → REQUIRED
PUT  /v1/jobs/{id}/cancel         → OPTIONAL (operation is naturally idempotent)
GET  /v1/jobs/{id}                → NOT NEEDED (reads are inherently idempotent)
DELETE /v1/dlq/items/{id}         → NOT NEEDED (deleting a deleted item is a no-op)
```

### C.3.5 Key Fingerprinting (Preventing Abuse)

```python
def validate_key_payload_match(key, request, existing_record):
    """Ensure the same key isn't used for different requests."""
    request_fingerprint = sha256(
        request.method + request.path + request.body
    )
    
    if existing_record.fingerprint != request_fingerprint:
        return Response(422, 
            "Idempotency key reused with different request payload. "
            "Generate a new key for each unique request."
        )
```

---

## C.4 Gateway-Level Concerns

### Rate Limiting Integration

```
Rate limiting happens BEFORE idempotency check:

  Request → Rate Limiter → Idempotency Check → Upstream

Why? If we check idempotency first, rate-limited retries would
return cached responses instead of 429, defeating rate limiting.
```

### Redis Cluster Topology

```
6-node Redis Cluster (3 masters, 3 replicas)
Hash slot distribution: 16,384 slots across 3 masters

Key hashing: HASH("idempotency:{tenant-42}:{uuid-abc}")
  → Slot 8,234 → Master 2

Write: SET with NX → Master 2
Read:  GET → Replica of Master 2 (for cached responses)

AOF persistence: fsync every second
  → Worst case: 1 second of idempotency keys lost on Redis crash
  → Acceptable: duplicate processing is idempotent at the worker level
```

---

## C.5 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Redis down** | Can't check idempotency | Fallback: allow request through (degrade to at-least-once); workers are idempotent anyway |
| **Lock held by dead pod** | Key stuck in IN_PROGRESS | 5-minute lock TTL auto-expires; client can retry after timeout |
| **Client reuses key with different payload** | Could return wrong cached response | Payload fingerprint validation — reject with 422 |
| **Cache poisoning (cached 500)** | Client permanently blocked | 5xx responses DELETE the key — never cached |
| **Redis memory pressure** | Keys evicted before TTL | Set maxmemory-policy to `volatile-ttl`; only idempotency keys have TTL |

---

## C.6 Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Redis for idempotency store | PostgreSQL | 5ms P99 requirement; Redis SET NX is O(1); PG row-level locking adds 10-50ms |
| Delete key on 5xx | Cache 5xx with short TTL | Caching any error response risks permanent failure; clean delete is simpler and safer |
| 409 for concurrent duplicates | Queue the retry internally | 409 is honest — tells client to retry later; internal queuing adds complexity |
| 24-hour key TTL | Infinite retention | 24 hours covers any reasonable retry window; infinite retention wastes memory |
| Payload fingerprinting | Trust client to use keys correctly | Defense in depth — catches bugs in client retry libraries |

---

## C.7 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just make every endpoint idempotent without a key?" | "Some operations are naturally idempotent (PUT, DELETE), but POST /batches is not — each call creates a new batch. The idempotency key gives the client control: 'This specific call should happen exactly once.' Without it, we'd need server-side deduplication based on payload hashing, which doesn't work for intentional duplicate requests." |
| "What if Redis loses the key?" | "Two layers of defense. First, Redis AOF persistence recovers most keys on restart. Second, the downstream workers are idempotent — if a duplicate slips through, the worker checks the DB for existing results and short-circuits. The gateway idempotency is an optimization to avoid wasting compute, not the sole safety net." |
| "Isn't 409 confusing for the client?" | "It's the correct semantic — 409 Conflict means 'your request conflicts with the current state,' which here is 'another instance of this request is already in-flight.' We include a clear error message. Stripe uses the same pattern." |
| "How do you handle idempotency across API versions?" | "The idempotency key is scoped to the endpoint path and method. A key used on v1/batches and v2/batches are different keys. The fingerprint includes the full URL path, so version migration doesn't cause key collisions." |

---

## Summary Narratives (Use These to Close Each Section)

### Task Queue Narrative
> "I'd design a **priority-aware async task queue** on GKE using **Google Cloud Pub/Sub** as the durable message broker and **PostgreSQL** as the job state registry. Workers pull messages using a priority-cascade pattern — high first, then normal, then low. Each worker extends its ack deadline via heartbeats for long-running jobs. The HPA auto-scales from 5 to 500 pods based on queue backlog. Workers are fully idempotent — duplicates are safe because of deterministic output paths and DB-level status checks. Failures route to a Dead Letter Topic after 3 retries."

### Retry/DLQ/Circuit Breaker Narrative
> "I'd design a **three-layer resilience system**: retry manager, circuit breaker, and dead letter queue. On failure, the Retry Manager classifies the error — transient errors get retried with **exponential backoff and full jitter**, while non-retriable errors go straight to the DLQ. Before each retry, we check the **per-dependency circuit breaker** — if it's tripped, we fail-fast instead of burning retry budgets. Jobs that exhaust retries land in the **DLQ with full diagnostic context** and support bulk replay by error class."

### Idempotent Gateway Narrative
> "I'd implement idempotency at the **API gateway layer** using Redis for fast key-value lookups. Every mutating endpoint requires an `X-Idempotency-Key`. On first request, we atomically acquire a lock via `SET NX`. On success (2xx) or deterministic failure (4xx), we cache with 24-hour TTL. On transient failure (5xx/429), we **delete the key** so clients can retry. This gives us **effectively-once semantics** at the API boundary, backed by idempotent workers as a safety net."

---

## Key Terms to Drop Naturally

- **Ack deadline extension**, **heartbeat**, **lease renewal**
- **Priority-cascade pull**, **topic-per-priority**
- **Idempotent worker**, **deterministic output path**
- **HPA custom metrics**, **Pub/Sub backlog scaling**
- **Spot/preemptible VMs**, **graceful SIGTERM handling**
- **Dead Letter Topic (DLT)**, **poison message**
- **At-least-once → effectively-once**
- **Job state machine**, **status transition**
- **Exponential backoff**, **full jitter**, **decorrelated jitter**
- **Circuit breaker** (CLOSED → OPEN → HALF-OPEN)
- **Error classification** (retriable vs. non-retriable)
- **Fail-fast**, **fast failure**, **thundering herd**, **retry storm**
- **Chaos engineering**, **fault injection**
- **Idempotent replay**, **bulk recovery**
- **429 Too Many Requests**, **Retry-After header**
- **Hysteresis**, **sliding window failure count**
- **X-Idempotency-Key**, **SET NX** (set-if-not-exists)
- **Deterministic vs. non-deterministic failure**
- **Payload fingerprint**, **cache poisoning**
- **Redis Cluster**, **hash slots**, **409 Conflict**
