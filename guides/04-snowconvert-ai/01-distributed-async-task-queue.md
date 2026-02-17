# Distributed Async Task Queue for SnowConvert AST Parsing

> **Interview Prompt:** "SnowConvert receives thousands of legacy SQL files per hour. Each file needs AST parsing, semantic analysis, and transpilation — jobs that take 5 seconds to 15 minutes each. Design the async task queue infrastructure that powers this."

---

## 1. Requirements

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

## 2. API Design

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

## 3. High-Level Architecture

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

## 4. Deep Dive: Critical Design Decisions

### 4.1 Why Pub/Sub Over Self-Hosted Kafka?

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

### 4.2 Priority Queue Design

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

### 4.3 Worker Lifecycle & Heartbeats

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

### 4.4 Autoscaling Strategy on GKE

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

### 4.5 Job State Machine

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

### 4.6 Idempotent Job Execution

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

## 5. Failure Modes & Mitigations

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

## 6. Trade-offs & Design Decisions

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Pub/Sub over Kafka | Self-hosted Kafka on GKE | Zero ops overhead; Kafka only justified at >100K msg/sec sustained |
| PostgreSQL for job state | Redis | Jobs must survive restarts; Redis persistence adds complexity for no gain |
| Separate priority topics | Single topic + priority field | Hard isolation prevents starvation of high-priority jobs |
| Pull-based workers | Push-based delivery | Workers control concurrency; avoids overwhelming slow workers |
| GCS for outputs | Inline in DB | Outputs can be 100MB+; GCS is cheaper and supports streaming reads |

---

## 7. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not use a simple database queue with SELECT FOR UPDATE?" | "Database queues work at low scale but create lock contention under high throughput. At 167 jobs/sec, the polling overhead and row-level locking would make PostgreSQL the bottleneck. A dedicated message broker gives us push-based delivery, built-in dead-lettering, and decouples producers from consumers." |
| "What if Pub/Sub loses messages?" | "Pub/Sub guarantees at-least-once delivery with message persistence to multiple zones. The real risk isn't message loss — it's message duplication. That's why workers are idempotent: same input always produces same output, and the DB status check prevents double-processing." |
| "How do you handle a 15-minute job? The ack deadline will expire." | "Workers extend the ack deadline every 60 seconds via a heartbeat goroutine. If the worker dies and stops heartbeating, the deadline expires and Pub/Sub redelivers the message within 10 minutes. This balances fast recovery from dead workers with support for legitimately long jobs." |
| "500 pods on GKE sounds expensive." | "We use spot/preemptible VMs for the worker pool — 60-80% cost reduction. Since workers are stateless and idempotent, preemption is safe. We NACK the current message on SIGTERM and another worker picks it up. The fleet auto-scales down to 5 pods during off-peak hours." |
| "What about exactly-once processing?" | "Exactly-once is an illusion in distributed systems. We implement effectively-once via idempotent workers: deterministic output paths, DB status checks, and GCS overwrite semantics. The cost of a duplicate execution is a few seconds of wasted compute, which is far cheaper than building a distributed transaction coordinator." |

---

## 8. Summary: Your Interview Narrative

> "I'd design a **priority-aware async task queue** on GKE using **Google Cloud Pub/Sub** as the durable message broker and **PostgreSQL** as the job state registry. The Job Submission Service validates incoming conversion requests, persists them to the DB, and publishes to one of three priority-tiered Pub/Sub topics. Workers on GKE pull messages using a priority-cascade pattern — high first, then normal, then low. Each worker extends its ack deadline via heartbeats for long-running AST jobs and writes outputs to GCS. The HPA auto-scales the worker pool from 5 to 500 pods based on subscription backlog depth, with aggressive scale-up and conservative scale-down to handle enterprise batch uploads. Workers are fully idempotent — duplicate deliveries are safe because of deterministic output paths and DB-level status checks. Failures route to a Dead Letter Topic after 3 retries for human review."

---

## 9. Key Terms to Drop Naturally

- **Ack deadline extension**, **heartbeat**, **lease renewal**
- **Priority-cascade pull**, **topic-per-priority**
- **Idempotent worker**, **deterministic output path**
- **HPA custom metrics**, **Pub/Sub backlog scaling**
- **Spot/preemptible VMs**, **graceful SIGTERM handling**
- **Dead Letter Topic (DLT)**, **poison message**
- **At-least-once → effectively-once**
- **Job state machine**, **status transition**
