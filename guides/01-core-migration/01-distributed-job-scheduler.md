# Design a Distributed Job Scheduler

> **Interview Prompt:** "How would you run 50,000 conversion jobs concurrently?"

---

## 1. Clarifying Questions to Ask the Interviewer

Before diving in, **always** ask questions. This shows structured thinking.

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | What's the expected job duration? (seconds vs. hours?) | Determines queue strategy and timeout policies |
| 2 | Are jobs independent or do some have dependencies? | DAG scheduling vs. flat queue |
| 3 | What's the SLA for job completion? | Drives scaling and prioritization decisions |
| 4 | Is exactly-once execution required, or is at-least-once acceptable? | Impacts idempotency and deduplication design |
| 5 | How are jobs submitted? (API, batch upload, event-driven?) | Shapes the ingestion layer |

---

## 2. High-Level Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Job API     │────▶│  Job Queue       │────▶│  Worker Pool     │
│  (Ingestion) │     │  (Kafka/SQS)     │     │  (Auto-scaled)   │
└──────────────┘     └──────────────────┘     └──────────────────┘
       │                      │                        │
       ▼                      ▼                        ▼
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Job Store   │     │  Scheduler       │     │  Result Store    │
│  (Postgres)  │     │  (Orchestrator)  │     │  (S3 + DB)       │
└──────────────┘     └──────────────────┘     └──────────────────┘
                              │
                              ▼
                     ┌──────────────────┐
                     │  Health Monitor  │
                     │  & Rebalancer    │
                     └──────────────────┘
```

### Key Components

1. **Job API** — REST/gRPC endpoint accepting job submissions, validates input, writes to Job Store, enqueues to Job Queue.
2. **Job Queue** — Durable message queue (Kafka partitions or SQS). Decouples submission from execution.
3. **Scheduler/Orchestrator** — Assigns jobs to workers, manages priorities, handles DAG dependencies if needed.
4. **Worker Pool** — Stateless workers pulling jobs. Auto-scaled via Kubernetes HPA or cloud functions.
5. **Job Store** — Persistent metadata (status, retries, timestamps) in Postgres/DynamoDB.
6. **Result Store** — Outputs go to S3; metadata updated in Job Store.
7. **Health Monitor** — Detects stalled/dead workers; reassigns orphaned jobs.

---

## 3. Deep-Dive: Core Design Decisions

### 3.1 Job Distribution Strategy

**Option A: Pull-Based (Workers poll the queue)**
```
Worker → Poll Queue → Get Job → Execute → Ack
```
- ✅ **Pros:** Simple, workers self-balance, easy to scale
- ❌ **Cons:** Polling overhead, uneven distribution possible

**Option B: Push-Based (Scheduler assigns jobs)**
```
Scheduler → Select Worker → Push Job → Worker Executes → Report Back
```
- ✅ **Pros:** Better load balancing, scheduler has global view
- ❌ **Cons:** Scheduler is a SPOF, more complex

**Recommended:** **Pull-based with partitioned queues.** Each partition guarantees ordering if needed, and Kafka consumer groups auto-balance partitions across workers.

### 3.2 Scaling to 50K Concurrent Jobs

| Strategy | How |
|----------|-----|
| **Horizontal worker scaling** | Kubernetes HPA based on queue depth metric |
| **Queue partitioning** | 100+ Kafka partitions, each consumed by different worker |
| **Batch processing** | Workers process micro-batches (e.g., 10 jobs per pull) |
| **Resource isolation** | CPU-bound vs. I/O-bound jobs on different worker pools |
| **Pre-warming** | Keep a warm pool of workers; spin up extras for bursts |

### 3.3 Failure Handling

```
Job Fails
   │
   ├─▶ Retryable? (timeout, OOM, transient error)
   │      │
   │      ├─ YES → Exponential backoff retry (max 3-5 attempts)
   │      │         └─ Still failing → Dead Letter Queue (DLQ)
   │      │
   │      └─ NO  → DLQ immediately (bad input, parse error)
   │
   └─▶ Worker dies mid-job?
          └─ Visibility timeout expires → Job re-appears in queue
             └─ Another worker picks it up
```

**Key principle:** Jobs must be **idempotent**. If a job runs twice, the output should be the same.

### 3.4 Job Prioritization

- Use **multiple queues** with priority levels (P0-critical, P1-high, P2-normal)
- Workers check P0 first, then P1, then P2
- Alternative: Kafka topics per priority, workers consume with weighted allocation (e.g., 60% P0, 30% P1, 10% P2)

### 3.5 DAG-Based Dependencies

```
Some jobs have dependencies (like Airflow DAGs):

            ┌─── Extract ───┐
            │               │
  Validate ─┤               ├─ Load ─── Notify
            │               │
            └── Transform ──┘

Scheduler logic:
  1. Parse DAG → build adjacency list
  2. Start jobs with no dependencies (in-degree = 0)
  3. As each job completes, decrement in-degree of dependents
  4. When a dependent’s in-degree reaches 0 → schedule it
  5. If any job fails → mark all downstream as BLOCKED

State machine per job in a DAG:
  PENDING → QUEUED → RUNNING → COMPLETED
                              → FAILED → RETRYING / DLQ
  BLOCKED (upstream failed)
```

### 3.6 Capacity Estimation

```
Assumptions:
  50K concurrent jobs
  Average job duration: 5 minutes
  Job arrival rate: 10K jobs/min at peak
  Job payload: stored in S3 (average 1MB)

Kafka sizing:
  200 partitions × 10K msg/sec throughput = easily handles 10K/min
  Retention: 24 hours
  Storage: 10K jobs/min × 1KB metadata × 1440 min = ~14GB/day

  Partition count calculation:
    Target throughput per partition: 50 MB/sec
    Message size: 1 KB
    Messages per partition per sec: 50,000
    Required partitions for 10K jobs/min (167/sec): ceil(167/50,000) = 1
    But for parallelism: 200 partitions allows 200 workers to read simultaneously

Worker sizing:
  50K concurrent ÷ 5 min average = 10K completions/min
  Each worker handles 1 job at a time (CPU-bound)
  Need: ~50K workers at peak
  With Kubernetes HPA: scale 0 → 50K pods (warm pool of 5K)

  Cost optimization:
    Spot instances for worker pods (50-70% cost savings)
    Preemption handling: workers gracefully shutdown on spot termination
    Checkpoint job progress to resume on new worker

Job Store (Postgres):
  10K inserts/min = 167/sec (well within Postgres capacity)
  Index on (status, priority) for scheduler queries
  Partition by created_at for archival

  Connection pooling: PgBouncer with 100 connections per worker pod
  Read replicas: Worker reads can hit read replicas to reduce primary load

Network bandwidth:
  50K jobs × 1MB payload = 50 GB data transfer
  With internal VPC: 10 Gbps network = ~50 seconds for full transfer
  S3 to worker streaming: 100 MB/sec per worker
```

---

## 4. Bottlenecks & How to Address Them

| Bottleneck | Symptom | Solution |
|------------|---------|----------|
| **Queue saturation** | Producers blocked | Add partitions, increase throughput limits |
| **Worker starvation** | Jobs pile up | Auto-scale workers, increase instance type |
| **Database hotspot** | Job Store writes lag | Batch writes, use write-ahead buffer, shard by job_id |
| **Network bandwidth** | Large payloads slow transfers | Store payloads in S3, pass references through queue |
| **Noisy neighbor** | One tenant's jobs starve others | Per-tenant rate limiting + fair-share scheduling |

---

## 5. Data Model

```sql
CREATE TABLE jobs (
    job_id          UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    status          ENUM('PENDING','RUNNING','COMPLETED','FAILED','DLQ'),
    priority        INT DEFAULT 2,
    payload_ref     TEXT,           -- S3 URI to input
    result_ref      TEXT,           -- S3 URI to output  
    worker_id       UUID,
    attempt_count   INT DEFAULT 0,
    max_retries     INT DEFAULT 3,
    created_at      TIMESTAMP,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    error_message   TEXT
);

-- Index for scheduler queries
CREATE INDEX idx_jobs_status_priority ON jobs(status, priority);
CREATE INDEX idx_jobs_tenant ON jobs(tenant_id, status);
```

---

## 6. How to Handle Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if the scheduler goes down?" | "The scheduler is stateless — it reads from the Job Store. We run multiple replicas behind a leader election (ZooKeeper/etcd). If the leader fails, a follower takes over in seconds." |
| "50K jobs is a lot — will Kafka handle it?" | "Kafka handles millions of messages/sec. With 200 partitions and proper consumer groups, 50K concurrent jobs is well within capacity. The bottleneck is more likely worker compute, which we address with auto-scaling." |
| "How do you prevent duplicate execution?" | "Three layers: (1) Queue visibility timeouts, (2) Idempotency keys in the Job Store — before starting, workers check `status != RUNNING`, (3) Distributed locks via Redis for critical sections." |
| "What about observability?" | "Every state transition emits a structured event. We track queue depth, worker utilization, p50/p99 job latency, failure rates, and DLQ growth. Alerts fire on anomalies." |
| "How do you handle a job that runs forever?" | "Each job has a max_duration timeout. Workers send heartbeats every 30 seconds. If no heartbeat for 2 minutes, the Health Monitor marks the job as TIMED_OUT and requeues it. After max_retries, it goes to DLQ." |
| "What about multi-tenant fairness?" | "Per-tenant concurrency limits and fair-share scheduling. If Tenant A submits 10K jobs, they get at most their fair share of workers (e.g., 10K out of 50K). Other tenants aren't starved." |
| "How do you handle worker crashes mid-job?" | "Workers catch SIGTERM and gracefully shutdown: (1) stop accepting new jobs, (2) mark current job as FAILED in Job Store, (3) job gets requeued after visibility timeout. For hard crashes (OOM, kernel panic), the Health Monitor detects missing heartbeats and reassigns the job." |
| "What's the recovery time objective (RTO)?" | "For worker failures: < 1 minute (heartbeat timeout + requeue). For Job Store failures: < 5 minutes (Postgres streaming replication with failover). For Kafka failures: < 30 seconds (ISR replica promotion)." |

---

## 7. Security Considerations

```
Authentication & Authorization:
  - Workers authenticate with Job API via mTLS certificates
  - Role-based access control: workers can only update jobs assigned to them
  - Job payloads encrypted at rest in S3 (SSE-KMS)

Data Privacy:
  - Tenant isolation: jobs filtered by tenant_id in all queries
  - Audit logging: who submitted which job, when, and result
  - PII in job payloads: encrypted before storage, decrypted only by workers

Network Security:
  - VPC endpoints for S3, Kafka (no internet gateway traversal)
  - Network policies restrict worker-to-worker communication
  - API Gateway rate limiting per tenant (see Rate Limiter guide)
```

---

## 8. Summary: Your Interview Narrative

> "I'd design a **pull-based distributed job scheduler** with Kafka as the backbone. Jobs are submitted via API, persisted to a Job Store, and enqueued to partitioned Kafka topics. A pool of stateless workers auto-scales based on queue depth, pulling and processing jobs independently. Failed jobs retry with exponential backoff before landing in a DLQ. For 50K concurrent jobs, I'd use 200+ Kafka partitions, Kubernetes HPA for worker scaling, and S3 for payload storage to keep the queue lightweight. Observability is built in from day one with structured event logging and metric dashboards."

---

## 9. Key Terms to Drop Naturally

- **Consumer groups** (Kafka), **visibility timeout** (SQS)
- **Idempotency**, **exactly-once semantics**
- **Horizontal Pod Autoscaler (HPA)**
- **Dead Letter Queue (DLQ)**
- **Leader election**, **partition rebalancing**
- **Exponential backoff with jitter**
- **Fan-out / fan-in pattern**
- **DAG** (Directed Acyclic Graph) scheduling
- **Fair-share scheduling**, **per-tenant quotas**
