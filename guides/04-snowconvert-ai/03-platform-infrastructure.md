# System Design: Deploy a Production Service in the Cloud

> **The Question:** "You have a service that processes jobs — think file conversions, data transformations, or API requests. Walk me through how you'd deploy it to production so that it can scale under heavy load, handle failures gracefully, and be fully observable. What does the production-grade setup look like?"

> **Duration:** ~1 hour. The interviewer will introduce constraints (cost-efficiency, high availability, multi-region, multi-tenant isolation) as you go. They want tradeoffs, prioritization, and adaptability.

> **Candidate Feedback:** *"This was more infrastructure-oriented than a classic system design. The interviewer expected me to think aloud while building a service that could scale, use reliable storage and durable queues, handle failures with retries and circuit breakers, and be observable. What mattered most wasn't completeness, but the tradeoffs I made and how I adapted when new constraints were introduced."*

---

## Step 1: Clarify the Service (First 5 Minutes)

Ask these before designing anything:

| Question | Why It Matters |
|----------|---------------|
| What does the service do? (API? Batch? Stream?) | Determines sync vs. async, latency vs. throughput |
| What's the traffic pattern? (Steady? Bursty?) | Bursty = auto-scaling critical. Steady = simpler |
| What's the SLA? (99.9%? 99.99%?) | Drives redundancy, multi-AZ, health checks |
| Who are the users? (Internal? Multi-tenant?) | Multi-tenant = isolation, quotas, cost attribution |
| What's the budget priority? | Shapes spot vs. on-demand, warehouse sizing |

### Example Service (Use Throughout)
A job processing service: users submit jobs via API, jobs are queued, workers process them asynchronously, results are stored. Think of it as any backend service that does heavy work.

### Non-Functional Requirements
- **Availability:** 99.9% (≈ 43 min downtime/month).
- **Latency:** API responds < 200ms (just enqueues). Job completion < 5 min for P99.
- **Throughput:** Handle 5,000 requests/sec at peak.
- **Durability:** No job silently dropped. Every submission produces a result.
- **Cost:** Minimize spend during idle periods.

---

## Step 2: High-Level Architecture (Next 10 Minutes)

```
                         ┌─────────────────────────┐
                         │       Load Balancer       │
                         │    (health checks, TLS)   │
                         └────────────┬──────────────┘
                                      │
                         ┌────────────▼──────────────┐
                         │       API Gateway          │
                         │  - Rate limiting            │
                         │  - Auth (JWT / API key)     │
                         │  - Request validation       │
                         │  - Returns 202 Accepted     │
                         └────────────┬──────────────┘
                                      │ publishes
                         ┌────────────▼──────────────┐
                         │       Message Queue        │
                         │   (Pub/Sub, SQS, Kafka)    │
                         │   Durable, at-least-once   │
                         └────────────┬──────────────┘
                                      │ workers pull
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
     ┌────────▼────────┐    ┌────────▼────────┐    ┌────────▼────────┐
     │    Worker 1      │    │    Worker 2      │    │    Worker N      │
     │  (Stateless pod) │    │  (Stateless pod) │    │  (Stateless pod) │
     └────────┬────────┘    └────────┬────────┘    └────────┬────────┘
              │                       │                       │
              └───────────────────────┼───────────────────────┘
                                      │ reads/writes
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
     ┌────────▼────────┐    ┌────────▼────────┐    ┌────────▼────────┐
     │  Object Storage  │    │    Database      │    │     Cache        │
     │  (GCS / S3)      │    │  (PostgreSQL)    │    │   (Redis)        │
     │  Files, results  │    │  Job state       │    │  Rate limits     │
     │  99.999% durable │    │  Checkpoints     │    │  Sessions        │
     └─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Why This Shape?

1. **API Gateway separated from workers:** API returns 202 instantly. Processing happens async. Keeps P99 response time < 200ms regardless of job complexity.
2. **Queue in the middle:** Decouples producers from consumers. Gives you retry, load balancing, and backpressure for free.
3. **Stateless workers:** Any worker can process any job. Kill one, another takes over. This is what makes auto-scaling and spot VMs possible.
4. **External storage for all state:** Workers have nothing to lose on crash.

---

## Step 3: Scaling Under Heavy Load

### Horizontal Scaling (Auto-Scaling Groups)

```
Scaling Signals (ranked by usefulness):
  1. Queue depth (BEST)     → Scale before workers are overwhelmed
  2. CPU utilization         → Scale when workers are busy
  3. Request rate            → Predictive, but doesn't reflect processing time

Auto-Scaling Config:
  Min pods:     0  (scale-to-zero during idle — $0 compute)
  Max pods:     500
  Scale-up:     +10 pods when queue > 50 messages, evaluated every 15s
  Scale-down:   -1 pod every 60s when queue = 0 (slow drain, avoid flapping)
  Cooldown:     5 minutes after last scale event
```

**Why scale to zero?**
If your service is bursty (200K jobs at 2 AM, then nothing until 4 PM), always-on infrastructure is wasteful:

```
Always-on:    100 workers × $0.10/hr × 24h × 365d = $87,600/year
Scale-to-zero: 100 workers × $0.10/hr × 4h × 365d  = $14,600/year  (83% savings)
With spot VMs: 100 workers × $0.03/hr × 4h × 365d  = $4,380/year   (95% savings)
```

### Cold Start Mitigation

Scale-to-zero means cold starts. To minimize:

```
Mitigation Strategy:
  1. Pre-pull container images on all nodes (DaemonSet)     → saves 10-30s
  2. Keep 2-3 "warm" nodes with no pods (spot, ~$5/day)     → saves 60-120s
  3. Lazy-load heavy resources (ML models, large configs)    → saves 5-10s
  4. Result: 0 → first pod running in < 15 seconds
```

### Spot / Preemptible VMs

```
How spot VMs work:
  - Cloud provider sells excess capacity at 60-80% discount
  - Can reclaim with 30-second warning (SIGTERM)
  - Your service MUST handle graceful shutdown

On SIGTERM:
  1. Stop pulling new messages from queue
  2. Finish current batch (if < 10s remaining) OR NACK it
  3. Save checkpoint of progress
  4. Exit cleanly

  → Message returns to queue. Another worker picks it up.
  → At most 30 seconds of work re-done (if checkpointed properly).
```

**When NOT to use spot:** Long-running jobs (> 1 hour) where re-processing is expensive. Use on-demand for those and spot for the rest.

---

## Step 4: Reliable Storage & Durable Queues

### Choosing Your Queue

| Queue | Best For | Guarantees |
|-------|----------|------------|
| **Pub/Sub (GCP)** | At-least-once, serverless, auto-scales | 7-day retention, no ordering guarantee |
| **SQS (AWS)** | At-least-once, simple, cheap | 14-day retention, FIFO option |
| **Kafka** | Exactly-once (with transactions), ordering, replay | You manage brokers (or use Confluent) |

**For most services, Pub/Sub or SQS is enough.** Use Kafka only if you need strict ordering, event replay, or multiple consumers reading the same events.

### At-Least-Once vs. Exactly-Once

```
At-least-once delivery (Pub/Sub, SQS):
  The queue guarantees every message is delivered AT LEAST once.
  But it might deliver it twice (e.g., after a timeout with no ACK).
  → YOUR SERVICE must be idempotent (processing twice = same result).

Exactly-once semantics (Kafka transactions):
  The queue + consumer can guarantee each message is processed exactly once.
  → More complex, requires Kafka transactions + consumer offset management.
  → Usually overkill. Just make your service idempotent.
```

### Making Operations Idempotent

```
Idempotency Key Pattern:
  Client sends:  POST /v1/jobs  { ..., "idempotency_key": "abc-123" }
  
  Server:
    1. Check Redis: EXISTS idempotency:abc-123
    2. If exists → return cached result (don't re-process)
    3. If not    → process, store result in Redis with TTL 24hr

Database Writes:
  Use UPSERT (INSERT ... ON CONFLICT DO UPDATE)
  or MERGE INTO — both are idempotent.
  Applying the same write twice produces the same result.
```

### Storage Tiers

```
Hot:   Redis / Memcached    ← < 1ms, volatile, rate limits + sessions
Warm:  PostgreSQL / MySQL   ← < 10ms, durable, job state + metadata
Cold:  GCS / S3             ← < 100ms, durable, files + results + backups
```

---

## Step 5: Failure Handling (The Core of This Interview)

### HTTP Status Codes — Know Which Are Retriable

```
RETRIABLE — The same request might succeed if you try again:
  408  Request Timeout         → Server didn't respond in time
  429  Too Many Requests       → Rate limited — respect Retry-After header
  500  Internal Server Error   → Server crashed, might recover
  502  Bad Gateway             → Upstream temporarily unreachable
  503  Service Unavailable     → Server overloaded or in maintenance
  504  Gateway Timeout         → Upstream too slow

NOT RETRIABLE — Retrying won't help, fix the request:
  400  Bad Request             → Malformed payload
  401  Unauthorized            → Invalid/expired credentials
  403  Forbidden               → No permission (valid creds)
  404  Not Found               → Resource doesn't exist
  409  Conflict                → Duplicate or state conflict
  422  Unprocessable Entity    → Valid syntax, invalid semantics

Nuances:
  429 → Always retry, but respect the Retry-After header if present
  401 → Retriable IF you can refresh an expired token automatically
  409 → Retriable IF the conflict is a race condition (retry with backoff)
  500 → Retry cautiously — could be a bug that always fails
```

### Exponential Backoff with Jitter

**Why not fixed delay retries?**

```
Fixed retry (1s, 1s, 1s):
  100 clients all retry at the same intervals
  → "Thundering herd" — synchronized retries overwhelm the server
  → Server recovers, gets hit again, goes down again

Linear retry (1s, 2s, 3s):
  Better, but still somewhat synchronized

Exponential backoff (1s, 2s, 4s, 8s):
  Spreads retries out over time
  But 100 clients all retry at exactly 4s → still synchronized
```

**The solution: exponential backoff + full jitter:**

```python
import random, time

def retry_with_backoff(func, max_retries=5, base=1.0, cap=60.0):
    for attempt in range(max_retries):
        try:
            return func()
        except RetriableError as e:
            if attempt == max_retries - 1:
                raise  # Final attempt failed — give up

            # Exponential backoff + full jitter
            max_delay = min(cap, base * (2 ** attempt))
            delay = random.uniform(0, max_delay)

            # Respect Retry-After header if present (HTTP 429)
            if hasattr(e, 'retry_after') and e.retry_after:
                delay = max(delay, e.retry_after)

            time.sleep(delay)
```

```
Example with 3 different clients (all failing at T=0):

           Client A          Client B          Client C
Attempt 0: wait 0.3s         wait 0.8s         wait 0.1s
Attempt 1: wait 1.7s         wait 0.4s         wait 1.9s
Attempt 2: wait 2.1s         wait 3.8s         wait 1.2s
Attempt 3: wait 5.9s         wait 2.1s         wait 7.4s

→ Retries are spread uniformly. No thundering herd.
→ Server has breathing room to recover.
```

### Circuit Breaker Pattern

```
         success               failure threshold met
    ┌──────────────┐          (e.g., 5 failures in 10 calls)
    │              │
    ▼              │
  CLOSED ──────────────────────▶ OPEN
  (normal)                       (fail-fast, no calls)
    ▲                               │
    │                               │ timeout (e.g., 30 seconds)
    │                               ▼
    │                           HALF-OPEN
    │                           (send ONE probe request)
    │         probe succeeds        │
    └───────────────────────────────┘
              probe fails → back to OPEN

Why circuit breaker?
  Without:  Worker calls dead service → waits 30s timeout → retry → waits again
            → 5 retries × 30s = 2.5 minutes WASTED per request
  With:     After 5 failures, circuit opens → instant failure (< 1ms)
            → No wasted time, no load on dying server
            → Probe every 30s to detect recovery
```

### Timeouts — Every External Call Needs One

```
Timeout Budget:
  API Gateway → Backend:    10 seconds
  Backend → Database:       5 seconds  (connect: 2s, query: 5s)
  Backend → Cache (Redis):  1 second
  Backend → Pub/Sub:        5 seconds
  Worker → External API:    10 seconds
  Worker → Storage (GCS):   30 seconds

Rule: Total timeout < caller's timeout
  If your API has a 10s timeout, your DB call can't be 15s.

Rule: Timeout < Queue visibility timeout
  If the queue redelivers after 5 min, your processing must finish in < 5 min.
  Otherwise, message is redelivered while you're still working → duplicate.
  Fix: Extend the lease (heartbeat) while processing.
```

### Dead Letter Queue (DLQ)

```
Main Queue ──(max 5 delivery attempts)──▶ Dead Letter Queue
                                              │
                                              ▼
                                        Alert fires
                                        Manual investigation
                                        Fix + re-drive back to main queue

Why 5 attempts?
  Transient failures (network blip, brief overload) resolve within 2-3 retries.
  If it fails 5 times, it's probably a permanent error:
    - Corrupt input data
    - Schema mismatch
    - Bug in processing logic
  These won't self-heal — alert a human.
```

### Retry Budget

```
Advanced: Instead of per-request retry limits, use a RETRY BUDGET.

Allow retries as long as retries < 10% of total requests in the last minute.

Why? If a downstream service starts failing for ALL requests,
unlimited retries from all clients would 10x the load and kill it.
A retry budget caps total retry traffic system-wide.

  Normal:  1000 req/min, 5 fail, 5 retries → 1005 total (0.5% retry ratio ✓)
  Failing: 1000 req/min, 800 fail → budget allows only 100 retries (10%)
           → 1100 total, not 1800. Server gets breathing room.
```

---

## Step 6: Observability (Logs, Metrics, Alerts, Dashboards)

### Metrics (What Is Happening)

Use **RED metrics** for every service:

```
Rate:     http_requests_total{service, method, status_code}
Errors:   http_requests_total{service, status=~"5.."}
Duration: http_request_duration_seconds{service} (histogram)

Key derived metrics:
  Error rate = sum(rate(requests{status=~"5.."}[5m])) / sum(rate(requests[5m]))
  P99 latency = histogram_quantile(0.99, rate(duration_bucket[5m]))
  Queue depth = pubsub_subscription_num_undelivered_messages
  Worker utilization = busy_workers / total_workers
```

### Logs (Why Did It Happen)

```
Structured JSON — never use unstructured text logs:

{
  "timestamp": "2025-01-15T10:00:34.123Z",
  "level": "ERROR",
  "service": "job-worker",
  "message": "Processing failed after 3 retries",
  "job_id": "job-a1b2c3",
  "error_code": "TIMEOUT",
  "error_message": "Snowflake COPY INTO exceeded 300s deadline",
  "attempt": 3,
  "trace_id": "4bf92f3577b34da6",     ← links to distributed trace
  "worker_id": "worker-pod-7f8d9",
  "duration_ms": 301234
}

Why structured?
  - Searchable (filter by job_id, error_code)
  - Parseable (aggregate error counts by type)
  - Correlatable (trace_id links log → trace → metric)
```

### Distributed Tracing (Where Did Time Go)

```
A single job touches multiple services. Tracing shows the full picture:

API Gateway ──span: gateway.handle (3ms)─────────────────────────┐
  │                                                                │
Job Service ──span: job.submit (12ms)──────────────────┐          │
  │                                                     │          │
  ├── span: validate_input (2ms)                        │          │
  ├── span: persist_to_db (8ms)                         │          │
  └── span: publish_to_queue (2ms)                      │          │
                                                         │          │
Worker ──span: worker.process (4500ms)────────┐         │          │
  │                                            │         │          │
  ├── span: download_file (120ms)              │         │          │
  ├── span: transform (3200ms)    ← SLOW!      │         │          │
  ├── span: upload_result (90ms)               │         │          │
  └── span: checkpoint (5ms)                   │         │          │
                                                └─────────┘──────────┘

Diagnosis: transform step took 3.2s (usually 500ms).
Click span → see attributes: file_size=2.5MB, complexity=high
→ Root cause found in 2 minutes instead of 30.
```

### Tail-Based Sampling (Cost Control)

```
Problem: 5,000 req/sec × 8 spans × 1KB = 3.4 TB/day of traces
At $0.10/GB = $340/day = $10,200/month  ← too expensive

Solution: Keep the interesting traces, drop the boring ones:
  - 100% of ERROR traces    (always keep failures)
  - 100% of SLOW traces     (P99, > 5s)
  - 1% of SUCCESS traces    (statistical sample)

  Result: ~207 GB/day → $621/month (94% savings)

Why TAIL-based (not HEAD-based)?
  Head-based: Decision at request START → don't know if it will fail yet
    → Randomly drops 99% of error traces (they're rare)
  Tail-based: Decision AFTER request completes → keep all errors
    → Every interesting trace is preserved
```

### Alerting (SLO-Based, Not Threshold-Based)

```
BAD alerting (threshold-based):
  "Alert if error rate > 1%"
  → Brief spikes trigger for self-healing issues → noisy
  → 0.5% sustained for hours doesn't page → silent failure

GOOD alerting (SLO burn rate):
  SLO: 99.9% success rate → error budget = 0.1% = 43 min/month

  Burn rate = (actual error rate) / (tolerated error rate)

  ┌───────────┬──────────┬──────────────┬────────────────────┐
  │ Burn Rate │ Window   │ Budget Used  │ Action             │
  ├───────────┼──────────┼──────────────┼────────────────────┤
  │ 14.4x     │ 1 hour   │ 2%           │ PAGE (critical)    │
  │ 6x        │ 6 hours  │ 5%           │ PAGE (warning)     │
  │ 3x        │ 1 day    │ 10%          │ Slack alert        │
  │ 1x        │ 3 days   │ 10%          │ Ticket (non-urgent)│
  └───────────┴──────────┴──────────────┴────────────────────┘

  14.4x burn rate = you'll exhaust your monthly error budget in 72 minutes.
  That deserves a 2 AM page.

  0.5% error rate sustained 6 hours = 3x burn rate → Slack alert.
  A brief 2% spike for 30 seconds? Doesn't page — it self-healed.
```

### Dashboard (One Page, Glanceable)

```
Service Health Dashboard:
  ┌──────────────────────────┬──────────────────────────┐
  │ Request Rate (req/sec)    │ Error Rate (%)            │
  │ ████████████ 4,200        │ ▁▁▁▂▁▁▁▁ 0.03%           │
  ├──────────────────────────┼──────────────────────────┤
  │ P99 Latency (ms)          │ Queue Depth               │
  │ ────────▲── 180ms         │ ▁▁▃▅▇█▇▅▃▁ peak: 350     │
  ├──────────────────────────┼──────────────────────────┤
  │ Active Workers: 48/50     │ SLO Budget: 92% remaining │
  │ Spot preemptions: 2/hr    │ DLQ Messages: 0           │
  └──────────────────────────┴──────────────────────────┘
```

---

## Step 7: Health Checks & Graceful Shutdown

### Health Check Endpoints

```
GET /healthz      → Liveness probe
  Returns 200 if the process is running.
  Kubernetes restarts the pod if this fails.
  Keep it simple: return 200. Don't check dependencies.

GET /readyz       → Readiness probe
  Returns 200 if the service can handle traffic.
  Check: Can I reach the database? Is the queue accessible?
  If this fails, the pod is REMOVED from the load balancer (not killed).

Why separate?
  A pod might be LIVE (process running) but not READY (database is down).
  You don't want to kill it (it might recover). You just stop sending traffic to it.
```

### Graceful Shutdown

```
On SIGTERM (Kubernetes is stopping the pod):

  1. Stop accepting new requests / stop pulling from queue
  2. Finish in-flight requests (grace period: 30 seconds)
  3. Flush logs and metrics
  4. Close database connections cleanly
  5. Exit with code 0

  In Kubernetes:
    terminationGracePeriodSeconds: 30

  If the pod doesn't exit in 30s → SIGKILL (force kill).
  Design your processing so each unit of work completes in < 30s,
  or use the heartbeat/lease extension pattern for longer jobs.
```

---

## Step 8: Trade-offs (What the Interviewer Really Wants)

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Async with queue | Synchronous processing | Decoupling, retry, scaling, backpressure. Sync means one failure blocks everything |
| Pub/Sub over Kafka | Kafka for everything | Pub/Sub is managed, scales to zero for cost. Kafka requires broker management. Use Kafka only if you need ordering or replay |
| Spot VMs | On-demand VMs | 60-80% savings. Workers are stateless — preemption is safe |
| Scale-to-zero | Keep minimum pods | $0 during idle. Cold start mitigated by image pre-pulling |
| SLO-based alerts | Static thresholds | Burns catch slow bleeds. Thresholds are either too noisy or too slow |
| Tail-based sampling | Sample everything / nothing | 94% cost savings while keeping 100% of error traces |
| Redis for rate limits | Database-backed | Sub-millisecond reads. Rate limiting must be fast to not add latency |
| PostgreSQL for state | Redis for state | Job state must survive restarts. Redis can lose data. Postgres is durable |

---

## Step 9: Adapting to New Constraints

### "Now make it multi-tenant"

```
Isolation layers (defense in depth):

  1. Compute isolation:
     ResourceQuotas per tenant namespace → guaranteed CPU/memory
     PriorityClasses → premium tenants preempt free-tier during contention

  2. Network isolation:
     NetworkPolicies → tenant A's pods can't talk to tenant B's pods

  3. Data isolation:
     Separate storage buckets per tenant (IAM-enforced)
     Per-tenant encryption keys (CMEK) → key revocation = instant data wipe

  4. Rate limiting per tenant:
     Premium: 10,000 req/min
     Standard: 1,000 req/min
     Free: 100 req/min
     Redis sliding window counter per tenant ID
```

### "Make it highly available"

```
Multi-AZ deployment (within one region):
  - 3 availability zones (standard for cloud)
  - Workers spread across zones (pod anti-affinity)
  - Database: Multi-AZ replica (automatic failover)
  - Queue: Managed service (already multi-AZ)
  - Storage: Automatically replicated across zones

  Failure: One AZ goes down → 33% capacity lost temporarily
  Recovery: Auto-scaling replaces pods in remaining zones
  RPO: 0 (synchronous replication)
  RTO: < 5 minutes (automatic failover)
```

### "Now multi-region for disaster recovery"

```
Active-Passive (simpler):
  Primary region handles all traffic.
  Secondary region has infrastructure provisioned but idle.
  Data replicated asynchronously (RPO: minutes).
  On failure: DNS failover to secondary. Resume from checkpoints.
  RTO: 5-15 minutes.
  Cost: ~30% premium for standby infra.

Active-Active (harder):
  Both regions serve traffic simultaneously.
  Global load balancer routes to nearest region.
  Database needs cross-region replication (Spanner / CockroachDB).
  RTO: < 1 minute. RPO: ~0 (sync replication).
  Cost: 2x infrastructure.
  Only justify for 99.99%+ SLA.
```

### "Reduce cost by 50%"

```
  - Spot/preemptible VMs       → 60-80% compute savings (already doing this)
  - Scale-to-zero              → $0 during idle hours (already doing this)
  - Right-size instances       → Don't use 16GB pods for 2GB workloads
  - Reserved instances         → 30-60% discount for steady-state baseline
  - Off-peak scheduling        → Run batch jobs at night (cheaper spot prices)
  - Compress stored data       → 3-5x less storage cost
  - Tail-based trace sampling  → 94% less observability storage cost
```

---

## Step 10: Interview Pushback Responses

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if the queue goes down?" | "Pub/Sub and SQS are managed services with 99.95%+ SLA, replicated across zones. If the queue is down, we circuit-break: stop publishing, buffer locally (or return 503), and retry with backoff. No data lost — the API hasn't ACK'd the client request." |
| "How do you debug a slow request?" | "Distributed tracing. Every request gets a trace ID propagated across all services. I look at the trace waterfall, find the slow span, click into it to see attributes (file size, query duration), then jump to correlated logs via trace_id. Root cause in < 5 minutes." |
| "Isn't this overengineered?" | "Each component earns its place. The queue gives us retry + decoupling. Checkpointing gives us resumability. Observability gives us debugging speed. Remove any one and you get silent data loss, restart-from-zero failures, or 30-minute debugging sessions. The first production incident pays for all of this." |
| "Why not just Kubernetes HPA?" | "HPA can't scale to zero — minimum is 1 replica. KEDA (Kubernetes Event-Driven Autoscaler) supports true scale-to-zero based on queue depth. For bursty workloads, the difference is $0/hr idle vs. $X/hr idle." |
| "What about data consistency?" | "We use idempotent operations (UPSERT / MERGE INTO), so retries are safe. Checkpoints are written AFTER the data commit — never before. This guarantees at-least-once delivery + idempotency = exactly-once semantics at the application level." |
| "Circuit breaker seems complex." | "It's 20 lines of code (or use a library). Without it, when a downstream service dies, every worker spends 30s hitting a dead endpoint, 5 retries × 30s = 2.5 minutes wasted per request. With circuit breaker: instant failure, no wasted resources, probe for recovery every 30s." |

---

## How to Structure Your 1-Hour Answer

```
Minutes 0-5:   Clarify requirements. Ask about traffic, SLA, budget.
Minutes 5-15:  Draw the high-level architecture on the whiteboard.
               Hit: API → Queue → Workers → Storage (separation of compute/storage)
Minutes 15-25: Scaling. Auto-scaling groups, queue-based scaling, spot VMs.
               This impresses because it shows cost-awareness.
Minutes 25-40: Failure handling (THE CORE — spend the most time here).
               Hit: HTTP codes (retriable vs not), exponential backoff + jitter,
               circuit breakers, timeouts, DLQ, idempotency.
Minutes 40-50: Observability. Metrics, structured logs, tracing, SLO-based alerts.
               Connect to business: "SLOs tied to customer contracts."
Minutes 50-60: Handle interviewer constraints (multi-tenant, HA, cost, multi-region).
               Show adaptability and tradeoff reasoning.
```

> **Winning strategy:** Start simple. Add complexity only when the interviewer introduces constraints. Every addition should come with a tradeoff: "We *could* do X, but it costs Y. Given the requirements, I'd recommend Z because..."

---

## Key Terms to Say Naturally

- **Horizontal scaling**, **auto-scaling group**, **scale-to-zero**
- **Spot instances**, **preemptible VMs**, **graceful shutdown** (SIGTERM)
- **Queue-based scaling**, **backpressure**, **visibility timeout**
- **At-least-once delivery**, **idempotency**, **UPSERT / MERGE INTO**
- **Dead letter queue** (DLQ), **poison pill** message
- **Exponential backoff**, **jitter** (full jitter, decorrelated jitter)
- **Circuit breaker** (closed → open → half-open)
- **Retry budget** (cap retry traffic at 10% of total)
- **HTTP 429 / 503**, **Retry-After header**, **retriable vs non-retriable**
- **Separation of compute and storage**, **stateless workers**
- **RED metrics** (Rate, Errors, Duration)
- **SLO**, **error budget**, **burn rate**
- **Tail-based sampling**, **trace-to-log correlation**
- **Liveness probe**, **readiness probe**
- **Multi-AZ**, **active-passive**, **RPO/RTO**
- **ResourceQuota**, **NetworkPolicy**, **rate limiting**
- **KEDA**, **cold start**, **image pre-pulling**
