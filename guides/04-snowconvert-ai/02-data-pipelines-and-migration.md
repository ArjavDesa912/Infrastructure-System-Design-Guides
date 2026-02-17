# System Design: Scalable Data Migration Pipeline

> **The Question:** "Design a system that migrates terabytes of data from a legacy database (Oracle/SQL Server) to Snowflake. It needs to handle failures gracefully, scale under heavy load, and be observable. Walk me through how you'd build it."

> **Duration:** ~1 hour. The interviewer will introduce constraints (cost, multi-region, higher availability) as you go. What matters is your tradeoffs, prioritization, and how you adapt.

---

## Step 1: Clarify Requirements (First 5 Minutes)

Ask these before drawing anything:

| Question | Why It Matters |
|----------|---------------|
| How much data? (GB vs TB vs PB) | Determines if you need parallelism at all |
| One-time migration or ongoing sync? | One-time = bulk load. Ongoing = CDC pipeline |
| Downtime tolerance? | Zero-downtime changes the entire design |
| Data formats? (CSV, Parquet, DB export) | Affects normalization layer |
| SLAs? (latency, throughput, durability) | Drives your non-functional requirements |

### Functional Requirements
- Accept data exports from legacy databases (Oracle DMP, CSV, Parquet, SQL Server BCP).
- Transform, validate, and load into Snowflake.
- Track progress per table/file — support resume after failure.
- Provide real-time visibility into pipeline health and progress.

### Non-Functional Requirements
- **Throughput:** 1 TB/hour sustained.
- **Durability:** Zero data loss — every source row appears in the target.
- **Resumability:** If the pipeline crashes at 60%, resume from 60%.
- **Cost:** Minimize Snowflake credits and cloud compute spend.

### Back-of-the-Envelope Numbers
```
Data volume:       10 TB across 500 tables
Target time:       10 hours → 1 TB/hour → ~278 MB/sec
Workers needed:    50 pods (each handles ~5.5 MB/sec)
Checkpoint store:  50 workers × 2 checkpoints/min × 20 hrs = 120K records (~240 MB)
Staging storage:   ~15 TB (source + converted Parquet)
```

---

## Step 2: High-Level Architecture (Next 10 Minutes)

Draw this on the whiteboard. Three layers, loosely coupled:

```
┌──────────────────────────────────────────────────────────────┐
│                     Control Plane                             │
│  API Gateway → Job Service → PostgreSQL (job state)          │
│  Accepts migration requests, tracks overall progress         │
└──────────────┬───────────────────────────────────────────────┘
               │ publishes tasks
               ▼
┌──────────────────────────────────────────────────────────────┐
│                     Message Queue                             │
│  Pub/Sub (or SQS / Kafka)                                    │
│  One message per table partition → natural load balancing    │
│  Durable, at-least-once delivery                             │
└──────────────┬───────────────────────────────────────────────┘
               │ workers pull
               ▼
┌──────────────────────────────────────────────────────────────┐
│                     Worker Fleet                              │
│  GKE pods (or EC2 ASG / ECS tasks)                           │
│  Stateless — all state in external stores                    │
│  Auto-scales 0 → 500 based on queue depth                   │
│                                                               │
│  Each worker:                                                 │
│    1. Pull task from queue                                    │
│    2. Extract data from source                                │
│    3. Convert to Parquet (optimal for Snowflake)              │
│    4. Stage to cloud storage (GCS / S3)                       │
│    5. COPY INTO Snowflake                                     │
│    6. Checkpoint progress                                     │
│    7. ACK the message                                         │
└──────────────┬───────────────────────────────────────────────┘
               │ reads/writes
               ▼
┌──────────────────────────────────────────────────────────────┐
│                   Persistent Storage                          │
│  Cloud Storage (GCS/S3):  Staging area for Parquet files     │
│  PostgreSQL:              Checkpoints, job state, metadata   │
│  Snowflake:               Target database                    │
└──────────────────────────────────────────────────────────────┘
```

**Key design principle:** Separate compute from storage. Workers are ephemeral and stateless. All durable state lives in external stores. This means any worker can be killed and replaced without data loss.

---

## Step 3: Scaling (The Interviewer Will Push Here)

### Horizontal Scaling

Workers scale based on **queue depth** (messages waiting to be processed):

```
Scaling signal:  Pub/Sub subscription backlog
  0 messages  → 0 pods    (scale-to-zero, $0 compute cost)
  50 messages → 10 pods   (1 pod per 5 messages)
  500 messages → 100 pods
  Max: 500 pods

Scale-up:   ~2 minutes (schedule pod → pull image → start)
Scale-down: 5-minute cooldown after last message processed
```

**Why queue-based scaling, not CPU-based?**
CPU utilization is a lagging indicator — by the time CPU spikes, queue is already backing up. Queue depth is a leading indicator — you scale before the bottleneck hits.

### Auto-Scaling Groups

```
Worker Pool Configuration:
  ┌─────────────────────────────────────────────────┐
  │  Pool A: "Standard Workers" (80% of fleet)       │
  │  - 4 CPU, 8 GB RAM per pod                       │
  │  - Handles tables < 100M rows                    │
  │  - Spot/preemptible VMs (60-80% cost savings)    │
  │                                                   │
  │  Pool B: "Heavy Workers" (20% of fleet)           │
  │  - 16 CPU, 32 GB RAM per pod                      │
  │  - Handles tables > 100M rows                     │
  │  - On-demand VMs (need reliability for big jobs)  │
  └─────────────────────────────────────────────────┘
```

**Tradeoff callout:** Spot VMs save 60-80% but can be preempted with 30s notice. This is fine because our workers are stateless — on SIGTERM, they NACK the message (returns to queue) and exit. Another worker picks it up. You only lose the in-progress batch (at most 30 seconds of work if you checkpoint properly).

### Table Partitioning for Parallelism

Large tables are split across multiple workers:

```
Table: ORDERS (1.2B rows, 400 GB)

Split by a monotonic column (e.g., order_date):
  Worker 1: WHERE order_date BETWEEN '2024-01' AND '2024-03'  → 300M rows
  Worker 2: WHERE order_date BETWEEN '2024-04' AND '2024-06'  → 300M rows
  Worker 3: WHERE order_date BETWEEN '2024-07' AND '2024-09'  → 300M rows
  Worker 4: WHERE order_date BETWEEN '2024-10' AND '2024-12'  → 300M rows

Each partition = one queue message. Workers process independently.
No coordination needed — partitions are non-overlapping.
```

---

## Step 4: Durable Queues & Async Operations

### Why a Message Queue?

Without a queue, the API server calls workers directly → tight coupling, no retry, no load balancing. With a queue:

1. **Decoupling:** API writes a message and returns `202 Accepted`. Worker processes it later.
2. **Natural load balancing:** Fast workers pull more messages. Slow workers pull fewer.
3. **Durability:** If a worker dies, the message returns to the queue (visibility timeout / NACK).
4. **Retry:** Failed messages are automatically redelivered.
5. **Backpressure:** Queue depth tells you if you're falling behind.

### Message Flow

```
API Server                    Queue                    Worker
    │                           │                         │
    ├── publish(task) ─────────▶│                         │
    │   return 202              │                         │
    │                           │── deliver ─────────────▶│
    │                           │                         ├── process
    │                           │                         ├── checkpoint
    │                           │◀── ACK ─────────────────┤  (success)
    │                           │                         │
    │                           │── redeliver ───────────▶│  (if no ACK
    │                           │   (after timeout)       │   within 5 min)
```

### Dead Letter Queue (DLQ)

After N failed attempts, messages move to a dead-letter queue for investigation:

```
Main Queue ──(max 5 retries)──▶ Dead Letter Queue
                                    │
                                    ▼
                              Alert + manual review
                              (don't silently drop failures)
```

**Why 5 retries?** Enough to handle transient failures (network blips, temporary resource exhaustion). If it fails 5 times, it's likely a permanent error (corrupt data, schema mismatch) that won't self-heal.

---

## Step 5: Failure Handling (This Is Where You Win the Interview)

### HTTP Status Codes — Which Are Retriable?

This is a common interview question. Know these cold:

```
Retriable (transient — the same request might succeed later):
  408  Request Timeout         → Server didn't respond in time
  429  Too Many Requests       → Rate limited, back off and retry
  500  Internal Server Error   → Server bug, might be transient
  502  Bad Gateway             → Upstream server down temporarily
  503  Service Unavailable     → Server overloaded, try later
  504  Gateway Timeout         → Upstream server too slow

NOT Retriable (permanent — retrying won't help):
  400  Bad Request             → Your payload is malformed
  401  Unauthorized            → Invalid credentials
  403  Forbidden               → Valid creds, no permission
  404  Not Found               → Resource doesn't exist
  409  Conflict                → State conflict (e.g., duplicate key)
  422  Unprocessable Entity    → Semantically invalid

Edge case:
  401  COULD be retriable if your token expired and you can refresh it.
  409  COULD be retriable if the conflict is a race condition (retry with backoff).
```

### Exponential Backoff with Jitter

**The naive approach (don't do this):**
```
Retry 1: wait 1 second
Retry 2: wait 2 seconds
Retry 3: wait 4 seconds
...
```

**The problem:** If 100 workers all hit a 503 at the same time and all retry at exactly {1s, 2s, 4s}, they create "thundering herd" — 100 simultaneous retries that overload the recovering server again.

**The correct approach — exponential backoff + full jitter:**

```
wait_time = random(0, min(cap, base * 2^attempt))

Parameters:
  base     = 1 second     (initial delay)
  cap      = 60 seconds   (maximum delay)
  attempt  = retry number (0, 1, 2, ...)

Example:
  Attempt 0: random(0, 1s)   → e.g., 0.7s
  Attempt 1: random(0, 2s)   → e.g., 1.3s
  Attempt 2: random(0, 4s)   → e.g., 2.9s
  Attempt 3: random(0, 8s)   → e.g., 5.1s
  Attempt 4: random(0, 16s)  → e.g., 11.2s
```

**Why jitter matters:** It spreads retries uniformly across the time window. Instead of 100 clients hitting at T+4s, they hit at random times between T+0 and T+4s. This gives the server time to recover.

**Why a cap?** Without it, attempt 10 = 1024 seconds (~17 minutes). That's too long. Cap at 60s means you retry at most once per minute.

### Decorrelated Jitter (Even Better)

```
sleep = min(cap, random(base, sleep * 3))

This is AWS's recommended approach. Each retry's delay is
based on the PREVIOUS delay (not the attempt number),
creating more variation between clients.
```

### Circuit Breaker Pattern

When a downstream service is completely down, retrying is wasteful. Circuit breaker stops trying:

```
States:
  CLOSED  → Normal operation. Requests flow through.
            Track failure rate over a sliding window.

  OPEN    → Too many failures (e.g., >50% fail rate over 10 calls).
            All requests immediately fail-fast (no network call).
            Wait a timeout period (e.g., 30 seconds).

  HALF-OPEN → After timeout, allow ONE probe request through.
              If it succeeds → transition to CLOSED.
              If it fails → transition back to OPEN.

         success
   ┌──────────┐
   ▼          │
 CLOSED ──(failure threshold)──▶ OPEN ──(timeout)──▶ HALF-OPEN
   ▲                                                    │
   └────────────────(probe succeeds)────────────────────┘
```

**Why not just retry forever?**
1. **Wasted resources:** Workers burning CPU on requests that will fail.
2. **Cascading failure:** Your retries add load to an already-struggling server, making it worse.
3. **Latency:** If the downstream is dead, fail fast (1ms) instead of waiting for timeout (30s).

### Timeouts

Every external call needs a timeout. Without one, a hung connection blocks the worker forever.

```
Timeout Strategy:
  Database queries:     30 seconds
  HTTP calls:           10 seconds (connect: 3s, read: 10s)
  Snowflake COPY INTO:  5 minutes (large loads take time)
  Queue message ACK:    5 minutes (ack deadline)

Rule: Timeout < Queue visibility timeout
  If processing might take 3 minutes, set visibility timeout to 5 minutes.
  If processing exceeds 5 minutes, the message reappears in the queue
  → another worker picks it up → potential duplicate processing.
  Solution: Extend the lease periodically (heartbeat pattern).
```

### Idempotency (Making Retries Safe)

Retries mean the same operation might run twice. Design for it:

```
Snowflake MERGE INTO (idempotent upsert):
  MERGE INTO target_table t
  USING staging_table s ON t.pk = s.pk
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ...

  Running this twice with the same data = same result. No duplicates.

Idempotency key pattern (API level):
  POST /v1/migrations
  X-Idempotency-Key: "migration-42-orders-Q3"

  Server checks: "Have I seen this key before?"
  If yes → return cached result (don't re-process)
  If no  → process and store result keyed by idempotency key
```

---

## Step 6: Checkpointing & Resumability

### Watermark-Based Checkpoints

Every 30 seconds (or every 1,000 rows), each worker saves its progress:

```
Checkpoint record:
  {
    migration_id:  "mig-42",
    table_name:    "ORDERS",
    partition:     "2024-Q3",
    watermark:     987654321,    ← last processed order_id
    rows_done:     80000000,
    worker_id:     "worker-5"
  }

On crash and restart:
  SELECT * FROM checkpoints WHERE migration_id = 'mig-42';
  → Resume each partition from its watermark.

  Worker resumes with:
    SELECT * FROM ORDERS
    WHERE order_date BETWEEN '2024-07' AND '2024-09'
      AND order_id > 987654321     ← seeks via index, no full scan
    ORDER BY order_id;
```

### Critical: Checkpoint AFTER Data Commit

```
Correct order (exactly-once):
  1. Process batch (rows 45001–46000)
  2. Write to Snowflake (COPY INTO / MERGE)
  3. Wait for Snowflake COMMIT confirmation  ← must succeed first
  4. Write checkpoint (watermark = 46000)     ← only after step 3

If crash between steps 2 and 4:
  → Checkpoint still says 45000
  → On recovery: re-process rows 45001–46000
  → MERGE is idempotent → no duplicates

NEVER checkpoint before data commit.
  → That causes data LOSS (checkpoint says "done" but data isn't there).
```

---

## Step 7: Observability (Logs, Metrics, Alerts)

### The Three Pillars

```
1. METRICS — "What is happening?" (aggregated numbers)
   Rate:     migrations_rows_processed_total (counter)
   Errors:   migrations_errors_total{error_type="timeout|schema|corrupt"}
   Duration: migration_batch_duration_seconds (histogram)
   Queue:    pubsub_subscription_backlog (gauge)

2. LOGS — "Why did it happen?" (detailed events)
   Structured JSON with correlation IDs:
   {
     "timestamp": "2025-01-15T10:00:34Z",
     "level": "ERROR",
     "migration_id": "mig-42",
     "table": "ORDERS",
     "partition": "2024-Q3",
     "error": "Snowflake COPY INTO timeout after 300s",
     "rows_in_batch": 50000,
     "file": "gs://staging/orders-Q3-batch-47.parquet",
     "worker": "worker-5",
     "trace_id": "abc123"
   }

3. TRACES — "Where did time go?" (request flow across services)
   API Gateway (2ms)
     → Job Service (8ms)
       → Queue publish (5ms)
         → Worker download (120ms)
           → Transform to Parquet (800ms)
             → COPY INTO Snowflake (3200ms)  ← bottleneck found
```

### Dashboards

```
Migration Health Dashboard (one page):
  ┌────────────────────────┬────────────────────────┐
  │  Throughput (rows/sec)  │  Error Rate (%)         │
  │  ████████████ 12,500    │  ▁▁▁▂▁▁▁▁  0.03%       │
  ├────────────────────────┼────────────────────────┤
  │  Queue Depth            │  Active Workers         │
  │  ▁▁▃▅▇█▇▅▃▁  peak: 500 │  ████████  48/50        │
  ├────────────────────────┼────────────────────────┤
  │  Tables: 342/500 done   │  ETA: 3h 22m            │
  │  ████████████████░░░░░  │  Checkpoint: 2s ago     │
  └────────────────────────┴────────────────────────┘
```

### Alerting

```
Alert on symptoms, not causes:

  CRITICAL (pages on-call):
    - Migration throughput drops below 50% of expected for > 5 minutes
    - Error rate exceeds 5% over a 5-minute window
    - Queue depth growing for > 10 minutes (workers not keeping up)
    - No checkpoint written for > 5 minutes (worker might be stuck)

  WARNING (Slack notification):
    - Snowflake warehouse credit burn rate > budget
    - Worker restarts > 3 per hour
    - DLQ messages > 0
```

---

## Step 8: Trade-offs (What the Interviewer Really Wants)

### Key Design Decisions

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Pub/Sub queue | Direct API calls to workers | Decoupling, retry, load balancing, durability — queue is the shock absorber |
| Parquet intermediate format | Load CSV directly | Parquet is columnar, typed, compressed. Catches schema errors on cheap compute before expensive Snowflake credits |
| Watermark checkpoints | No checkpointing | Without checkpoints, any failure restarts from zero. 18 hours of wasted work vs. 30 seconds |
| Spot VMs for workers | On-demand VMs | 60-80% savings. Workers are stateless and idempotent — preemption is safe |
| PostgreSQL for checkpoints | Redis | Checkpoints must survive restarts. Redis can lose the last second. 100 writes/min is trivial for Postgres |
| MERGE INTO (idempotent) | INSERT | MERGE handles retries safely. INSERT would create duplicates |
| Queue-based autoscaling | CPU-based autoscaling | Queue depth is a leading indicator, CPU is lagging. Scale before the bottleneck |

### Cost Analysis

```
Option A: Always-on infrastructure
  50 workers × 24/7 × $0.15/hr = $65,700/year

Option B: Scale-to-zero + spot VMs (our design)
  50 workers × 4 hrs/day × $0.05/hr (spot) = $3,650/year
  + Storage: $200/month = $2,400/year
  Total: ~$6,000/year (91% cheaper)
```

---

## Step 9: Adapting to New Constraints

The interviewer will likely introduce one or more of these. Here's how to adapt:

### "Now make it multi-region for disaster recovery"

```
Active-Passive:
  Primary region runs the migration.
  Cloud Storage replication (GCS dual-region or S3 cross-region).
  Standby region has infrastructure provisioned but idle.
  On failure: DNS failover, resume from checkpoints (stored in replicated DB).
  RPO: ~30 seconds (checkpoint interval)
  RTO: ~5 minutes (spin up workers + resume)

Active-Active (much harder, probably overkill for migration):
  Both regions process different tables simultaneously.
  Need global checkpoint coordination.
  Only justify if SLA demands < 1 minute RPO.
```

### "The client can't afford any downtime during the switch"

```
Add a CDC (Change Data Capture) layer:
  1. Start bulk migration (handles historical data)
  2. Simultaneously start CDC from source DB's transaction log
  3. CDC captures all changes made during migration
  4. When bulk load finishes, CDC catches up (< 30s lag)
  5. Brief quiesce (block writes for ~30 seconds)
  6. Verify row counts match
  7. Flip DNS to point to Snowflake

  Cutover window: ~30 seconds
  Rollback: Keep source DB in read-only for 72 hours
```

### "Reduce costs by 50%"

```
  - Use spot/preemptible VMs (already doing this: 60-80% savings)
  - Schedule migrations during off-peak hours (cheaper spot prices)
  - Compress staging data (Parquet + Snappy = 3-5x compression)
  - Right-size Snowflake warehouse (XL only during COPY, suspend immediately after)
  - Scale-to-zero when idle ($0 compute cost)
```

---

## Step 10: Interview Pushback Responses

| Interviewer Says | Your Response |
|-----------------|---------------|
| "What if the queue itself goes down?" | "Pub/Sub and SQS are managed services with 99.95%+ availability and data replicated across zones. If the queue is down, everything is down. We circuit-break: workers pause and retry connecting with exponential backoff. No data loss because nothing was ACK'd." |
| "Isn't checkpointing overhead?" | "A 2KB record every 30 seconds to PostgreSQL is ~100 writes/min. That's nothing for Postgres. Compare to restarting an 18-hour migration from scratch — checkpointing is 0.01% overhead for 99.95% recovery savings." |
| "Why not just use a cron job?" | "Cron gives you one process. If it crashes, you restart from zero. No parallelism, no load balancing, no retry. A queue-based worker fleet gives you all of those, plus auto-scaling and observability, for very little additional complexity." |
| "How do you know it's working?" | "Three ways: (1) metrics dashboard shows throughput, error rate, queue depth; (2) alerts fire if throughput drops or errors spike; (3) post-migration validation — row counts and checksums between source and target." |
| "What about data validation?" | "Three phases: pre-load (schema check, type mapping), during-load (row counts per batch via checkpoints), post-load (SELECT COUNT(*) + HASH_AGG(*) between source and target). Any mismatch triggers an alert and blocks promotion." |

---

## How to Structure Your 1-Hour Answer

```
Minutes 0-5:   Clarify requirements (ask the questions in Step 1)
Minutes 5-15:  Draw the high-level architecture (Step 2)
Minutes 15-25: Discuss scaling — queue-based, horizontal, spot VMs (Step 3)
Minutes 25-35: Failure handling — this is the CORE (Step 5)
               Hit: retries, backoff+jitter, circuit breakers, idempotency
Minutes 35-45: Observability — metrics, logs, alerts (Step 7)
Minutes 45-55: Trade-offs and cost (Step 8)
Minutes 55-60: Handle interviewer constraints (Step 9)
```

> **The winning formula:** Don't try to design a perfect system. Design a simple one, explain the tradeoffs clearly, and show that you can adapt when constraints change. The interviewer is testing your thinking process, not your ability to memorize architectures.

---

## Key Terms to Say Naturally

- **Horizontal scaling**, **auto-scaling group**, **queue-based scaling**
- **Exponential backoff**, **jitter** (full jitter, decorrelated jitter)
- **Circuit breaker** (closed → open → half-open)
- **Idempotency**, **idempotent retry**, **MERGE INTO**
- **Dead letter queue** (DLQ)
- **Checkpoint**, **watermark**, **resume from failure**
- **At-least-once delivery**, **exactly-once semantics** (via idempotency)
- **Visibility timeout**, **NACK/ACK**, **message lease**
- **Spot instances**, **preemptible VMs**, **stateless workers**
- **Separation of compute and storage**
- **RED metrics** (Rate, Errors, Duration)
- **SLO**, **error budget**, **burn rate alerting**
- **Structured logging**, **trace correlation**
- **Backpressure**, **flow control**
- **Retriable vs. non-retriable errors**, **HTTP 429/503**
