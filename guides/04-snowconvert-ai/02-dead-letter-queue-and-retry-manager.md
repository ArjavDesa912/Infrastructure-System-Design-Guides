# Dead Letter Queue & Retry Manager

> **Interview Prompt:** "A SnowConvert worker fails to parse a 3,000-line Oracle stored procedure. The job has already been retried 5 times. Design the retry infrastructure — including exponential backoff, jitter, circuit breakers, and a dead letter queue — that ensures no job is silently lost and transient failures self-heal."

---

## 1. Requirements

### Functional
- Automatically retry failed conversion jobs with configurable retry policies.
- Implement exponential backoff with full jitter to prevent thundering herds.
- Route permanently failed jobs to a Dead Letter Queue (DLQ) with full diagnostic context.
- Provide a circuit breaker that halts retries when a downstream dependency (e.g., the AST parser service) is unhealthy.
- Expose a DLQ management UI: inspect, replay, purge, and bulk-retry.
- Support per-tenant and per-error-class retry policies (e.g., OOM errors get 2 retries; syntax errors get 0).

### Non-Functional
- **Zero silent job loss:** Every accepted job must reach COMPLETED or land in the DLQ.
- **Retry latency budget:** Total retry window ≤ 30 minutes per job before DLQ routing.
- **Observability:** Every retry attempt is logged with structured metadata (attempt number, delay, error class).
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

## 2. API Design

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

## 3. High-Level Architecture

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

## 4. Deep Dive: Retry Strategy

### 4.1 Exponential Backoff with Full Jitter

**Naive exponential backoff** causes retry storms — all workers that failed at the same time retry at the same time:

```
Naive:   delay = base × 2^attempt
         Attempt 0: 1s, Attempt 1: 2s, Attempt 2: 4s, Attempt 3: 8s
         Problem: 100 workers all retry at exactly 1s, 2s, 4s...
```

**Full jitter** randomizes the delay uniformly between 0 and the exponential ceiling:

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

### 4.2 Decorrelated Jitter (Alternative)

```
Decorrelated:  delay = min(cap, random(base, prev_delay × 3))

  More aggressive spread than full jitter.
  Each delay is based on the PREVIOUS delay, not the attempt number.
  Better for systems where retry timing matters more than ordering.
```

### 4.3 Error Classification Engine

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

## 5. Deep Dive: Circuit Breaker

### 5.1 Three-State Circuit Breaker

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

### 5.2 Configuration

```python
class CircuitBreakerConfig:
    failure_threshold: int = 10       # Failures to trip OPEN
    success_threshold: int = 3        # Successes in HALF-OPEN to close
    timeout_seconds: int = 30         # Time in OPEN before probing
    window_seconds: int = 60          # Sliding window for failure counting
    half_open_max_concurrent: int = 1 # Probes allowed simultaneously
```

### 5.3 Per-Dependency Circuit Breakers

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

### 5.4 Circuit Breaker + Retry Interaction

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

## 6. Deep Dive: Dead Letter Queue

### 6.1 DLQ Entry Schema

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

### 6.2 DLQ Processing Workflow

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

### 6.3 DLQ Alerting

```
Alert Rules:
  - DLQ ingest rate > 50 items/min           → PagerDuty Critical
  - DLQ depth > 1,000 items                  → Slack Warning
  - Single error class > 80% of DLQ items    → PagerDuty (systemic failure)
  - DLQ item age > 24 hours                  → Slack Reminder
  - DLQ replay failure rate > 50%            → PagerDuty (root cause not fixed)
```

---

## 7. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Retry storm after outage** | Recovered service re-overwhelmed | Full jitter spreads retries; circuit breaker limits concurrent probes |
| **DLQ overflow** | Storage fills up | 30-day TTL on DLQ items; alert at 80% capacity; auto-purge items older than TTL |
| **Incorrect error classification** | Retriable error treated as non-retriable → premature DLQ | Default to retriable for unknown errors; classification is a config, not code |
| **Circuit breaker flapping** | Rapidly opens/closes | Hysteresis: require 3 consecutive successes to close, not just 1 |
| **Clock skew on retry delay** | Workers compute different delays | Delay is computed per-worker, skew is irrelevant — jitter already randomizes |
| **DLQ replay causes cascading failure** | Replaying 10K items floods the system | Replay with rate limiting (e.g., 100 items/min); respect circuit breaker state during replay |

---

## 8. Trade-offs & Design Decisions

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Full jitter over equal jitter | Equal jitter or no jitter | Full jitter minimizes total completion time under contention (AWS research) |
| Per-dependency circuit breakers | Global breaker | Fault isolation — one bad parser shouldn't block all dialects |
| Cloud Tasks for delayed retry | Sleep in worker, or Pub/Sub scheduling | Workers shouldn't block on sleep; Cloud Tasks gives exactly-once delayed delivery |
| Error classification as config | Hardcoded in worker code | New error types appear constantly; config-driven lets ops adjust without deploys |
| 30-min total retry budget | Unlimited retries | Bounded retry window prevents resource waste; DLQ catches the rest |

---

## 9. Handling Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just retry immediately?" | "Immediate retries amplify the problem. If a service is overloaded and 100 requests fail, retrying all 100 immediately doubles the load. Exponential backoff gives the service breathing room, and jitter ensures retries don't arrive in synchronized waves. This is critical for production resilience." |
| "Isn't a DLQ just sweeping failures under the rug?" | "The opposite — a DLQ makes failures visible and actionable. Without a DLQ, failed jobs disappear silently. With one, every failure is preserved with full context (retry history, error traces, payload), alerting fires, and ops can replay when the root cause is fixed. It's an acknowledgment that some failures need human judgment." |
| "Circuit breakers seem complex. Why not just retry more?" | "Retrying against a dead service is worse than not retrying at all — it wastes compute, burns retry budgets, and adds load to the failing service. A circuit breaker is a fast-fail mechanism. When the Teradata parser is down, we fail-fast in microseconds instead of waiting 30 seconds for a timeout, saving thousands of pod-seconds per minute." |
| "How do you test all these failure modes?" | "Chaos engineering. We inject failures in staging: kill the parser pod, simulate OOM with memory limits, add artificial latency with Istio fault injection, and trigger circuit breakers intentionally. We verify that retries backoff correctly, circuits trip at the right threshold, and DLQ entries have complete context." |
| "What about exactly-once processing on replay?" | "Replay is idempotent by design. Each job uses a deterministic output path and checks for existing completion status before processing. Replaying a completed job is a no-op. This is the same idempotency pattern used in the main processing pipeline." |

---

## 10. Summary: Your Interview Narrative

> "I'd design a **three-layer resilience system**: retry manager, circuit breaker, and dead letter queue. When a conversion job fails, the Retry Manager first classifies the error — transient errors like OOM or timeouts get retried with **exponential backoff and full jitter** (randomized delay = `random(0, min(cap, base × 2^attempt))`), while non-retriable errors like invalid SQL go straight to the DLQ. Before each retry, we check the **per-dependency circuit breaker** — if the Oracle parser has failed 10 times in 60 seconds, the circuit opens and we fail-fast instead of burning retry budgets. Jobs that exhaust retries land in the **DLQ with full diagnostic context**: original payload, every retry attempt with timestamps and errors, and circuit breaker state. The DLQ supports bulk replay filtered by error class — so when we fix the OOM issue by increasing worker memory, we replay all WORKER_OOM items in one call. Alerting fires on DLQ depth, ingest rate, and error class concentration."

---

## 11. Key Terms to Drop Naturally

- **Exponential backoff**, **full jitter**, **decorrelated jitter**
- **Circuit breaker** (CLOSED → OPEN → HALF-OPEN)
- **Dead Letter Queue (DLQ)**, **poison message**
- **Error classification** (retriable vs. non-retriable)
- **Fail-fast**, **fast failure**
- **Thundering herd**, **retry storm**
- **Chaos engineering**, **fault injection**
- **Idempotent replay**, **bulk recovery**
- **429 Too Many Requests**, **Retry-After header**
- **Hysteresis**, **sliding window failure count**
