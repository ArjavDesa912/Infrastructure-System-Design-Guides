# Part 3: Platform Infrastructure, Observability & Multi-Tenancy

> **Scope:** This guide consolidates three platform-level topics — end-to-end observability, separation of compute and storage, and multi-tenant resource isolation — into one reference. These are the "how does the platform operate at scale" questions that distinguish senior/staff answers.

---

## Candidate Feedback: Cloud Deployment Round

> *"The infrastructure round was less about designing a feature and more about operating a production service. I was asked: 'You have a service running in production on GKE. Walk me through how you'd ensure it's reliable, cost-efficient, observable, and isolated for multiple customers.' The interviewer wanted to see that I could think across the stack — from pod scheduling and resource quotas to distributed tracing and cost allocation. The winning moment was when I connected observability to business metrics: not just 'I'd add Prometheus' but 'I'd define SLOs tied to customer contracts, alert on burn rate, and use tail-based sampling to control tracing costs.'"*

**Key takeaway:** Interviewers want you to connect infrastructure decisions to business outcomes — cost, reliability, and customer experience.

---

# Section A: End-to-End Observability Platform

> **Interview Prompt:** "A SnowConvert conversion job takes 45 seconds instead of the expected 5 seconds. You need to find out why. Design the observability platform — tracing, metrics, and logging — that lets you go from 'job is slow' to 'root cause identified' in under 5 minutes."

---

## A.1 Requirements

### Functional
- Distributed tracing across all services (API gateway → queue → worker → storage).
- RED metrics (Rate, Errors, Duration) on every service.
- Structured, correlated logging (trace_id, span_id in every log line).
- SLO-based alerting with burn rate tracking.
- Unified dashboard: one page tells you system health.

### Non-Functional
- **Trace completeness:** 100% of error traces, 1% of success traces (tail-based sampling).
- **Metric resolution:** 15-second scrape interval.
- **Log retention:** 30 days hot (searchable), 1 year cold (GCS archive).
- **Alert latency:** < 60 seconds from metric anomaly to PagerDuty notification.
- **Cost efficiency:** Observability cost < 5% of total infrastructure cost.

### Capacity Estimation
```
Request rate:        5,000 req/sec
Spans per request:   avg 8 spans
Total spans:         40,000 spans/sec
After tail sampling: 100% errors (5%) + 1% success = ~2,400 spans/sec
Storage per span:    ~1 KB
Trace storage:       2,400 × 86,400 × 1KB ≈ 207 GB/day

Metrics:
  Series:            ~50,000 time series (cardinality)
  15s scrape:        50K × 5,760 points/day × 8B = ~2.3 GB/day

Logs:
  1,000 logs/sec × avg 500B = 43 GB/day
```

---

## A.2 Architecture — The Three Pillars

```
┌──────────────────────────────────────────────────────────────────┐
│                    Application Services                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ API Gateway  │  │ Job Service  │  │ Conversion Workers      │  │
│  │              │  │              │  │                         │  │
│  │ OTel SDK     │  │ OTel SDK     │  │ OTel SDK               │  │
│  │ → traces     │  │ → traces     │  │ → traces               │  │
│  │ → metrics    │  │ → metrics    │  │ → metrics               │  │
│  │ → logs       │  │ → logs       │  │ → logs                  │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────────┘  │
└─────────│────────────────│─────────────────────│─────────────────┘
          │                │                     │
          └────────────────┼─────────────────────┘
                           │ OTLP (gRPC)
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│              OTel Collector Fleet (DaemonSet)                    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Pipeline:                                                 │  │
│  │  Receivers → Processors → Exporters                        │  │
│  │                                                            │  │
│  │  Traces:  receive → tail-based sampling → Tempo            │  │
│  │  Metrics: receive → batch → Prometheus Remote Write        │  │
│  │  Logs:    receive → enrich (trace_id) → Loki              │  │
│  └────────────────────────────────────────────────────────────┘  │
└────────────────┬──────────────┬──────────────┬──────────────────┘
                 │              │              │
          ┌──────▼──────┐ ┌────▼──────┐ ┌─────▼──────┐
          │   Grafana    │ │ Prometheus │ │    Loki     │
          │   Tempo      │ │            │ │             │
          │  (traces)    │ │  (metrics)  │ │   (logs)    │
          └──────────────┘ └───────────┘ └────────────┘
                 │              │              │
          ┌──────▼──────────────▼──────────────▼──────┐
          │         Grafana (Unified Dashboard)         │
          │                                             │
          │  Trace → Metric → Log correlation           │
          │  SLO dashboards with burn rate               │
          │  Alert manager → PagerDuty / Slack           │
          └─────────────────────────────────────────────┘
```

---

## A.3 Deep Dive: Distributed Tracing with OpenTelemetry

### Trace Propagation

```
Client Request
  │
  ▼
API Gateway ──span: gateway.handle──────────────────────────────┐
  │                                                              │
  ▼                                                              │
Job Service ──span: job.submit────────────────────────┐         │
  │                                                    │         │
  ├── span: job.validate_input (2ms)                   │         │
  ├── span: job.persist_to_db (8ms)    ← pg_duration   │         │
  └── span: job.publish_to_pubsub (5ms)                │         │
                                                        │         │
Worker ──span: worker.process────────────────┐         │         │
  │                                           │         │         │
  ├── span: worker.download_file (120ms)      │         │         │
  ├── span: worker.ast_parse (800ms)          │         │         │
  ├── span: worker.transpile (3200ms)← SLOW!  │         │         │
  ├── span: worker.validate (400ms)           │         │         │
  └── span: worker.upload_output (90ms)       │         │         │
                                               │         │         │
Total: 45,000ms                                │         │         │
                                               └─────────┘─────────┘

Diagnosis: worker.transpile took 3200ms (usually 500ms)
  → Click span → see span attributes:
    file_size: 2.5MB, line_count: 8000, recursive_cte_count: 14
  → Root cause: large file with 14 recursive CTEs causing exponential AST traversal
```

### Context Propagation

```
Every HTTP/gRPC call carries:
  traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
               │  │                                │                  │
               │  trace_id                         span_id            sampled flag
               version

Pub/Sub message attributes:
  {"traceparent": "...", "tracestate": "..."}

Log line (structured JSON):
  {
    "timestamp": "2025-01-15T10:00:34.123Z",
    "level": "WARN",
    "message": "Transpilation slow for file",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "span_id": "00f067aa0ba902b7",
    "service": "conversion-worker",
    "file_size": 2500000,
    "duration_ms": 3200
  }

→ In Grafana: click trace → see all logs with same trace_id
→ Jump from "slow job" alert to exact log lines in seconds
```

---

## A.4 Deep Dive: Tail-Based Sampling

### The Cost Problem

```
40,000 spans/sec × 1KB × 86,400 sec = 3.4 TB/day
At $0.10/GB storage: $340/day = $10,200/month  ← too expensive

Solution: Tail-based sampling
  Keep:  100% of error traces     (5% of traffic)
  Keep:  100% of slow traces      (P99, ~1%)
  Keep:  1% of successful traces  (random sample)
  Drop:  ~93% of normal traces

  Result: 2,400 spans/sec → 207 GB/day → $621/month (94% savings)
```

### Why Tail-Based, Not Head-Based?

```
Head-based sampling (at request start):
  Decision made BEFORE knowing if request will fail or be slow.
  → Drops 99% of error traces (they're rare, so rarely sampled)

Tail-based sampling (after request completes):
  OTel Collector buffers spans for ~30 seconds.
  After trace is complete, evaluates sampling rules:
    - Error? → KEEP 100%
    - Duration > P99? → KEEP 100%
    - Otherwise → KEEP 1%
  
  → Every interesting trace is preserved.
  → Boring traces are still represented (1% sample = statistically valid for metrics).
```

### Collector Configuration

```yaml
processors:
  tail_sampling:
    decision_wait: 30s        # Buffer spans for 30s before deciding
    num_traces: 100000         # Max traces in decision buffer
    policies:
      - name: errors-always
        type: status_code
        status_code: {status_codes: [ERROR]}   # Keep all errors
      - name: slow-traces
        type: latency
        latency: {threshold_ms: 5000}          # Keep traces > 5s
      - name: sample-remainder
        type: probabilistic
        probabilistic: {sampling_percentage: 1} # 1% of the rest
```

---

## A.5 Deep Dive: RED Metrics & SLOs

### RED for Every Service

```
Rate:     requests_total{service, method, status}
Errors:   requests_total{service, method, status=~"5.."}
Duration: request_duration_seconds{service, method} (histogram)

Example PromQL:
  # Error rate
  sum(rate(requests_total{status=~"5.."}[5m]))
    / sum(rate(requests_total[5m]))

  # P99 latency
  histogram_quantile(0.99, sum(rate(request_duration_seconds_bucket[5m])) by (le))
```

### SLO-Based Alerting (Multi-Window, Multi-Burn-Rate)

```
SLO: 99.9% of conversion jobs complete successfully within 5 minutes

Error budget: 0.1% → ~43 minutes of downtime per month

Burn Rate = actual error rate / tolerated error rate

Alert tiers:
  ┌──────────┬─────────┬──────────┬────────────────────────┐
  │ Burn Rate│ Window  │ Budget   │ Action                 │
  │          │         │ Consumed │                        │
  ├──────────┼─────────┼──────────┼────────────────────────┤
  │ 14.4x    │ 1 hour  │ 2%       │ PagerDuty Critical     │
  │ 6x       │ 6 hours │ 5%       │ PagerDuty Warning      │
  │ 3x       │ 1 day   │ 10%     │ Slack Alert            │
  │ 1x       │ 3 days  │ 10%     │ Ticket (non-urgent)    │
  └──────────┴─────────┴──────────┴────────────────────────┘

14.4x burn rate for 1 hour = you'll exhaust your entire monthly error budget in 72 minutes.
This deserves a 2 AM page.
```

**Why burn rate instead of raw threshold?**
A threshold like "alert if error rate > 1%" is noisy — brief spikes trigger for self-healing issues. Burn rate measures *sustained* impact on the SLO budget. A 2% error spike that lasts 30 seconds won't page. A 0.5% error rate sustained for 6 hours will — because it's slowly eating the budget.

---

## A.6 Deep Dive: Structured Logging

```json
{
  "timestamp": "2025-01-15T10:00:34.123Z",
  "level": "ERROR",
  "logger": "conversion-worker",
  "message": "Transpilation failed for stored procedure",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "af7651916cd43dd8",
  "tenant_id": "tenant-42",
  "job_id": "job-a1b2c3d4",
  "file_ref": "gs://uploads/.../proc_get_orders.sql",
  "error": {
    "class": "UNSUPPORTED_DIALECT",
    "message": "CONNECT BY PRIOR not supported in target dialect",
    "source_line": 847,
    "source_construct": "hierarchical_query"
  },
  "context": {
    "attempt": 3,
    "worker_pod": "conversion-worker-7f8d9-xa2k1",
    "k8s_node": "gke-pool-1-abc123"
  }
}
```

### Trace-to-Log Correlation (The Money Feature)

```
In Grafana:
  1. Dashboard shows P99 latency spike at 10:00
  2. Click → Exemplar → Opens trace 4bf92f...
  3. Trace waterfall shows worker.transpile took 42s
  4. Click span → "View logs" → Loki query: {trace_id="4bf92f..."}
  5. See exact error: "CONNECT BY PRIOR not supported"
  6. Root cause identified in < 2 minutes

Without correlation:
  1. See latency spike
  2. Grep through 43 GB of logs for... what?
  3. Hope the timestamps align
  4. 30+ minutes to find root cause (maybe)
```

---

## A.7 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **OTel Collector OOM** | Traces/logs lost | DaemonSet per node (blast radius = 1 node) |
| **Prometheus disk full** | Metrics gap | Alert at 80% capacity; auto-compact; retention policy |
| **Cardinality explosion** | Prometheus OOM | Label allowlisting; drop high-cardinality labels |
| **Tail sampling buffer overflow** | Traces dropped | Increase buffer + add fallback head-based sampling |
| **Loki ingester crash** | Logs lost for 1 node | WAL (Write-Ahead Log) recovers on restart |

---

## A.8 Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| OTel over vendor SDK | Datadog/New Relic SDK | Vendor-neutral; avoids lock-in; single SDK for all signals |
| Tail-based over head-based sampling | Head-based | Tail preserves all errors/slow traces; head randomly drops them |
| Grafana stack (Tempo, Loki, Prometheus) | Elastic/Splunk | Open-source; no per-GB pricing; Grafana correlation is excellent |
| SLO burn rate alerts | Static threshold | Burn rate catches slow burns; static thresholds are either too noisy or too slow |
| DaemonSet collectors | Sidecar per pod | Lower resource overhead; one collector per node vs. one per pod |

---

## A.9 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Why not just use CloudWatch/Stackdriver?" | "Cloud-native tools work for basic metrics but lack deep correlation. Our workflow crosses 5 services — we need distributed traces with log correlation to diagnose cross-service latency. OTel gives us that with a single SDK and no vendor lock-in." |
| "Tail-based sampling adds latency." | "30-second buffer at the collector, not at the application. Application latency is unaffected. The 30s delay means traces appear in Grafana 30 seconds later — acceptable for after-the-fact debugging." |
| "How do you handle cardinality explosions?" | "Label allowlisting at the collector — we explicitly define which labels are permitted. A runaway pod that emits per-request-ID labels gets its labels stripped before reaching Prometheus. We also alert on series count approaching limits." |
| "Isn't this overengineered for a migration tool?" | "A migration tool runs on customer data. When a Fortune 500 client's 200K-file batch takes 18 hours instead of 2, we need to diagnose it in minutes, not hours. The observability investment pays for itself the first time we avoid a P1 incident." |

---

# Section B: Separation of Compute and Storage

> **Interview Prompt:** "SnowConvert's processing load is extremely bursty — 200K files at 2 AM, then nothing until 4 PM. How do you avoid paying for idle infrastructure? Design a system that scales to zero when idle and scales to 500 workers in minutes."

---

## B.1 Requirements

### Functional
- Decouple compute (workers) from persistent data (files, checkpoints, metadata).
- Scale compute independently of storage.
- Support scale-to-zero during idle periods.
- Workers must be stateless and interchangeable.
- Storage must be durable and consistently available regardless of compute state.

### Non-Functional
- **Scale-up latency:** 0 → 100 workers in < 3 minutes.
- **Scale-down:** Workers terminate within 5 minutes of idle.
- **Cost:** $0 compute cost during idle hours.
- **Availability:** Storage 99.999%, compute 99.95%.
- **Data locality:** < 50ms P99 for warm data reads.

### Capacity Estimation
```
Peak compute:        500 GKE pods × 4 CPU × 8GB = 2000 CPU, 4 TB RAM
Idle compute:        0 pods (scale-to-zero)
Peak compute cost:   500 pods × $0.10/hr (preemptible) = $50/hr
24/7 compute cost:   $50/hr × 8,760 hrs = $438,000/year ← wasteful
Actual usage:        ~4 hours/day → 500 × $0.10 × 4 × 365 = $73,000/year
Savings:             $438K - $73K = $365,000/year (83% savings)

Storage:             10 TB GCS @ $0.02/GB/mo = $200/month (always on)
```

---

## B.2 Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Persistent Layer (Always On)                    │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │                     GCS (Object Storage)                   │   │
│  │  gs://snowconvert-data/                                    │   │
│  │  ├── uploads/      (input SQL files)                       │   │
│  │  ├── outputs/      (converted Snowflake SQL)               │   │
│  │  ├── checkpoints/  (migration state)                       │   │
│  │  ├── cache/        (AST parse cache)                       │   │
│  │  └── models/       (ML models, conversion rules)           │   │
│  │                                                            │   │
│  │  Cost: $0.02/GB/month          99.999999999% durability    │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  ┌──────────────────────────┐  ┌──────────────────────────┐      │
│  │   Cloud SQL (PostgreSQL)  │  │   Redis (Memorystore)    │      │
│  │   Job registry            │  │   Idempotency cache      │      │
│  │   Checkpoints             │  │   Session cache          │      │
│  │   Tenant configs          │  │   Rate limit counters    │      │
│  │   $200/month (HA)         │  │   $100/month             │      │
│  └──────────────────────────┘  └──────────────────────────┘      │
└──────────────────────────────────────────────────────────────────┘

                           ▲ ▲ ▲
                           │ │ │  Data access (GCS API, SQL, Redis)
                           │ │ │
┌──────────────────────────│─│─│──────────────────────────────────┐
│                    Ephemeral Compute Layer                        │
│                    (Scales 0 → 500)                               │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    GKE Autopilot / Node Pools                │  │
│  │                                                              │  │
│  │   Idle state:       0 pods, 0 nodes                         │  │
│  │                                                              │  │
│  │   Active state:     500 pods across 100 nodes               │  │
│  │   ┌──────┐ ┌──────┐ ┌──────┐         ┌──────┐             │  │
│  │   │Pod 1 │ │Pod 2 │ │Pod 3 │   ...   │Pod 500│             │  │
│  │   │ ┌──┐ │ │      │ │      │         │      │             │  │
│  │   │ │SSD│ │ │      │ │      │         │      │             │  │
│  │   │ │  (cache)│    │ │      │         │      │             │  │
│  │   │ └──┘ │ │      │ │      │         │      │             │  │
│  │   └──────┘ └──────┘ └──────┘         └──────┘             │  │
│  │                                                              │  │
│  │   Auto-scales based on:                                      │  │
│  │   - Pub/Sub subscription backlog                             │  │
│  │   - CPU utilization                                          │  │
│  │   - Custom metrics (jobs pending)                            │  │
│  └─────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## B.3 Analogy: Snowflake's Own Architecture

```
Snowflake:                          SnowConvert:
─────────                          ──────────
Cloud Services                     API Gateway + Job Service
  (query optimization, metadata)     (job submission, status tracking)

Virtual Warehouses                  GKE Worker Pools
  (ephemeral compute clusters)       (ephemeral conversion workers)
  (auto-suspend after 5 min)         (scale-to-zero after 5 min)
  (auto-resume on demand)            (auto-resume on queue activity)
  (size: XS to 6XL)                  (size: 5 to 500 pods)

Storage Layer                       GCS + Cloud SQL
  (S3/GCS micro-partitions)          (SQL files, checkpoints, metadata)
  (persist forever)                  (persist forever)
  ($0.023/GB/month)                  ($0.020/GB/month)
```

> *Snowflake's separation of compute and storage is its defining architectural innovation. We apply the same principle — our compute fleet is ephemeral, our data persist independently. This alignment impresses Snowflake interviewers because you're using their own insight as a design principle.*

---

## B.4 Deep Dive: Scale-to-Zero & Fast Scale-Up

### Scale-to-Zero Implementation

```yaml
# KEDA ScaledObject (Kubernetes Event-Driven Autoscaler)
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: conversion-worker-scaler
spec:
  scaleTargetRef:
    name: conversion-worker
  minReplicaCount: 0               # Scale to ZERO
  maxReplicaCount: 500
  cooldownPeriod: 300              # Wait 5 min before scaling to zero
  triggers:
  - type: gcp-pubsub
    metadata:
      subscriptionName: conversion-jobs-sub
      value: "5"                   # 1 pod per 5 messages
```

### Cold Start Mitigation

```
Problem: Scale from 0 → 100 requires:
  1. Schedule pods (5s)
  2. Pull container image (10-30s)
  3. Node auto-provisioning if needed (60-120s)
  4. Application startup (5s)
  Total: 20s (warm node) to 150s (new node)

Mitigations:
  1. Pre-pull images on all nodes (DaemonSet image-puller)
  2. Keep 2-3 "warm" nodes with 0 workload pods (spot VMs, ~$5/day)
  3. Application: lazy-load ML models (don't block startup)
  4. Use GKE node pools with pre-provisioned capacity reservation

Result: Cold start from 0 → first pod running in < 15s
```

### Local SSD Cache for GCS Latency Mitigation

```
GCS read latency: 50-100ms (first byte)
Local SSD read latency: 0.1ms

Strategy: Cache hot files on local NVMe SSDs (375 GB per node)

Cache hierarchy:
  1. L1: In-process memory cache (100 MB per pod)      ← microseconds
  2. L2: Local SSD on GKE node (375 GB shared)         ← 0.1ms
  3. L3: GCS (persistent, unlimited)                   ← 50-100ms

Cache eviction: LRU within each tier
Cache invalidation: Not needed — source files are immutable
  (Each upload gets a unique GCS path → natural cache-friendliness)
```

---

## B.5 Deep Dive: Workload Isolation

```
Namespace-based isolation (detailed in Section C):

  Compute:  Separate GKE node pools per workload class
    ┌────────────────────┐  ┌─────────────────────────┐
    │ Pool: fast-workers │  │ Pool: heavy-workers      │
    │ e2-standard-4      │  │ n2-highmem-16            │
    │ 80 nodes (spot)    │  │ 20 nodes (on-demand)     │
    │ Taints/tolerations │  │ Taints/tolerations       │
    └────────────────────┘  └─────────────────────────┘

  Storage: Per-tenant GCS prefixes (or buckets)
    gs://data/tenant-42/...     ← IAM-isolated via Workload Identity
    gs://data/tenant-99/...     ← Different service account, different keys
```

---

## B.6 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **GCS unavailable** | Workers can't read/write | GCS is 99.999% available; circuit breaker + retry |
| **Cold start too slow** | Jobs queue during scale-up | Image pre-pulling; warm node pool; KEDA scaling buffer |
| **Local SSD failure** | Cache lost | Cache is ephemeral; re-populate from GCS; no data loss |
| **Node preemption (spot)** | Workers killed | Graceful SIGTERM → NACK message → redelivery; checkpoints saved |
| **Storage cost drift** | GCS grows unbounded | Lifecycle policies: delete intermediate files after 7 days |

---

## B.7 Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| GCS over local persistent disks | PersistentVolumes (PD) | PDs are per-zone; GCS is global; workers are ephemeral |
| KEDA over HPA | Kubernetes HPA | KEDA supports scale-to-zero; HPA minimum = 1 |
| Spot/preemptible VMs | On-demand | 60-80% savings; workers are stateless and idempotent |
| Local SSD cache | No cache | 50-100ms GCS latency adds up at scale; SSD is 500x faster |
| Cloud SQL over CockroachDB | CockroachDB, Spanner | Cloud SQL is simpler; our write rate (100/min) doesn't need distributed SQL |

---

## B.8 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Scale-to-zero sounds risky. What if you lose data?" | "Compute is stateless — all state lives in GCS (files), Cloud SQL (metadata), and Redis (cache). Scaling to zero is like closing a browser tab — you lose nothing, because nothing was stored locally. This is the exact same model Snowflake uses: suspending a warehouse loses no data." |
| "GCS latency (50ms) for every file read?" | "Three-tier caching: in-memory (microseconds), local SSD (0.1ms), GCS (50ms). Hot files hit L1/L2. For batch processing, workers pre-fetch the next N files while processing the current one. GCS P50 is actually ~20ms, and we pipeline reads with processing." |
| "What if a spot VM is preempted mid-job?" | "Worker catches SIGTERM, NACKs the current Pub/Sub message, and exits cleanly. The message returns to the queue in seconds and is picked up by another worker. The checkpoint store tracks progress, so resumption is fast." |
| "Why not serverless (Cloud Run / Lambda)?" | "Cold start for our worker is 5-15s (loading parser, ML models). Cloud Run/Lambda adds 1-5s additional cold start. More critically, our workers need 4-16 GB RAM and run for 5s-15min — serverless platforms penalize long-running, memory-heavy workloads. GKE gives us direct control over scheduling and resource allocation." |

---

# Section C: Multi-Tenant Resource Isolation

> **Interview Prompt:** "SnowConvert serves 50 enterprise tenants, each with different SLA tiers (Platinum, Gold, Silver). Platinum tenants need guaranteed capacity even during a Silver tenant's 200K-file burst. Design the multi-tenant isolation model."

---

## C.1 Requirements

### Functional
- Isolate tenants so one tenant's workload doesn't affect another's performance.
- Support SLA tiers (Platinum, Gold, Silver) with guaranteed resource allocations.
- Per-tenant data isolation (encryption, access control).
- Per-tenant cost attribution and usage metering.
- Fair scheduling that prevents starvation of lower-tier tenants.

### Non-Functional
- **Performance isolation:** Platinum P99 < 5s regardless of system load.
- **Data isolation:** No cross-tenant data access.
- **Resource efficiency:** Target 70% cluster utilization.
- **Onboarding:** New tenant provisioned in < 5 minutes.

### Capacity Estimation
```
Tenants:              50 (10 Platinum, 15 Gold, 25 Silver)
Per-tenant peak:      Platinum: 5K jobs/min, Gold: 2K, Silver: 500
Cluster total:        ~50K jobs/min peak
Resource allocation:
  Platinum: 40% capacity guaranteed
  Gold:     35% capacity guaranteed
  Silver:   25% capacity guaranteed
  Burst:    All tiers can burst into unused capacity
```

---

## C.2 Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      GKE Cluster                                  │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Namespace: tenant-platinum-42    ResourceQuota: HIGH       │ │
│  │  ┌─────────────────┐ ┌────────────────────┐                │ │
│  │  │ Conversion Pods  │ │ Dedicated GCS      │                │ │
│  │  │ Limit: 200 CPU   │ │ Bucket: gs://t-42/ │                │ │
│  │  │        400 Gi    │ │ CMEK: key-42       │                │ │
│  │  └─────────────────┘ └────────────────────┘                │ │
│  │                                                              │ │
│  │  NetworkPolicy: Only traffic within namespace               │ │
│  │  Workload Identity: t-42-sa@project.iam                     │ │
│  │  PriorityClass: platinum (priority=1000)                    │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Namespace: tenant-gold-99        ResourceQuota: MEDIUM     │ │
│  │  ┌─────────────────┐ ┌────────────────────┐                │ │
│  │  │ Conversion Pods  │ │ Dedicated GCS      │                │ │
│  │  │ Limit: 100 CPU   │ │ Bucket: gs://t-99/ │                │ │
│  │  │        200 Gi    │ │ CMEK: key-99       │                │ │
│  │  └─────────────────┘ └────────────────────┘                │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Namespace: tenant-silver-77      ResourceQuota: LOW        │ │
│  │  ...                                                         │ │
│  └──────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## C.3 Deep Dive: Kubernetes-Level Isolation

### ResourceQuotas (Compute Guarantees)

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: platinum-quota
  namespace: tenant-platinum-42
spec:
  hard:
    requests.cpu: "200"          # Guaranteed 200 CPU cores
    requests.memory: "400Gi"     # Guaranteed 400 GB RAM
    limits.cpu: "300"            # Can burst to 300 CPU
    limits.memory: "600Gi"       # Can burst to 600 GB
    pods: "200"                  # Max 200 pods
    persistentvolumeclaims: "50" # Max 50 PVCs
```

### PriorityClasses (Scheduling Hierarchy)

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: platinum
value: 1000000
preemptionPolicy: PreemptLowerPriority
description: "Platinum tenants preempt Gold and Silver when resources are scarce"

---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: gold
value: 100000
preemptionPolicy: PreemptLowerPriority

---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: silver
value: 10000
preemptionPolicy: Never   # Silver never preempts — waits in queue
```

**What happens during resource contention:**
```
Cluster at 95% capacity. Platinum tenant submits 50 new jobs.
  1. Scheduler finds no free resources
  2. Evaluates PriorityClass: platinum (1M) > gold (100K) > silver (10K)
  3. Preempts 50 Silver pods (graceful — SIGTERM with 30s grace)
  4. Schedules 50 Platinum pods
  5. Silver pods NACK their Pub/Sub messages → jobs redelivered later
  6. When cluster utilization drops, Silver pods are re-created
```

### NetworkPolicies (Network Isolation)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny-cross-tenant
  namespace: tenant-platinum-42
spec:
  podSelector: {}                # Apply to all pods in namespace
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              tenant: tenant-42  # Only from own namespace
        - namespaceSelector:
            matchLabels:
              role: system       # Allow system services (monitoring, etc.)
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              tenant: tenant-42
        - namespaceSelector:
            matchLabels:
              role: system
    - to:                        # Allow external access (GCS, Pub/Sub)
        - ipBlock:
            cidr: 0.0.0.0/0
      ports:
        - port: 443
          protocol: TCP
```

---

## C.4 Deep Dive: Data Isolation

### Storage Isolation with Workload Identity

```
Each tenant gets:
  1. Dedicated GCS bucket:  gs://snowconvert-tenant-42/
  2. Service account:       tenant-42-sa@project.iam.gserviceaccount.com
  3. IAM binding:           tenant-42-sa → storage.objectAdmin on gs://tenant-42/
  4. Kubernetes SA:         sa-42 → annotated with Workload Identity for tenant-42-sa

Pod → K8s SA → Workload Identity → GCP SA → IAM → GCS bucket

Result: Pod in namespace tenant-42 can ONLY access gs://tenant-42/
        Even if code has a bug referencing gs://tenant-99/, IAM blocks it.
```

### Per-Tenant Encryption (CMEK)

```
Customer-Managed Encryption Keys (CMEK):

  Tenant 42: KMS key → projects/p/locations/us/keyRings/kr/cryptoKeys/t-42
  Tenant 99: KMS key → projects/p/locations/us/keyRings/kr/cryptoKeys/t-99

  GCS objects encrypted at rest with tenant-specific keys.
  Tenant can rotate or revoke their key.
  Key revocation = instant data inaccessibility (crypto-shred).

  Why CMEK over Google-managed keys?
  → Enterprise compliance (SOC 2, HIPAA) often requires customer key management.
  → Crypto-shredding enables instant tenant data deletion without scanning storage.
```

---

## C.5 Deep Dive: Fair Scheduling & Rate Limiting

### Weighted Fair Queuing for Pub/Sub

```
Pub/Sub Subscriptions (one per tier):

  subscription-platinum: flowControl.maxMessages = 500
  subscription-gold:     flowControl.maxMessages = 300
  subscription-silver:   flowControl.maxMessages = 100

Worker Pull Strategy:
  Each worker pulls from its tenant's subscription.
  flowControl limits prevent one tenant from saturating the worker pool.

Alternative: Single subscription + tenant-aware worker:
  Worker checks tenant tier → applies rate limit
  But this mixes tenants on the same workers → harder isolation
```

### API Rate Limiting per Tenant

```
Rate limits (enforced at API Gateway):

  Platinum: 10,000 requests/minute, 500 concurrent conversions
  Gold:     5,000 requests/minute, 200 concurrent conversions
  Silver:   1,000 requests/minute, 50 concurrent conversions

Implementation: Redis sliding window counter per tenant
  Key: ratelimit:{tenant_id}:{minute}
  Value: request count
  TTL: 60s
```

---

## C.6 Deep Dive: Cost Attribution & Metering

```
Usage Metering Pipeline:

  ┌──────────┐   ┌─────────────┐   ┌────────────────┐
  │ Workers   │──▶│ Usage Events │──▶│  BigQuery       │
  │ emit usage│   │ (Pub/Sub)    │   │  (analytics)    │
  │ events    │   └─────────────┘   └────────┬───────┘
  └──────────┘                               │
                                    ┌────────▼───────┐
                                    │ Billing Service │
                                    │ (monthly)       │
                                    └────────────────┘

Usage Event Schema:
{
  "tenant_id": "tenant-42",
  "event_type": "conversion",
  "timestamp": "2025-01-15T10:00:34Z",
  "resources": {
    "cpu_seconds": 12.5,
    "memory_gb_seconds": 50.0,
    "gcs_bytes_read": 2500000,
    "gcs_bytes_written": 1800000
  },
  "job_id": "job-a1b2c3d4",
  "file_count": 1
}
```

---

## C.7 Tenant Onboarding Automation

```
New Tenant Provisioning (< 5 minutes):

  1. Create Kubernetes namespace:       kubectl create ns tenant-{id}
  2. Apply ResourceQuota:              kubectl apply -f quota-{tier}.yaml
  3. Apply NetworkPolicy:              kubectl apply -f netpol-{id}.yaml
  4. Create GCS bucket:                gsutil mb gs://snowconvert-tenant-{id}
  5. Create KMS key:                   gcloud kms keys create t-{id}...
  6. Create GCP service account:        gcloud iam service-accounts create...
  7. Bind Workload Identity:           gcloud iam...
  8. Create Pub/Sub subscription:       gcloud pubsub subscriptions create...
  9. Apply PriorityClass:              kubectl apply -f priority-{tier}.yaml
  10. Insert tenant config:            INSERT INTO tenants...

  All steps are Terraform-managed:
    terraform apply -var="tenant_id=42" -var="tier=platinum"
```

---

## C.8 Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Resource quota exhausted** | Tenant can't schedule pods | Alert at 80%; auto-scale if bursting is allowed |
| **Cross-tenant data access** | Data breach | Workload Identity + IAM + NetworkPolicy layered defense |
| **Noisy neighbor (CPU steal)** | Performance degradation | ResourceQuota + PriorityClass + node pool isolation |
| **Tenant key revocation** | Data inaccessible | Expected behavior; alert tenant admin; data is crypto-shredded |
| **Cost overrun** | Unexpected bill | Budget alerts in BigQuery; hard limits via ResourceQuota |

---

## C.9 Trade-offs

| Decision | Alternative | Why This Choice |
|----------|-------------|-----------------|
| Namespace isolation | Separate clusters per tenant | One cluster is cheaper; namespaces + RBAC are sufficient |
| Workload Identity | Service account key files | No keys to rotate/leak; GKE-native |
| CMEK | Google-managed keys | Enterprise compliance; crypto-shredding capability |
| PriorityClass preemption | Dedicated node pools per tenant | More efficient resource sharing; dedicated pools waste capacity |
| Per-tenant Pub/Sub subscription | Single subscription + routing | Hard isolation; prevents cross-tenant queue interference |

---

## C.10 Interviewer Pushback

| Interviewer Says | Your Response |
|-----------------|---------------|
| "Namespaces aren't real isolation." | "Namespaces alone aren't, but combined with ResourceQuotas, NetworkPolicies, Workload Identity, and dedicated node pools, they provide strong isolation. For hard multi-tenancy (hostile tenants), we'd use GKE Sandbox (gVisor). For our enterprise SaaS, namespace + policy isolation meets compliance requirements." |
| "What if a Platinum tenant uses 100% of their quota?" | "ResourceQuota has both requests (guaranteed) and limits (burstable). At 100% of limits, pods get throttled (CPU) or OOM-killed (memory). We alert at 80% and proactively scale node pools. Platinum quotas are sized for peak load + 30% headroom." |
| "How do you handle tenant data deletion (GDPR)?" | "CMEK + crypto-shredding. Delete the KMS key → all GCS objects encrypted with that key become permanently inaccessible. No need to scan and delete individual files. For metadata in PostgreSQL, a DELETE cascade on tenant_id removes all rows. Complete deletion in under 60 seconds." |
| "50 namespaces — doesn't that create management headaches?" | "Terraform + GitOps. Each tenant is a Terraform module with 10 resources. Changes go through PR review. ArgoCD syncs the namespace configs to GKE. Adding a tenant is `terraform apply` — under 5 minutes with zero manual steps." |

---

## Summary Narratives

### Observability
> "I'd build an **observability platform on the OTel + Grafana stack**. OTel SDK instruments every service with traces, metrics, and logs. OTel Collectors run as DaemonSets with **tail-based sampling** — 100% of errors, 1% of successes — reducing storage costs by 94%. **SLO-based alerting with burn rates** catches slow burns without noise. **Trace-to-log correlation** lets us go from 'job slow' to 'exact error line' in 2 minutes."

### Compute/Storage Separation
> "I'd **separate compute from storage** — identical to Snowflake's architecture. Compute is ephemeral GKE pods that scale 0 → 500 via KEDA. Storage is persistent GCS + Cloud SQL. **Scale-to-zero** saves 83% on compute costs. **Local SSD caches** eliminate GCS latency. Workers are stateless — preemption-safe, with all state in the checkpoint store."

### Multi-Tenancy
> "I'd isolate tenants using **Kubernetes namespaces** with layered security: **ResourceQuotas** for compute guarantees, **NetworkPolicies** for network isolation, **Workload Identity** for GCS access control, and **CMEK** for per-tenant encryption. **PriorityClasses** ensure Platinum tenants preempt Silver during resource contention. Onboarding is a single Terraform module — under 5 minutes."

---

## Key Terms to Drop Naturally

- **Three pillars** (traces, metrics, logs)
- **OpenTelemetry**, **OTLP**, **context propagation**, **traceparent header**
- **Tail-based sampling**, **head-based sampling**, **sampling decision**
- **RED metrics** (Rate, Errors, Duration)
- **SLO**, **error budget**, **burn rate**, **multi-window alerting**
- **Exemplars** (link metrics → traces)
- **Trace-to-log correlation**, **structured logging**
- **Cardinality explosion**, **label allowlisting**
- **Separation of compute and storage**, **ephemeral compute**
- **Scale-to-zero**, **KEDA**, **cold start mitigation**
- **Image pre-pulling**, **warm node pool**, **node pool capacity reservation**
- **Three-tier cache** (memory → SSD → GCS)
- **Spot / preemptible VMs**, **SIGTERM graceful shutdown**
- **Auto-suspend**, **auto-resume** (Snowflake analogy)
- **Namespace isolation**, **ResourceQuota**, **LimitRange**
- **NetworkPolicy**, **Workload Identity**, **CMEK**
- **PriorityClass**, **preemption**, **weighted fair queuing**
- **Crypto-shredding**, **tenant offboarding**
- **Terraform + GitOps**, **tenant-as-code**
- **Cost attribution**, **usage metering**, **chargeback**
